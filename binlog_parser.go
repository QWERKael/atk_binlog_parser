package main

import (
	"io"
	"bytes"
	"fmt"
	"os"
	"runtime"
	"atk_binlog_parser/distribute"
	"atk_binlog_parser/parse"
	"github.com/siddontang/go-mysql/replication"
	"atk_binlog_parser/util"
	"atk_binlog_parser/connect"
	"time"
	"sync"
)

var wg sync.WaitGroup

func checkBinlogFile(f io.ReaderAt) error {
	BinLogFileHeader := []byte{0xfe, 0x62, 0x69, 0x6e}
	b := make([]byte, 4)
	if _, err := f.ReadAt(b, 0); err != nil {
		return err
	} else if !bytes.Equal(b, BinLogFileHeader) {
		fmt.Println("该文件可能不是binlog文件")
		return err
	}
	fmt.Println("校验binlog文件成功")
	return nil
}

func main() {
	config := util.GetConfig()
	runtime.GOMAXPROCS(config.MaxParallel)
	if config.DSN == "" {
		fmt.Println("请输入数据库连接信息")
		os.Exit(1)
	}
	file, err := os.Open(config.FileName)
	if err != nil {
		panic(err.Error())
	}
	connect.GetAllTableSchema()
	//解析单元的通道，缓冲为maxParallel，当正在解析的解析单元大于此数量时会阻塞，也可以将所有的分割都缓存起来使其不阻塞，
	//但是会占用大量的内存，而对性能的提升并不大
	pus := make(chan *parse.ParseUnit, config.MaxGoroutine)
	//检查文件
	err = checkBinlogFile(file)
	if err != nil {
		panic(err.Error())
	}
	//获取FormatDescriptionEvent
	p, nextPos, err := parse.GetFormatDescriptionEvent(file)
	//根据EventType生成预处理过滤器
	eventFilterElement := []replication.EventType{replication.QUERY_EVENT,
		replication.XID_EVENT,
		replication.TABLE_MAP_EVENT,
		replication.WRITE_ROWS_EVENTv2,
		replication.UPDATE_ROWS_EVENTv2,
		replication.DELETE_ROWS_EVENTv2}
	myPreFilter := distribute.MakePreFilter(eventFilterElement)
	//根据BeginPos和BeginTime确定开始解析的pos位置
	if config.BeginPos > nextPos {
		nextPos = config.BeginPos
	}
	if config.BeginTime > 0 {
		nextPos = distribute.GetPosFromTime(file, config.BeginTime, nextPos)
	}
	//分配者对剩余Event进行切分
	go distribute.Distributor(pus, file, nextPos, config, myPreFilter)
	//根据库表名生成事务过滤器
	transFilterElement := parse.ParseTableInfo(config.TableFiler)
	myTransFilter := parse.MakeTransFilter(transFilterElement)
	//以ParseUnit为单位，并行解析
	idx := 0
	//顺序输出
	var writers = make(chan chan string, 1000)
	wg.Add(1)
	go func() {
		for writer := range writers {
			for str := range writer {
				//str = ""
				fmt.Printf("%s", str)
				str = ""
			}
		}
		defer wg.Done()
	}()
	//并行解析
	for pu := range pus {
		wg.Add(1)
		idx++
		writer := make(chan string, config.MaxEvents)
		pu.BinlogParser = p
		go func(idx int, file io.ReaderAt, pu parse.ParseUnit, writer chan string, myTransFilter parse.TransFilter) {
			fmt.Printf("\ngoroutine %d begin:%s\n", idx, time.Now().Format("2006-01-02 15:04:05"))
			if err = parse.MyParser(idx, file, pu, writer, myTransFilter); err != nil {
				os.Exit(1)
			}
			defer wg.Done()
			fmt.Printf("\ngoroutine %d end:%s\n", idx, time.Now().Format("2006-01-02 15:04:05"))
		}(idx, file, *pu, writer, myTransFilter)
		writers <- writer
		pu = nil
	}
	close(writers)
	wg.Wait()
	fmt.Printf("\n-- 解析完成\n")
	//connect.CloseConn()
	//close(pus)
}
