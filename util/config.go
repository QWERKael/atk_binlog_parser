package util

import (
	"flag"
	"time"
	"fmt"
	"os"
)

type Config struct {
	DSN          string
	FileName     string
	MaxEvents    int
	MaxGoroutine int
	MaxParallel  int
	TableFiler   string
	MergeFlag    bool
	FlashBack    bool
	Stat         bool
	BeginPos     uint32
	EndPos       uint32
	BeginTime    uint32
	EndTime      uint32
}

var DSN = flag.String("DSN", "", "数据库连接信息")
var MaxEvents = flag.Int("maxevent", 10000, "每个解析单元处理的event数量")
var MaxGoroutine = flag.Int("maxgoroutine", 10, "最大活跃goroutine数量")
var MaxParallel = flag.Int("maxparallel", 2, "使用的cpu数量")
var TableFiler = flag.String("tables", "*.*", "只解析指定的表，请写成database.table形式，如有多个，用逗号分隔，使用*匹配全库或全表")
var MergeFlag = flag.Bool("merge", false, "是否开启merge模式")
var FlashBack = flag.Bool("flashback", false, "是否开启flashback模式")
var Stat = flag.Bool("stat", false, "是否开启统计模式")
var BeginPos = flag.Uint64("beginpos", 0, "开始的pos位置")
var EndPos = flag.Uint64("endpos", 0, "结束的pos位置")
var BeginTimeString = flag.String("begintime", "", "开始时间")
var EndTimeString = flag.String("endtime", "", "结束时间")
var BeginTime uint32
var EndTime uint32
var FileName string

func GetConfig() Config {
	flag.Parse()
	FileName = flag.Arg(0)
	if *BeginTimeString != "" {
		myTime, err := time.Parse("2006-01-02 15:04:05", *BeginTimeString)
		if err != nil {
			fmt.Printf("开始时间错误：%#v", err)
			os.Exit(1)
		}
		BeginTime = uint32(myTime.Unix())
	} else {
		BeginTime = 0
	}
	if *EndTimeString != "" {
		myTime, err := time.Parse("2006-01-02 15:04:05", *EndTimeString)
		if err != nil {
			fmt.Printf("开始时间错误：%#v", err)
			os.Exit(1)
		}
		EndTime = uint32(myTime.Unix())
	} else {
		EndTime = 0
	}
	var config = Config{
		DSN:          *DSN,
		FileName:     FileName,
		MaxEvents:    *MaxEvents,
		MaxGoroutine: *MaxGoroutine,
		MaxParallel:  *MaxParallel,
		TableFiler:   *TableFiler,
		MergeFlag:    *MergeFlag,
		FlashBack:    *FlashBack,
		Stat:         *Stat,
		BeginPos:     uint32(*BeginPos),
		EndPos:       uint32(*EndPos),
		BeginTime:    BeginTime,
		EndTime:      EndTime,
	}
	return config
}
