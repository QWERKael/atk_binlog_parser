package parse

import (
	"github.com/siddontang/go-mysql/replication"
	"io"
	"bytes"
	"fmt"
	"encoding/binary"
	"atk_binlog_parser/util"
)

type ParseUnit struct {
	BinlogParser replication.BinlogParser
	EventList    []MyEvent
}

//func NewParseUnit() ParseUnit {
//	return ParseUnit{
//		BinlogParser: *(replication.NewBinlogParser()),
//		EventList:    make([]MyEvent, 0),
//	}
//}

type MyEvent struct {
	StartPos    uint32
	EventLength uint32
	RawData     []byte
	MyEvent     replication.BinlogEvent
}

func GetEventBinary(file io.ReaderAt, startPos uint32, eventLength uint32) ([]byte, error) {
	b := make([]byte, eventLength)
	if _, err := file.ReadAt(b, int64(startPos)); err != nil {
		return nil, err
	}
	//打印信息
	//var str string
	//str = fmt.Sprintf("\nstart--goroutinne %d, length %d\n", idx, eventLength)
	//for _, bin := range b {
	//	str += fmt.Sprintf("%02X ", bin)
	//}
	//str += fmt.Sprintf("\nend--goroutinne %d, length %d\n", idx, eventLength)
	//fmt.Print(str)
	//fmt.Println()
	//fmt.Printf("--\n--b的地址:%p\n--\n",b)
	return b, nil
}

//func divider(allData []byte, start uint32, end uint32) ([]byte, []byte, error) {
//	rawData := allData[:end:end]
//	remainData := allData[end:]
//	return rawData, remainData, nil
//}

func MyParser(idx int, file io.ReaderAt, pu ParseUnit, writer chan string, myTransFilter TransFilter) error {
	p := pu.BinlogParser
	//fmt.Printf("--idx:%d\n--p的地址：%p\n--\n", idx, &p)
	//le := len(pu.EventList)
	//startLine := pu.EventList[0].StartPos
	//sumLength := pu.EventList[le-1].StartPos + pu.EventList[le-1].EventLength - startLine
	//allData, err := GetEventBinary(file, startLine, sumLength)
	//if err != nil {
	//	return err
	//}
	var trans = newTrans(myTransFilter)
	trans.mergeFlag = *(util.MergeFlag)
	trans.flashback = *(util.FlashBack)
	for _, me := range pu.EventList {
		rawData, err := GetEventBinary(file, me.StartPos, me.EventLength)
		//rawData, remainData, err := divider(allData, me.StartPos-startLine, me.StartPos+me.EventLength-startLine)
		if err != nil {
			return err
		}
		me.MyEvent.RawData = rawData
		var parsedBinlogEvent *replication.BinlogEvent
		if me.EventLength > 0 {
			parsedBinlogEvent, err = p.Parse(me.MyEvent.RawData)
			//_, err = p.Parse(me.MyEvent.RawData)
			header := parsedBinlogEvent.Header
			event := parsedBinlogEvent.Event
			trans.decode(idx, *header, event)
			if trans.transDone {
				str := fmt.Sprint(trans.strBuf)
				//fmt.Print(str)
				writer <- str
			}
		}
		//allData = remainData
	}
	if *(util.MergeFlag) {
		str := trans.tableMergeUnits.Decode()
		writer <- str
	}
	close(writer)
	return nil
}

func GetFormatDescriptionEvent(file io.ReaderAt) (replication.BinlogParser, uint32, error) {
	var err error = nil
	var startPos, nextPos, eventLength uint32
	var rawData []byte
	startPos = 4
	atPos := startPos + 9
	b := make([]byte, 4)
	if _, err := file.ReadAt(b, int64(atPos)); err != nil {
		p := replication.NewBinlogParser()
		return *p, 0, err
	}
	eventLength = binary.LittleEndian.Uint32(b)
	nextPos = startPos + eventLength
	rawData, err = GetEventBinary(file, startPos, eventLength)
	p := replication.NewBinlogParser()
	//var parsedEvent *replication.BinlogEvent
	_, err = p.Parse(rawData)
	strBuf := bytes.NewBufferString(fmt.Sprintf("\ngoroutine num %d\n", 0))
	//(*parsedEvent).Header.Dump(os.Stdout)
	//(*parsedEvent).Event.Dump(os.Stdout)
	//(*parsedEvent).Header.Dump(strBuf)
	//(*parsedEvent).Event.Dump(strBuf)
	fmt.Fprintf(strBuf,"\n-- 开始解析\n")
	fmt.Println(strBuf)
	return *p, nextPos, err
}
