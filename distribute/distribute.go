package distribute

import (
	"io"
	"github.com/siddontang/go-mysql/replication"
	"atk_binlog_parser/parse"
	"encoding/binary"
	"atk_binlog_parser/util"
)

//分配者
func Distributor(pus chan<- *parse.ParseUnit, file io.ReaderAt, startPos uint32, config util.Config, f PreFilter) {
	maxEvents := config.MaxEvents
	endPos := config.EndPos
	endTime := config.EndTime
	// flashback设置，把maxEvents锁定为1，让每个解析的goroutine近似为一个事务
	if config.FlashBack == true {
		maxEvents = 1
	}
	var reverseList = make([]*parse.ParseUnit, 0)

	var err error = nil
	var nextPos, eventLength uint32
	var eventTimestamp uint32 = 0
	var eventType replication.EventType
	for (endPos == 0 || startPos < endPos) && (endTime == 0 || eventTimestamp < endTime) && err == nil {
		pu := new(parse.ParseUnit)
		//pu := parse.NewParseUnit()
		for i := 0; (i < maxEvents || eventType != replication.XID_EVENT) && (endPos == 0 || startPos < endPos) && (endTime == 0 || eventTimestamp < endTime) && err == nil; {
			eventTimestamp, eventLength, nextPos, eventType, err = PreDistribute(file, startPos)
			if ok := f(eventType); ok {
				me := new(parse.MyEvent)
				me.StartPos = startPos
				me.EventLength = eventLength
				(*pu).EventList = append((*pu).EventList, *me)
				i++
			}
			startPos = nextPos
		}
		if config.FlashBack == false {
			pus <- pu
		} else {
			reverseList = append(reverseList, pu)
		}
	}
	if config.FlashBack == true {
		llen := len(reverseList)
		for llen > 0 {
			llen--
			pus <- reverseList[llen]
		}
	}
	close(pus)
}

func PreDistribute(file io.ReaderAt, startPos uint32) (uint32, uint32, uint32, replication.EventType, error) {
	var b []byte
	var atPos = startPos
	b = make([]byte, 4)
	if _, err := file.ReadAt(b, int64(atPos)); err != nil {
		return 0, 0, 0, 0, err
	}
	eventTimestamp := binary.LittleEndian.Uint32(b)
	b = make([]byte, 1)
	atPos = atPos + 4
	if _, err := file.ReadAt(b, int64(atPos)); err != nil {
		return 0, 0, 0, 0, err
	}
	eventType := replication.EventType(b[0])
	b = make([]byte, 4)
	atPos = atPos + 5
	if _, err := file.ReadAt(b, int64(atPos)); err != nil {
		return 0, 0, 0, 0, err
	}
	eventLength := binary.LittleEndian.Uint32(b)
	//打印信息
	//fmt.Println("event长度：", eventLength)
	//for _, bin := range b {
	//	fmt.Printf("%02X ", bin)
	//}
	//fmt.Println()
	nextPos := startPos + eventLength
	return eventTimestamp, eventLength, nextPos, eventType, nil
}

type PreFilter func(eventType replication.EventType) bool

func MakePreFilter(eventFilterElement []replication.EventType) PreFilter {
	return func(eventType replication.EventType) bool {
		for _, efe := range eventFilterElement {
			if eventType == efe {
				return true
			}
		}
		return false
	}
}

func GetPosFromTime(file io.ReaderAt, beginTime uint32, startPos uint32) uint32 {
	var err error = nil
	nextPos := startPos
	var eventTimestamp uint32
	for err == nil && eventTimestamp < beginTime {
		eventTimestamp, _, nextPos, _, err = PreDistribute(file, startPos)
		startPos = nextPos
	}
	//if err != nil {
	//	if err == io.EOF {
	//		println("未能找到开始时间点")
	//		os.Exit(1)
	//	}
	//	println("获取起始位置出错")
	//	os.Exit(1)
	//}
	return nextPos
}
