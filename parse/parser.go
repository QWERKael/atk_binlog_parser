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
	return b, nil
}

func MyParser(idx int, file io.ReaderAt, pu ParseUnit, writer chan string, myTransFilter TransFilter) error {
	p := pu.BinlogParser
	var trans = newTrans(myTransFilter)
	trans.mergeFlag = *(util.MergeFlag)
	trans.flashback = *(util.FlashBack)
	for _, me := range pu.EventList {
		rawData, err := GetEventBinary(file, me.StartPos, me.EventLength)
		if err != nil {
			return err
		}
		me.MyEvent.RawData = rawData
		var parsedBinlogEvent *replication.BinlogEvent
		if me.EventLength > 0 {
			parsedBinlogEvent, err = p.Parse(me.MyEvent.RawData)
			header := parsedBinlogEvent.Header
			event := parsedBinlogEvent.Event
			trans.decode(idx, *header, event)
			if trans.transDone {
				str := fmt.Sprint(trans.strBuf)
				writer <- str
			}
		}
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
	_, err = p.Parse(rawData)
	strBuf := bytes.NewBufferString(fmt.Sprintf("\ngoroutine num %d\n", 0))
	fmt.Fprintf(strBuf,"\n-- 开始解析\n")
	fmt.Println(strBuf)
	return *p, nextPos, err
}
