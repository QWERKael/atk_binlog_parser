package parse

import (
	"bytes"
	"github.com/siddontang/go-mysql/replication"
	"fmt"
	"strings"
	"atk_binlog_parser/merge"
	"time"
)

type SchemaTable struct {
	SchemaName string
	TableName  string
}

func ParseTableInfo(str string) []SchemaTable {
	filterStrings := strings.Split(str, ",")
	var tables = make([]SchemaTable, len(filterStrings))
	for idx, fs := range filterStrings {
		schemaTable := strings.Split(fs, ".")
		var ti = SchemaTable{
			SchemaName: schemaTable[0],
			TableName:  schemaTable[1],
		}
		tables[idx] = ti
	}
	return tables
}

type tableMap struct {
	tableID     uint64
	schemaName  string
	tableName   string
	columnNames []string
}

func newTableMap(tableID uint64, schemaName string, tableName string, columnNames []string) tableMap {
	return tableMap{
		tableID:     tableID,
		schemaName:  schemaName,
		tableName:   tableName,
		columnNames: columnNames,
	}
}

type Trans struct {
	transID         uint64
	threadID        uint32
	GTID            uint32
	tableMaps       map[uint64]tableMap
	strBuf          *bytes.Buffer
	transDone       bool
	myTransFilter   TransFilter
	effectedRows    uint64
	mergeFlag       bool
	flashback       bool
	tableMergeUnits merge.TableMergeUnits
}

func newTrans(myTransFilter TransFilter) Trans {
	return Trans{
		transID:         0,
		threadID:        0,
		GTID:            0,
		tableMaps:       make(map[uint64]tableMap),
		strBuf:          new(bytes.Buffer),
		transDone:       true,
		myTransFilter:   myTransFilter,
		effectedRows:    0,
		mergeFlag:       false,
		flashback:       false,
		tableMergeUnits: merge.NewTableMergeUnits(),
	}
}

func (trans *Trans) decode(idx int, header replication.EventHeader, event replication.Event) {
	if trans.transDone == true {
		trans.strBuf = bytes.NewBufferString(fmt.Sprintf("\n-- goroutine num %d\n", idx))
	}
	fmt.Fprintf(trans.strBuf, "-- time: %s, pos: %d, next-pos:%d\n", time.Unix(int64(header.Timestamp), 0).Format("2006-01-02 15:04:05"), header.LogPos-header.EventSize, header.LogPos)
	switch header.EventType {
	case replication.QUERY_EVENT:
		trans.formatQuery(event)
		break
	case replication.XID_EVENT:
		trans.formatXid(event)
		break
	case replication.TABLE_MAP_EVENT:
		trans.formatTableMap(event)
		break
	case replication.WRITE_ROWS_EVENTv2:
		trans.formatWriteEvent(event)
		break
	case replication.UPDATE_ROWS_EVENTv2:
		trans.formatUpdateEvent(event)
		break
	case replication.DELETE_ROWS_EVENTv2:
		trans.formatDeleteEvent(event)
		break
	default:
		event.Dump((*trans).strBuf)
		break
	}
}

func (trans *Trans) done() {
	trans.transDone = true
}

func (trans *Trans) start() {
	trans.transDone = false
}

func (trans *Trans) reset() {
	trans.transID = 0
	trans.threadID = 0
	trans.GTID = 0
	trans.tableMaps = make(map[uint64]tableMap)
	trans.strBuf = new(bytes.Buffer)
	trans.transDone = true
	trans.effectedRows = 0
}

type TransFilter func(schemaName string, tableName string) bool

func MakeTransFilter(transFilterElement []SchemaTable) TransFilter {
	return func(schemaName string, tableName string) bool {
		for _, tfe := range transFilterElement {
			if (tfe.SchemaName == "*" || tfe.SchemaName == schemaName) && (tfe.TableName == "*" || tfe.TableName == tableName) {
				return true
			}
		}
		return false
	}
}
