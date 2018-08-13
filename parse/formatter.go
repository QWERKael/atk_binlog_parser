package parse

import (
	"github.com/siddontang/go-mysql/replication"
	"strings"
	"fmt"
	"atk_binlog_parser/connect"
	"bytes"
	"atk_binlog_parser/format"
)

func (trans *Trans) formatQuery(event replication.Event) {
	trans.reset()
	queryEvent := event.(*replication.QueryEvent)
	query := string(queryEvent.Query)
	threadID := queryEvent.SlaveProxyID
	dbName := queryEvent.Schema
	if strings.ToUpper(strings.Trim(query, " ")) == "BEGIN" {
		trans.start()
		trans.threadID = threadID
		fmt.Fprintf(trans.strBuf, "\n%s;\t-- 开始事务，线程ID %d 库名 %s\n", query, threadID, dbName)
	} else {
		trans.done()
		if trans.flashback == false {
			fmt.Fprintf(trans.strBuf, "\n%s;\t-- 线程ID %d 库名 %s\n", query, threadID, dbName)
		}
	}
}

func (trans *Trans) formatXid(event replication.Event) {
	xidEvent := event.(*replication.XIDEvent)
	xid := uint64(xidEvent.XID)
	trans.transID = xid
	trans.done()
	if trans.effectedRows == 0 {
		trans.strBuf = bytes.NewBufferString("")
	} else if trans.effectedRows > 0 {
		fmt.Fprintf(trans.strBuf, "COMMIT;\t-- 事务结束，事务号 %d，解析行数 %d", xid, trans.effectedRows)
	} else {
		fmt.Println("事务影响行数统计错误")
	}
}

func (trans *Trans) formatTableMap(event replication.Event) {
	tableMapEvent := event.(*replication.TableMapEvent)
	schemaName := string(tableMapEvent.Schema)
	tableName := string(tableMapEvent.Table)
	if trans.myTransFilter(schemaName, tableName) == false {
		return
	}
	tableID := uint64(tableMapEvent.TableID)
	columnNames := connect.GetMySQLTableMap(schemaName, tableName)
	trans.tableMaps[tableID] = newTableMap(tableID, schemaName, tableName, columnNames)
	fmt.Fprintf(trans.strBuf, "-- 解析表信息，库名 %s，表名 %s，表ID %d\n-- 表结构 %#v\n", schemaName, tableName, tableID, columnNames)
}

func (trans *Trans) formatWriteEvent(event replication.Event) {
	rowsEvent := event.(*replication.RowsEvent)
	tableId := uint64(rowsEvent.TableID)
	tm, err := trans.tableMaps[tableId]
	if err == false {
		return
	}
	schemaName := tm.schemaName
	tableName := tm.tableName
	columnNames := tm.columnNames
	row := rowsEvent.Rows[0]
	if len(columnNames) != len(row) {
		fmt.Fprintf(trans.strBuf, "-- `%s`.`%s`表中，列名数量与binlog中的列数量不符\n", schemaName, tableName)
		return
	}
	//开启merge选项后，执行merge操作
	if trans.mergeFlag {
		trans.tableMergeUnits.InsertMerge(schemaName, tableName, rowsEvent.Rows)
		return
	}
	trans.effectedRows++
	//执行flashback
	if trans.flashback {
		stmt := format.DeleteMaker(schemaName, tableName, row, columnNames)
		fmt.Fprintf(trans.strBuf, "%s\n", stmt)
		return
	}
	stmt := format.InsertMaker(schemaName, tableName, row, columnNames)
	fmt.Fprintf(trans.strBuf, "%s\n", stmt)
}

func (trans *Trans) formatUpdateEvent(event replication.Event) {
	rowsEvent := event.(*replication.RowsEvent)
	tableId := uint64(rowsEvent.TableID)
	tm, err := trans.tableMaps[tableId]
	if err == false {
		return
	}
	schemaName := tm.schemaName
	tableName := tm.tableName
	columnNames := tm.columnNames
	rowOld := rowsEvent.Rows[0]
	rowNew := rowsEvent.Rows[1]
	if len(columnNames) != len(rowOld) || len(columnNames) != len(rowNew) {
		fmt.Fprintf(trans.strBuf, "-- `%s`.`%s`表中，列名数量与binlog中的列数量不符\n", schemaName, tableName)
		return
	}
	//开启merge选项后，执行merge操作
	if trans.mergeFlag {
		trans.tableMergeUnits.UpdateMerge(schemaName, tableName, rowsEvent.Rows)
		return
	}
	trans.effectedRows++
	//执行flashback
	if trans.flashback {
		stmt := format.UpdateMaker(schemaName, tableName, rowNew, rowOld, columnNames)
		fmt.Fprintf(trans.strBuf, "%s\n", stmt)
		return
	}
	stmt := format.UpdateMaker(schemaName, tableName, rowOld, rowNew, columnNames)
	fmt.Fprintf(trans.strBuf, "%s\n", stmt)
}

func (trans *Trans) formatDeleteEvent(event replication.Event) {
	rowsEvent := event.(*replication.RowsEvent)
	tableId := uint64(rowsEvent.TableID)
	tm, err := trans.tableMaps[tableId]
	if err == false {
		return
	}
	schemaName := tm.schemaName
	tableName := tm.tableName
	columnNames := tm.columnNames
	row := rowsEvent.Rows[0]
	if len(columnNames) != len(row) {
		fmt.Fprintf(trans.strBuf, "-- `%s`.`%s`表中，列名数量与binlog中的列数量不符\n", schemaName, tableName)
		return
	}
	//开启merge选项后，执行merge操作
	if trans.mergeFlag {
		trans.tableMergeUnits.DeleteMerge(schemaName, tableName, rowsEvent.Rows)
		return
	}
	trans.effectedRows++
	//执行flashback
	if trans.flashback {
		stmt := format.InsertMaker(schemaName, tableName, row, columnNames)
		fmt.Fprintf(trans.strBuf, "%s\n", stmt)
		return
	}
	stmt := format.DeleteMaker(schemaName, tableName, row, columnNames)
	fmt.Fprintf(trans.strBuf, "%s\n", stmt)
}