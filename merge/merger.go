package merge

import (
	"crypto/md5"
	"encoding/gob"
	"bytes"
	"encoding/hex"
	"atk_binlog_parser/connect"
	"atk_binlog_parser/format"
	"fmt"
	"os"
)

type DataMergeUnit struct {
	//operation: 1 insert, 2 delete, 3 update
	firstOperation uint8
	lastOperation  uint8
	oldRow         []interface{}
	newRow         []interface{}
}

type SchemaTable struct {
	SchemaName string
	TableName  string
}

type TableMergeUnits map[SchemaTable]map[string]*DataMergeUnit

func NewTableMergeUnits() TableMergeUnits {
	return make(map[SchemaTable]map[string]*DataMergeUnit)
}

func GetBytes(key interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func GetMD5(row interface{}) string {
	code := md5.New()
	encode, _ := GetBytes(row)
	code.Write(encode)
	cipherStr := code.Sum(nil)
	str := hex.EncodeToString(cipherStr)
	return str
}

func (tableMergeUnits *TableMergeUnits) UpdateMerge(schemaName string, tableName string, rows [][]interface{}) {
	var schemaTable = SchemaTable{
		SchemaName: schemaName,
		TableName:  tableName,
	}
	var newDataMergeUnit = DataMergeUnit{
		firstOperation: 3,
		lastOperation:  3,
		oldRow:         rows[0],
		newRow:         rows[1],
	}
	oldCode := GetMD5(newDataMergeUnit.oldRow)
	newCode := GetMD5(newDataMergeUnit.newRow)
	tableMergeUnit, err := (*tableMergeUnits)[schemaTable]
	if err == false {
		//如果不存在这张表，则初始化表
		(*tableMergeUnits)[schemaTable] = map[string]*DataMergeUnit{
			newCode: &newDataMergeUnit,
		}
	} else {
		//如果存在表，则尝试获取旧的DataMergeUnit
		oldDataMergeUnit, err := tableMergeUnit[oldCode]
		if err == false {
			//如果不存在旧的DataMergeUnit，则插入新的DataMergeUnit
			(*tableMergeUnits)[schemaTable][newCode] = &newDataMergeUnit
		} else if oldDataMergeUnit.lastOperation != 2 {
			//如果存在旧的DataMergeUnit，则把新的DataMergeUnit的oldRow改为旧的DataMergeUnit的oldRow，然后用新的DataMergeUnit替换旧的DataMergeUnit
			newDataMergeUnit.oldRow = oldDataMergeUnit.oldRow
			newDataMergeUnit.firstOperation = oldDataMergeUnit.firstOperation
			(*tableMergeUnits)[schemaTable][newCode] = &newDataMergeUnit
			delete((*tableMergeUnits)[schemaTable], oldCode)
		} else {
			println("UpdateMerge 失败：不能更新删除的行")
		}
	}
}

func (tableMergeUnits *TableMergeUnits) InsertMerge(schemaName string, tableName string, rows [][]interface{}) {
	var schemaTable = SchemaTable{
		SchemaName: schemaName,
		TableName:  tableName,
	}
	var newDataMergeUnit = DataMergeUnit{
		firstOperation: 1,
		lastOperation:  1,
		oldRow:         make([]interface{}, 0),
		newRow:         rows[0],
	}
	newCode := GetMD5(newDataMergeUnit.newRow)
	tableMergeUnit, err := (*tableMergeUnits)[schemaTable]
	if err == false {
		//如果不存在这张表，则初始化表
		(*tableMergeUnits)[schemaTable] = map[string]*DataMergeUnit{
			newCode: &newDataMergeUnit,
		}
	} else {
		//如果存在表，则尝试获取旧的DataMergeUnit
		oldDataMergeUnit, err := tableMergeUnit[newCode]
		if err == false {
			//如果不存在旧的DataMergeUnit，则插入新的DataMergeUnit
			(*tableMergeUnits)[schemaTable][newCode] = &newDataMergeUnit
		} else if oldDataMergeUnit.lastOperation == 2 {
			//如果存在旧的DataMergeUnit，且旧的DataMergeUnit已经被删除，则相当于没有进行过任何操作
			delete((*tableMergeUnits)[schemaTable], newCode)
		} else {
			println("InsertMerge 失败：同一张表内存在重复数据")
		}
	}
}

func (tableMergeUnits *TableMergeUnits) DeleteMerge(schemaName string, tableName string, rows [][]interface{}) {
	var schemaTable = SchemaTable{
		SchemaName: schemaName,
		TableName:  tableName,
	}
	var newDataMergeUnit = DataMergeUnit{
		firstOperation: 2,
		lastOperation:  2,
		oldRow:         rows[0],
		newRow:         make([]interface{}, 0),
	}
	oldCode := GetMD5(newDataMergeUnit.oldRow)
	tableMergeUnit, err := (*tableMergeUnits)[schemaTable]
	//在删除操作中，newRow不存在，所以tableMergeUnit中使用oldCode作为map的索引
	if err == false {
		//如果不存在这张表，则初始化表
		(*tableMergeUnits)[schemaTable] = map[string]*DataMergeUnit{
			oldCode: &newDataMergeUnit,
		}
	} else {
		//如果存在表，则尝试获取旧的DataMergeUnit
		oldDataMergeUnit, err := tableMergeUnit[oldCode]
		if err == false {
			//如果不存在旧的DataMergeUnit，则插入新的DataMergeUnit
			(*tableMergeUnits)[schemaTable][oldCode] = &newDataMergeUnit
		} else if oldDataMergeUnit.lastOperation != 2 {
			//如果存在旧的DataMergeUnit，则把新的DataMergeUnit的oldRow改为旧的DataMergeUnit的oldRow，然后用新的DataMergeUnit替换旧的UpdateMerge
			newDataMergeUnit.oldRow = oldDataMergeUnit.oldRow
			newDataMergeUnit.firstOperation = oldDataMergeUnit.firstOperation
			(*tableMergeUnits)[schemaTable][oldCode] = &newDataMergeUnit
			delete((*tableMergeUnits)[schemaTable], oldCode)
		} else if oldDataMergeUnit.firstOperation == 1 {
			delete((*tableMergeUnits)[schemaTable], oldCode)
		} else {
			println("DeleteMerge 失败：同一张表内存不能重复删除数据")
		}
	}
}

func (tableMergeUnits *TableMergeUnits) Decode() string {
	var mergeBuffer = new(bytes.Buffer)
	for schemaTable, tableMergeUnit := range *tableMergeUnits {
		schemaName := schemaTable.SchemaName
		tableName := schemaTable.TableName
		columnNames := connect.GetMySQLTableMap(schemaTable.SchemaName, schemaTable.TableName)
		for _, dataMergeUnit := range tableMergeUnit {
			if dataMergeUnit.firstOperation == 1 {
				stmt := format.InsertMaker(schemaName, tableName, dataMergeUnit.newRow, columnNames)
				fmt.Fprintf(mergeBuffer, "%s\n", stmt)
				break
			}
			switch dataMergeUnit.lastOperation {
			case 2:
				stmt := format.DeleteMaker(schemaName, tableName, dataMergeUnit.oldRow, columnNames)
				fmt.Fprintf(mergeBuffer, "%s\n", stmt)
				break
			case 3:
				stmt := format.UpdateMaker(schemaName, tableName, dataMergeUnit.oldRow, dataMergeUnit.newRow, columnNames)
				fmt.Fprintf(mergeBuffer, "%s\n", stmt)
				break
			default:
				fmt.Println("merge解析错误")
				os.Exit(1)
			}
		}
	}
	str := fmt.Sprint(mergeBuffer)
	return str
}
