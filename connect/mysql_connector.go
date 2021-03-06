package connect

import (
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
	"atk_binlog_parser/util"
)

var TableSchemaCache = make(map[string][]string)


func GetConn(DSN string) (*sql.DB, error) {
	db, err := sql.Open("mysql", DSN)
	if err != nil {
		panic(err.Error())
	}
	//DB = db
	return db, nil
}

func GetMySQLTableMap(schemaName string, tableName string) []string {
	fullName := schemaName + "." + tableName
	columnNames := TableSchemaCache[fullName]
	return columnNames
}

func GetAllTableSchema() map[string][]string {
	config := util.GetConfig()
	dsn := config.DSN
	DB, err := GetConn(dsn)
	if err != nil {
		panic(err.Error())
	}
	defer DB.Close()
	rows, err := DB.Query("SELECT TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA NOT IN ('sys','performance_schema','mysql','information_schema')")
	if err != nil {
		panic(err.Error())
	}
	var schemaName string
	var tableName string
	var columnName string
	for rows.Next() {
		err = rows.Scan(&schemaName, &tableName, &columnName)
		if err != nil {
			panic(err.Error())
		}
		fullName := schemaName + "." + tableName
		TableSchemaCache[fullName] = append(TableSchemaCache[fullName], columnName)
	}
	return TableSchemaCache
}
