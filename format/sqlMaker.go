package format

import (
	"bytes"
	"fmt"
)

func InsertMaker(schemaName string, tableName string, row []interface{}, columnNames []string) string {
	var strValueNames = new(bytes.Buffer)
	var strValues = new(bytes.Buffer)
	for idx, colName := range columnNames {
		if idx == 0 {
			fmt.Fprintf(strValueNames, "`%s`", colName)
		} else {
			fmt.Fprintf(strValueNames, ", `%s`", colName)
		}
	}
	for idx, colValue := range row {
		if idx == 0 {
			fmt.Fprintf(strValues, "%s", typeCovert(colValue))
		} else {
			fmt.Fprintf(strValues, ", %s", typeCovert(colValue))
		}
	}
	stmt := fmt.Sprintf("INSERT INTO `%s`.`%s`(%s) VALUES (%s);", schemaName, tableName, strValueNames, strValues)
	return stmt
}

func UpdateMaker(schemaName string, tableName string, rowOld []interface{}, rowNew []interface{}, columnNames []string) string {
	var setStmt = new(bytes.Buffer)
	var whereStmt = new(bytes.Buffer)
	for idx, colName := range columnNames {
		if idx == 0 {
			fmt.Fprintf(setStmt, "`%s` = %s", colName, typeCovert(rowNew[idx]))
		} else {
			fmt.Fprintf(setStmt, ", `%s` = %s", colName, typeCovert(rowNew[idx]))
		}
	}
	for idx, colName := range columnNames {
		if idx == 0 {
			fmt.Fprintf(whereStmt, "`%s` = %s", colName, typeCovert(rowOld[idx]))
		} else {
			fmt.Fprintf(whereStmt, " AND `%s` = %s", colName, typeCovert(rowOld[idx]))
		}
	}
	stmt := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s;", schemaName, tableName, setStmt, whereStmt)
	return stmt
}

func DeleteMaker(schemaName string, tableName string, row []interface{}, columnNames []string) string {
	var whereStmt = new(bytes.Buffer)
	for idx, colName := range columnNames {
		if idx == 0 {
			fmt.Fprintf(whereStmt, "`%s` = %s", colName, typeCovert(row[idx]))
		} else {
			fmt.Fprintf(whereStmt, " AND `%s` = %s", colName, typeCovert(row[idx]))
		}
	}
	stmt := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s;", schemaName, tableName, whereStmt)
	return stmt
}

func typeCovert(val interface{}) string {
	switch v := val.(type) {
	case string:
		return fmt.Sprintf("'%s'", v)
	case nil:
		return fmt.Sprint("Null")
	default:
		return fmt.Sprint(v)
	}
}
