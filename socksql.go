package socksql

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/Stachio/go-extdata"
	"github.com/fatih/structs"
)

var silent = false

//ToggleSilence - No
func ToggleSilence() (out bool) {
	silent = !silent
	return silent
}

//New - No
func New(username, password, server, port, schema string) (out *SockSQL, err error) {
	log.Println(fmt.Sprintf("Connecting to %s:%s:%s with user %s", server, port, schema, username))
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, server, port, schema))
	if err == nil {
		out = &SockSQL{db: db}
	}
	return
}

//Execute - No
func (ssql *SockSQL) Execute(query string, args ...interface{}) (err error) {
	if !silent {
		log.Println(fmt.Sprintf("Execute: %s %v", query, args))
	}
	statement, err := ssql.db.Prepare(query)
	if err == nil {
		_, err = statement.Exec(args...)
		statement.Close()
	}
	return
}

//Query - No
func (ssql *SockSQL) Query(query string, args ...interface{}) (out *sql.Rows, err error) {
	if !silent {
		log.Println(fmt.Sprintf("Query: %s %v", query, args))
	}
	statement, err := ssql.db.Prepare(query)
	if err == nil {
		out, err = statement.Query(args...)
		statement.Close()
	}
	return
}

/* Deprecated
func (ssql *SockSQL) InitTable(tableName string, stbl *SockSQLTable) {
	colItems := stbl.colItems
	columns := make([]string, len(colItems))
	counter := 0
	for colName, colDef := range colItems {
		columns[counter] = colName + " " + colDef
		//log.Println(columns[counter])
		counter++
	}
	ssql.Execute(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, strings.Join(columns, ", ")), false)
}
*/

//InitTableV2 - No
//Replacement for InitTable
func (ssql *SockSQL) InitTableV2(v interface{}, postName string, pluralize bool) {
	//Get the name of the table
	tableName := structs.Name(v) + postName
	if pluralize {
		tableName = tableName + "s"
	}
	//fieldNames := structs.Names(v)
	fields := structs.Fields(v)
	namesToFields := make(map[string]*structs.Field)

	log.Println(tableName)
	for _, field := range fields {
		namesToFields[field.Name()] = field
		log.Println(fmt.Sprintf("%s %s", field.Name(), field.Tag("sql")))
	}

	columns := make([]string, len(fields))
	for i := range columns {
		columns[i] = fields[i].Name() + " " + fields[i].Tag("sql")
		log.Println(columns[i])
	}

	query := "CREATE TABLE IF NOT EXISTS " + tableName + " (" + strings.Join(columns, ", ") + ")"
	ssql.Execute(query, false)

	var columnName string
	var columnNames []string
	query = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? and TABLE_NAME = ?"
	//ssql.Execute(query)
	sqlRows, _ := ssql.Query(query, false, "sockio-bot", tableName)
	for sqlRows.Next() {
		sqlRows.Scan(&columnName)
		columnNames = append(columnNames, columnName)
	}
	sqlRows.Close()

	// Remove columns
	for _, columnName = range columnNames {
		_, ok := namesToFields[columnName]
		if !ok {
			log.Println(columnName, "dropping from", tableName)
			query = "ALTER TABLE " + tableName + " DROP COLUMN " + columnName
			ssql.Execute(query, false)
		}
	}

	// Add columns
	for _, columnName = range structs.Names(v) {
		if !extdata.StringArrayContains(columnNames, columnName) {
			log.Println(columnName, "adding to", tableName)
			query = "ALTER TABLE " + tableName + " ADD COLUMN " + columnName + " " + namesToFields[columnName].Tag("sql")
			ssql.Execute(query, false)
		}
	}
}

/* Deprecated
func NewSockField(sl *SockLogger, name string, dataType int) *SockSQLField {
	return &SockSQLField{sl: sl, name: name, dataType: dataType}
}

func (ssqlf *SockSQLField) EnforceType(dataType int) {
	if ssqlf.dataType != dataType {
		message := "Expected " + SQLASSOC[dataType] + " datatype"
		ssqlf.sl.Panic(errors.New(message))
	}
}

func (ssqlf *SockSQLField) PutSQLInt(in int) {
	ssqlf.EnforceType(SQLINT)
	ssqlf.data = strconv.Itoa(in)
}

func (ssqlf *SockSQLField) GetSQLInt() (out int) {
	ssqlf.EnforceType(SQLINT)
	out, err := strconv.Atoi(ssqlf.data)
	ssqlf.sl.Panic(err)
	return
}

func (ssqlf *SockSQLField) PutSQLBlob(in []byte) {
	ssqlf.EnforceType(SQLBLOB)
	ssqlf.data = "'" + string(in) + "'"
}

func (ssqlf *SockSQLField) GetSQLBlob() []byte {
	ssqlf.EnforceType(SQLINT)
	return []byte(ssqlf.data[1 : len(ssqlf.data)-2])
}
*/

//func (ssql *SockSQL) UpdateTable()

//Close - No
func (ssql *SockSQL) Close() {
	ssql.db.Close()
}
