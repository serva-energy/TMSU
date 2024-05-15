// Copyright 2011-2018 Paul Ruane.

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package database

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3" // initialised Sqlite3
	"github.com/oniony/TMSU/common/log"
)

type Database struct {
	db *sql.DB
}

// Return scheme. The part before '://'.
// Example: return 'mysql' for 'mysql://db'
func GetScheme(path string) string {
	re := regexp.MustCompile(`\w+?:\/\/`)
	match := re.Find([]byte(path))
	if match == nil {
		return ""
	}
	return strings.Replace(string(match), "://", "", 1)
}

// Return the part after scheme or original string if not found
func SplitPathFromScheme(path string) string {
	splitPath := strings.SplitAfterN(path, "://", 2)
	if len(splitPath) != 2 {
		return path
	}
	return splitPath[1]
}

func HasScheme(path string) bool {
	scheme := GetScheme(path)
	return len(scheme) > 1
}

// Return the registered driver name for a 'sql.DB' or empty string if not registered
// Assume that the driver name is also the package name
// This should be the same name that is used in 'sql.Open'
// Example: return 'sqlite3' for driver with type '*sqlite3.SQLiteDriver'
func GetDriverName(db *sql.DB) string {
	dbDriverType := reflect.TypeOf(db.Driver()).Elem().String()
	dbDriverName := strings.Split(dbDriverType, ".")[0]
	for _, name := range sql.Drivers() {
		if name == dbDriverName {
			return name
		}
	}
	return ""
}

func OpenDB(path string) (*sql.DB, error) {
	if (HasScheme(path)) {
		scheme := GetScheme(path)
		dbPath := SplitPathFromScheme(path)
		return sql.Open(scheme, dbPath)
	}
	return sql.Open("sqlite3", path)
}

func CreateAt(path string) error {
	log.Infof(2, "creating database at '%v'.", path)

	_db, err := OpenDB(path)
	if err != nil {
		return DatabaseAccessError{path, err}
	}
	db := Database{_db}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return DatabaseTransactionError{path, err}
	}

	if err := upgrade(tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return DatabaseTransactionError{path, err}
	}

	return nil
}

func OpenAt(path string) (*Database, error) {
	log.Infof(2, "opening database at '%v'.", path)

	_, err := os.Stat(path)
	if err != nil && !HasScheme(path) {
		switch {
		case os.IsNotExist(err):
			return nil, DatabaseNotFoundError{path}
		default:
			return nil, DatabaseAccessError{path, err}
		}
	}

	_db, err := OpenDB(path)
	if err != nil {
		return nil, DatabaseAccessError{path, err}
	}
	db := Database{_db}
	tx, err := db.Begin()
	if err != nil {
		return nil, DatabaseTransactionError{path, err}
	}

	if err := upgrade(tx); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, DatabaseTransactionError{path, err}
	}

	return &db, nil
}

func (database *Database) Close() error {
	return database.db.Close()
}

func (database *Database) Begin() (*Tx, error) {
	tx, err := database.db.Begin()
	if err != nil {
		return nil, err
	}

	return &Tx{tx, GetDriverName(database.db)}, nil
}

type Tx struct {
	tx *sql.Tx
	driver string
}

func (tx *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	query = finalizeQuery(tx, query)
	log.Info(3, query)
	log.Infof(3, "params: %v", args)

	return tx.tx.Exec(query, args...)
}

func (tx *Tx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	query = finalizeQuery(tx, query)
	log.Info(3, query)
	log.Infof(3, "params: %v", args)

	return tx.tx.Query(query, args...)
}

func (tx *Tx) Commit() error {
	log.Info(2, "committing transaction")

	return tx.tx.Commit()
}

func (tx *Tx) Rollback() error {
	log.Info(2, "rolling back transaction")

	return tx.tx.Rollback()
}

// unexported

func readCount(rows *sql.Rows) (uint, error) {
	if !rows.Next() {
		return 0, errors.New("could not get count")
	}
	if rows.Err() != nil {
		return 0, rows.Err()
	}

	var count uint
	err := rows.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func collationFor(ignoreCase bool) string {
	if ignoreCase {
		return " COLLATE NOCASE"
	}

	return ""
}

func getCount(tx *Tx, table string, column string) (uint, error) {
	sql := fmt.Sprintf(`SELECT COUNT(%s) FROM %s`, column, table)
	rows, err := tx.Query(sql)
	if err != nil {
		return 0, err
	}
	count, err := readCount(rows)
	if err != nil {
		return 0, err
	}
	rows.Close()
	return count, err
}

func getNextId(tx *Tx, table string, idColumn string) (uint, error) {

	count, err := getCount(tx, table, idColumn)
	if count == 0 {
		return 1, err
	}
	sql := fmt.Sprintf(`SELECT MAX(%s) FROM %s`, idColumn, table)
	rows, err := tx.Query(sql)
	if err != nil {
		return 0, err
	}

	if !rows.Next() {
		return 0, fmt.Errorf("could not get next id for table '%s', column '%s'", table, idColumn)
	}
	if rows.Err() != nil {
		return 0, rows.Err()
	}
	var lastID uint
	err = rows.Scan(&lastID)
	rows.Close()
	return lastID + 1, err
}

func finalizeQuery(tx *Tx, query string) string {
	switch tx.driver {
	case "mysql":
		return compatMySql(query)
	default:
		return query
	}
}

func compatMySql(query string) string {
	// Fix parameters
	// TODO: Could be better
	query = regexp.MustCompile(`\?\d+`).ReplaceAllString(query, "?")
	// Fix INSERT OR IGNORE
	query = regexp.MustCompile(`INSERT\s+OR\s+IGNORE\s+`).ReplaceAllString(query, "INSERT IGNORE ")
	// Fix INSERT OR REPLACE
	query = regexp.MustCompile(`INSERT\s+OR\s+REPLACE\s+`).ReplaceAllString(query, "REPLACE ")
	// Remove '==' comparison
	query = regexp.MustCompile(`==`).ReplaceAllString(query, "=")
	return query
}
