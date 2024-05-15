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
	"fmt"

	"github.com/oniony/TMSU/common"
)

// unexported

var latestSchemaVersion = schemaVersion{common.Version{0, 7, 0}, 1}

func currentSchemaVersion(tx *Tx) schemaVersion {
	sql := `
SELECT *
FROM version`

	var major, minor, patch, revision uint

	rows, err := tx.Query(sql)
	if err != nil {
		return schemaVersion{}
	}
	defer rows.Close()

	if rows.Next() && rows.Err() == nil {
		err := rows.Scan(&major, &minor, &patch, &revision)
		if err != nil {
			// might be prior to schema 0.7.0-1 where there was no revision column
			rows.Scan(&major, &minor, &patch) // ignore errors
		}
	}

	return schemaVersion{common.Version{major, minor, patch}, revision}
}

func insertSchemaVersion(tx *Tx, version schemaVersion) error {
	sql := `
INSERT INTO version (major, minor, patch, revision)
VALUES (?, ?, ?, ?)`

	result, err := tx.Exec(sql, version.Major, version.Minor, version.Patch, version.Revision)
	if err != nil {
		return fmt.Errorf("could not update schema version: %v", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("version could not be inserted: expected exactly one row to be affected")
	}

	return nil
}

func updateSchemaVersion(tx *Tx, version schemaVersion) error {
	sql := `
UPDATE version SET major = ?, minor = ?, patch = ?, revision = ?`
	cur := currentSchemaVersion(tx)
	if cur == version {
		return nil
	}
	result, err := tx.Exec(sql, version.Major, version.Minor, version.Patch, version.Revision)
	if err != nil {
		return fmt.Errorf("could not update schema version: %v", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("version could not be updated: expected exactly one row to be affected")
	}

	return nil
}

func createSchema(tx *Tx) error {
	if err := createTagTable(tx); err != nil {
		return err
	}

	if err := createFileTable(tx); err != nil {
		return err
	}

	if err := createValueTable(tx); err != nil {
		return err
	}

	if err := createFileTagTable(tx); err != nil {
		return err
	}

	if err := createImplicationTable(tx); err != nil {
		return err
	}

	if err := createQueryTable(tx); err != nil {
		return err
	}

	if err := createSettingTable(tx); err != nil {
		return err
	}

	if err := createVersionTable(tx); err != nil {
		return err
	}

	if err := insertDefaultValue(tx); err != nil {
		return err
	}

	if err := insertSchemaVersion(tx, latestSchemaVersion); err != nil {
		return err
	}

	return nil
}

func createTagTable(tx *Tx) error {
	sql := `
CREATE TABLE IF NOT EXISTS tag (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL
)`

	if _, err := tx.Exec(sql); err != nil {
		return err
	}

	sql = `
CREATE INDEX IF NOT EXISTS idx_tag_name
ON tag(name)`

	if _, err := tx.Exec(sql); err != nil {
		return err
	}

	return nil
}

func createFileTable(tx *Tx) error {
	sql := `
CREATE TABLE IF NOT EXISTS file (
    id INTEGER PRIMARY KEY,
    directory VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    fingerprint VARCHAR(255) NOT NULL,
    mod_time DATETIME NOT NULL,
    size INTEGER NOT NULL,
    is_dir BOOLEAN NOT NULL,
    CONSTRAINT con_file_path UNIQUE (directory, name)
)`

	if _, err := tx.Exec(sql); err != nil {
		return err
	}

	sql = `
CREATE INDEX IF NOT EXISTS idx_file_fingerprint
ON file(fingerprint)`

	if _, err := tx.Exec(sql); err != nil {
		return err
	}

	return nil
}

func createValueTable(tx *Tx) error {
	sql := `
CREATE TABLE IF NOT EXISTS ` + "`value`" + ` (
    id INTEGER PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    CONSTRAINT con_value_name UNIQUE (name)
)`

	if _, err := tx.Exec(sql); err != nil {
		return err
	}

	return nil
}

func createFileTagTable(tx *Tx) error {
	sql := `
CREATE TABLE IF NOT EXISTS file_tag (
    file_id INTEGER NOT NULL,
    tag_id INTEGER NOT NULL,
    value_id INTEGER NOT NULL,
    PRIMARY KEY (file_id, tag_id, value_id),
    FOREIGN KEY (file_id) REFERENCES file(id),
    FOREIGN KEY (tag_id) REFERENCES tag(id),
    FOREIGN KEY (value_id) REFERENCES ` + "`value`" + `(id)
)`

	if _, err := tx.Exec(sql); err != nil {
		return err
	}

	sql = `
CREATE INDEX IF NOT EXISTS idx_file_tag_file_id
ON file_tag(file_id)`

	if _, err := tx.Exec(sql); err != nil {
		return err
	}

	sql = `
CREATE INDEX IF NOT EXISTS idx_file_tag_tag_id
ON file_tag(tag_id)`

	if _, err := tx.Exec(sql); err != nil {
		return err
	}

	sql = `
CREATE INDEX IF NOT EXISTS idx_file_tag_value_id
ON file_tag(value_id)`

	if _, err := tx.Exec(sql); err != nil {
		return err
	}

	return nil
}

func createImplicationTable(tx *Tx) error {
	sql := `
CREATE TABLE IF NOT EXISTS implication (
    tag_id INTEGER NOT NULL,
    value_id INTEGER NOT NULL,
    implied_tag_id INTEGER NOT NULL,
    implied_value_id INTEGER NOT NULL,
    PRIMARY KEY (tag_id, value_id, implied_tag_id, implied_value_id)
)`

	if _, err := tx.Exec(sql); err != nil {
		return err
	}

	return nil
}

func createQueryTable(tx *Tx) error {
	sql := `
CREATE TABLE IF NOT EXISTS query (
    text VARCHAR(255) PRIMARY KEY
)`

	if _, err := tx.Exec(sql); err != nil {
		return err
	}

	return nil
}

func createSettingTable(tx *Tx) error {
	sql := `
CREATE TABLE IF NOT EXISTS setting (
    name VARCHAR(255) PRIMARY KEY,
    value VARCHAR(255) NOT NULL
)`

	if _, err := tx.Exec(sql); err != nil {
		return err
	}

	return nil
}

func createVersionTable(tx *Tx) error {
	sql := `
CREATE TABLE IF NOT EXISTS version (
    major INT NOT NULL,
    minor INT NOT NULL,
    patch INT NOT NULL,
    revision INT NOT NULL,
    PRIMARY KEY (major, minor, patch, revision)
)`

	if _, err := tx.Exec(sql); err != nil {
		return err
	}

	return nil
}

// Insert a default record into value table.
// Some Databases have foreign key constraints so we have to a record.
// ! ID: 0 should not be used but name must have a value else weird recursive loops occur in vfs
// TODO: Explicitly exclude ID: 0 from all queries
func insertDefaultValue(tx *Tx) error {
	sql := `
INSERT INTO ` + "`value`" + ` (id, name)
VALUES (?, ?)`
	result, err := tx.Exec(sql, 0, "dummy")
	if err != nil {
		return fmt.Errorf("could not insert default value: %v", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("default value could not be inserted: expected exactly one row to be affected")
	}
	return nil
}