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

package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/oniony/TMSU/common/log"
	"github.com/oniony/TMSU/storage"
	"github.com/oniony/TMSU/storage/database"
)

var InitCommand = Command{
	Name:     "init",
	Synopsis: "Initializes a new database",
	Usages:   []string{"tmsu init [PATH] [--root-path=[ROOTPATH]]"},
	Description: `Initializes a new local database.

Creates a .tmsu directory under PATH and initialises a new empty database within it.

If no PATH is specified then the current working directory is assumed.

The new database is used automatically whenever TMSU is invoked from a directory under PATH (unless overridden by the global --database option or the TMSU_DB environment variable.`,
	Options: Options{Option{"--root-path", "-P", "root path to use for relative paths; only for networked databases", true, ""},},
	Exec:    initExec,
}

// unexported

func initExec(options Options, args []string, databasePath string) (error, warnings) {
	paths := args

	if database.HasScheme(databasePath) {
		paths = []string{databasePath}
	} else if len(paths) == 0 {
		workingDirectory, err := os.Getwd()
		if err != nil {
			return fmt.Errorf("could not identify working directory: %v", err), nil
		}

		paths = []string{workingDirectory}
	}

	warnings := make(warnings, 0, 10)
	for _, path := range paths {
		if err := initializeDatabase(path); err != nil {
			warnings = append(warnings, fmt.Sprintf("%v: could not initialize database: %v", path, err))
		} else {
			if err := insertRootPath(options, path); err != nil {
				warnings = append(warnings, fmt.Sprintf("%v: could not initialize database with root path: %v", path, err))
			}
		}
	}

	return nil, warnings
}

func initializeDatabase(path string) error {
	log.Warnf("%v: creating database", path)
	var dbPath string = path
	if !database.HasScheme(path) {
		tmsuPath := filepath.Join(path, ".tmsu")
		os.Mkdir(tmsuPath, 0755)

		dbPath = filepath.Join(tmsuPath, "db")
	}

	return storage.CreateAt(dbPath)
}

func insertRootPath(options Options, path string) error {
	if database.HasScheme(path) && options.HasOption("--root-path") {
		db, err := database.OpenAt(path)
		if err != nil {
			return err
		}

		tx, err := db.Begin()
		if err != nil {
			return err
		}

		defer tx.Commit()
		defer db.Close()

		_, err = database.UpdateSetting(tx, "rootPath", options.Get("--root-path").Argument)
		return err
	}
	return nil
}