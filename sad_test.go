package sad_test

import (
	"database/sql"
	"errors"
	"os"
	"testing"

	"github.com/joseph0x45/sad"
)

func TestCreateDatabaseNoAppName(t *testing.T) {
	_, err := sad.OpenDBConnection(sad.DBConnectionOptions{}, nil)
	if err == nil {
		t.Fatal("Expected connection to fail")
	}
}

func TestCreateDatabaseWithAppName(t *testing.T) {
	databasePath := "test.db"
	db, err := sad.OpenDBConnection(sad.DBConnectionOptions{
		DatabasePath: databasePath,
	}, nil)
	if err != nil {
		t.Fatal("Expected nil but got error", err.Error())
	}
	defer db.Close()
	if _, err := os.Stat(databasePath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Expected file %s to exist but doesn't", databasePath)
		}
	}
	//prepare next test
	_, err = db.Exec("create table dummy(dummy integer);")
	if err != nil {
		t.Fatal("Failed to prepare next test", err.Error())
	}
}

func TestCreateDatabaseReset(t *testing.T) {
	databasePath := "test.db"
	db, err := sad.OpenDBConnection(sad.DBConnectionOptions{
		DatabasePath: databasePath,
		Reset:        true,
	}, nil)
	if err != nil {
		t.Fatal("Expected nil but got error", err.Error())
	}
	defer db.Close()
	var name string
	err = db.Get(&name, `
    select name from sqlite_master where type='table' and name='dummy'
  `)
	if err == nil {
		t.Fatal("Expected table 'dummy' not to exist but it does")
	} else if err != sql.ErrNoRows {
		t.Fatal("Unexpected error checking for table existence:", err.Error())
	}
}

func TestCreateDatabaseForeignKeysEnabled(t *testing.T) {
	databasePath := "test.db"
	db, err := sad.OpenDBConnection(sad.DBConnectionOptions{
		DatabasePath:      databasePath,
		Reset:             true,
		EnableForeignKeys: true,
	}, nil)
	if err != nil {
		t.Fatal("Expected nil but got error", err.Error())
	}
	defer db.Close()
	var fkEnabled int
	err = db.Get(&fkEnabled, "PRAGMA foreign_keys")
	if err != nil {
		t.Fatal("Unexpected error checking for foreign_keys:", err.Error())
	}
	if fkEnabled != 1 {
		t.Fatal("Expected foreign keys to be enabled but are not")
	}
}

func TestMigrations(t *testing.T) {
	var testMigrations = []sad.Migration{
		{
			Version: 1,
			Name:    "create dummy table",
			SQL: `
            CREATE TABLE dummy (
                id INTEGER PRIMARY KEY,
                name TEXT
            );
        `,
		},
		{
			Version: 2,
			Name:    "add age column",
			SQL: `
            ALTER TABLE dummy ADD COLUMN age INTEGER DEFAULT 0;
        `,
		},
		{
			Version: 3,
			Name:    "insert initial row",
			SQL: `
            INSERT INTO dummy (name, age) VALUES ('Alice', 30);
        `,
		},
		{
			Version: 4,
			Name:    "create another table",
			SQL: `
            CREATE TABLE dummy2 (
                id INTEGER PRIMARY KEY,
                dummy_id INTEGER,
                FOREIGN KEY(dummy_id) REFERENCES dummy(id)
            );
        `,
		},
		{
			Version: 5,
			Name:    "insert row into dummy2",
			SQL: `
            INSERT INTO dummy2 (dummy_id) VALUES (1);
        `,
		},
	}

	databasePath := "test.db"
	db, err := sad.OpenDBConnection(sad.DBConnectionOptions{
		DatabasePath:      databasePath,
		EnableForeignKeys: true,
	}, testMigrations)
	if err != nil {
		t.Fatal("Expected nil but got error", err.Error())
	}
	defer db.Close()
	var latestVersion sql.NullInt64
	err = db.QueryRow("select MAX(version) from schema_versions").Scan(&latestVersion)
	if err != nil {
		t.Fatal("Expected nil but got error", err.Error())
	}
	if latestVersion.Valid {
		if latestVersion.Int64 != 5 {
			t.Log("Expected latestVersion to be 5 but got", latestVersion.Int64)
		}
	} else {
		t.Fatal("Failed to run migrations")
	}
}

func Cleanup(t *testing.T) {
	if err := os.Remove("test.db"); err != nil {
		t.Fatal("Cleanup Failed:", err.Error())
	}
}
