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
	appName := "sad_test"
	db, err := sad.OpenDBConnection(sad.DBConnectionOptions{
		AppName: appName,
	}, nil)
	if err != nil {
		t.Fatal("Expected nil but got error", err.Error())
	}
	defer db.Close()
	dbPath := sad.GetDatabaseFilePath(appName)
	if _, err := os.Stat(dbPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Expected file %s to exist but doesn't", dbPath)
		}
	}
	//prepare next test
	_, err = db.Exec("create table dummy(dummy integer);")
	if err != nil {
		t.Fatal("Failed to prepare next test", err.Error())
	}
}

func TestCreateDatabaseReset(t *testing.T) {
	appName := "sad_test"
	db, err := sad.OpenDBConnection(sad.DBConnectionOptions{
		AppName: appName,
		Reset:   true,
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
	appName := "sad_test"
	db, err := sad.OpenDBConnection(sad.DBConnectionOptions{
		AppName:           appName,
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

func Cleanup(t *testing.T) {
	if err := os.Remove(sad.GetDatabaseFilePath("sad_test")); err != nil {
		t.Fatal("Cleanup Failed:", err.Error())
	}
}
