package sad

import (
	"errors"
	"fmt"
	"os"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type Migration struct {
	Version int
	Name    string
	SQL     string
}

type DBConnectionOptions struct {
	Reset             bool
	EnableForeignKeys bool
	DatabasePath      string
}

func applyMigration(db *sqlx.DB, migration *Migration) error {
	tx, err := db.Beginx()
	if err != nil {
		return fmt.Errorf("Failed to start transaction: %w", err)
	}
	_, err = db.Exec(migration.SQL)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			err = errors.Join(err, rollbackErr)
		}
		return err
	}
	//insert corresponding versions
	_, err = db.Exec(
		`
      insert into schema_versions(
        version
      ) values(
        ?
      );
    `,
		migration.Version,
	)
	if err != nil {
		rollbackErr := tx.Rollback()
		if rollbackErr != nil {
			err = errors.Join(err, rollbackErr)
		}
		return err
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("Failed to commit transaction: %w", err)
	}
	return nil
}

func runMigrations(db *sqlx.DB, migrations []Migration) error {
	appliedMigrations := map[int]bool{}
	dbSchemaVersions := []int{}
	const getSchemaVersionsQuery = "select version from schema_versions"
	err := db.Select(&dbSchemaVersions, getSchemaVersionsQuery)
	if err != nil {
		return err
	}
	for _, version := range dbSchemaVersions {
		appliedMigrations[version] = true
	}
	for _, migration := range migrations {
		if !appliedMigrations[migration.Version] {
			err := applyMigration(db, &migration)
			if err != nil {
				return fmt.Errorf("Failed to run migration %d: %w", migration.Version, err)
			}
		}
	}
	return nil
}

func ensureSchemaVersions(db *sqlx.DB) error {
	const query = `
    create table if not exists schema_versions (
      version integer primary key
    );
  `
	_, err := db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func OpenDBConnection(opts DBConnectionOptions, migrations []Migration) (*sqlx.DB, error) {
	if opts.DatabasePath == "" {
		return nil, errors.New("DatabasePath can not be an empty string")
	}
	if opts.Reset {
		err := os.Remove(opts.DatabasePath)
		if err != nil {
			return nil, fmt.Errorf("Failed to reset database: %w", err)
		}
	}
	db, err := sqlx.Connect("sqlite3", opts.DatabasePath)
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to database: %w", err)
	}
	if opts.EnableForeignKeys {
		const query = "PRAGMA foreign_keys=ON"
		_, err := db.Exec(query)
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("Failed to enable Foreign Keys: %w", err)
		}
	}
	if err := ensureSchemaVersions(db); err != nil {
		return nil, fmt.Errorf("EnsureSchemaVersion failde: %w", err)
	}
	if err := runMigrations(db, migrations); err != nil {
		db.Close()
		return nil, fmt.Errorf("Failed to run transactions: %w", err)
	}
	return db, nil
}
