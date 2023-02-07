package registry

import (
	"context"
	"errors"
	"fmt"
	"time"
	"database/sql"

	_ "github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	defaultTableName = "nola_kv"
	postgresKVDriverName = "postgres"
)

type sqlKV struct {
	db *sql.DB
	sqlKVStatement
}

type sqlKVStatement struct {
	tableName  string
	vst        time.Time
	setStmt    *sql.Stmt
	getStmt    *sql.Stmt
	deleteStmt *sql.Stmt
	rangeStmt  *sql.Stmt
}

func newPostgresSQLKV(dsn string) (kv, error) {
	tableName := defaultTableName
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open %q connection: %w", postgresKVDriverName, err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping database (%q), faild init: %w", postgresKVDriverName, err)
	}

	// XXX Could we possibly index the part of the prefix to improve the
	// performance of iterRange?
	_, err = db.Exec(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %v (k BYTEA PRIMARY KEY, v BYTEA NOT NULL);", defaultTableName))
	if err != nil {
		return nil, fmt.Errorf("failed to create table (%q): %w", postgresKVDriverName, err)
	}

	setStmt, err := db.Prepare(fmt.Sprintf("INSERT INTO %q (k, v) VALUES ($1,$2) ON CONFLICT (k) DO UPDATE SET v=$2;", tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to prepare insert statement (%q): %w", postgresKVDriverName, err)
	}

	getStmt, err := db.Prepare(fmt.Sprintf("SELECT v FROM %v WHERE k=$1;", tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to prepare select statement (%q): %w", postgresKVDriverName, err)
	}

	deleteStmt, err := db.Prepare(fmt.Sprintf("DELETE FROM %v where k=$1;", tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to prepare delete statement (%q): %w", postgresKVDriverName, err)
	}

	rangeStmt, err := db.Prepare(fmt.Sprintf("SELECT k, v FROM %v WHERE k LIKE $1;", tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to prepare range statement (%q): %w", postgresKVDriverName, err)
	}

	return &sqlKV{
		db: db,
		sqlKVStatement: sqlKVStatement{
			vst:        time.Now(),
			tableName:  tableName,
			setStmt:    setStmt,
			getStmt:    getStmt,
			deleteStmt: deleteStmt,
			rangeStmt:  rangeStmt,
		},
	}, nil
}

func (sqlkv *sqlKV) beginTransaction(ctx context.Context) (transaction, error) {
	tx, err := sqlkv.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction (%q): %w", postgresKVDriverName, err)
	}

	return &sqlTransactionKV{tx: tx, sqlKVStatement: &sqlKVStatement{
		tableName:  sqlkv.tableName,
		vst:        sqlkv.vst,
		setStmt:    tx.StmtContext(ctx, sqlkv.setStmt),
		getStmt:    tx.StmtContext(ctx, sqlkv.getStmt),
		deleteStmt: tx.StmtContext(ctx, sqlkv.deleteStmt),
		rangeStmt:  tx.StmtContext(ctx, sqlkv.rangeStmt),
	}}, nil
}

func (sqlkv *sqlKV) transact(txfn func(transaction) (any, error)) (any, error) {
	// This limits the total time of the transaction to 10sec including the time
	// to start/stop. FDB has a max 5sec transaction, which we're aiming for
	// here.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tx, err := sqlkv.beginTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed transact begin: %w", err)
	}

	rt, err := txfn(tx)
	if err != nil {
		return nil, err // user supplied error
	}

	err = tx.commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed transact commit: %w", err)
	}

	return rt, nil
}

func (sqlkv *sqlKV) close(ctx context.Context) error {
	return sqlkv.db.Close()
}

func (sqlkv *sqlKV) unsafeWipeAll() error {
	_, err := sqlkv.db.Exec("DELETE FROM $1;", defaultTableName)
	if err != nil {
		return fmt.Errorf("failed to wipe table: %w", err)
	}

	return nil
}

type sqlTransactionKV struct {
	tx *sql.Tx
	*sqlKVStatement
}

func (sqltransactionkv *sqlKVStatement) put(ctx context.Context, key []byte, value []byte) error {
	_, err := sqltransactionkv.setStmt.ExecContext(ctx, key, value, value)
	if err != nil {
		return fmt.Errorf("failed to put (%q): %w", postgresKVDriverName, err)
	}

	return nil
}

func (sqltransactionkv *sqlKVStatement) get(ctx context.Context, key []byte) ([]byte, bool, error) {
	var data []byte

	err := sqltransactionkv.getStmt.QueryRowContext(ctx, key).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, fmt.Errorf("failed to get kv: %w", err)
	}

	return data, true, nil
}

func (sqltransactionkv *sqlKVStatement) iterPrefix(ctx context.Context, prefix []byte, fn func(k, v []byte) error) error {
	rows, err := sqltransactionkv.rangeStmt.QueryContext(ctx, append(prefix, '%'))
	if err != nil {
		return fmt.Errorf("failed to execute iterprefix: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var key []byte
		var value []byte
		if err := rows.Scan(&key, &value); err != nil {
			return fmt.Errorf("failed to scan in iterPrefix: %w", err)
		}

		err = fn(key, value)
		if err != nil {
			return fmt.Errorf("failed to apply iterPrefix function %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to list rows when iterating prefix: %w", err)
	}

	return nil
}

func (sqltransactionkv *sqlTransactionKV) getVersionStamp() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var now time.Time
	err := sqltransactionkv.tx.QueryRowContext(ctx, "SELECT NOW() AT TIMEZONE 'UTC';").Scan(&now)
	if err != nil {
		return 0, fmt.Errorf("failed to get versionstamp (%q): %w", postgresKVDriverName, err)
	}

	return now.UTC().UnixMicro(), nil
}

func (sqltransactionkv *sqlTransactionKV) commit(ctx context.Context) error {
	err := sqltransactionkv.tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction (%q): %w", postgresKVDriverName, err)
	}

	return nil
}

func (sqltransactionkv *sqlTransactionKV) cancel(ctx context.Context) error {
	err := sqltransactionkv.tx.Rollback()
	if err != nil {
		return fmt.Errorf("failed to cancel transaction (%q): %w", postgresKVDriverName, err)
	}

	return nil
}
