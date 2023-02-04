package registry

import (
	"context"
	"database/sql"
	"errors"
	"time"
    "fmt"
)

// TODO: hardcoded for now, but may be useful as a configuration option for
// some type of isolation
const defaultTableName = "nola_kv"

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

func newSQLKV(driver, dsn string) (kv, error) {
    tableName := defaultTableName
    db, err := sql.Open(driver, dsn) // TODO support selecting driver at runtime
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(0)

	err = db.Ping()
	if err != nil {
		return nil, err
	}

    // TODO Improve indexing
    _, err = db.Exec("CREATE TABLE IF NOT EXISTS nola_kv (k BLOB PRIMARY KEY, v BLOB NOT NULL);")
	if err != nil {
        return nil, fmt.Errorf("failed to create table: %w", err)
	}

	setStmt, err := db.Prepare("INSERT INTO nola_kv (k, v) VALUES (?,?) ON CONFLICT (k) DO UPDATE SET v=?;")
	if err != nil {
        return nil, fmt.Errorf("failed to prepare insert statement: %w", err)
	}

	getStmt, err := db.Prepare("SELECT v FROM nola_kv WHERE k=?;")
	if err != nil {
        return nil, fmt.Errorf("failed to prepare select statement: %w", err)
	}

	deleteStmt, err := db.Prepare("DELETE FROM nola_kv where k=?;")
	if err != nil {
        return nil, fmt.Errorf("failed to prepare delete statement: %w", err)
	}

	rangeStmt, err := db.Prepare("SELECT k, v FROM nola_kv WHERE k LIKE ?;")
	if err != nil {
		return nil, fmt.Errorf("failed to prepare range statement: %w", err)
	}

	return &sqlKV{
		db: db,
		sqlKVStatement: sqlKVStatement{
			vst: time.Now(),
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
		return nil, err
	}

	return &sqlTransactionKV{tx: tx, sqlKVStatement: &sqlKVStatement{
		tableName:  sqlkv.tableName,
		vst:        sqlkv.vst,
		setStmt:    tx.StmtContext(ctx, sqlkv.setStmt),
		getStmt:    tx.StmtContext(ctx, sqlkv.getStmt),
		deleteStmt: tx.StmtContext(ctx, sqlkv.deleteStmt),
		rangeStmt: tx.StmtContext(ctx, sqlkv.rangeStmt),
	}}, nil
}

func (sqlkv *sqlKV) transact(txfn func(transaction) (any, error)) (any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 5)
	defer cancel()
	tx, err := sqlkv.beginTransaction(ctx)
	if err != nil {
		return nil, err
	}

	rt, err := txfn(tx)
	if err != nil {
		return nil, err
	}

	err = tx.commit(context.TODO())
	if err != nil {
		return nil, err
	}

	return rt, nil
}

func (sqlkv *sqlKV) close(ctx context.Context) error {
	return sqlkv.db.Close()
}

func (sqlkv *sqlKV) unsafeWipeAll() error {
	_, err := sqlkv.db.Exec("DELETE FROM nola_kv;")
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
	return err
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
		    return err
        }

        err = fn(key, value)
        if err != nil {
            return err
        }
	}

	if err := rows.Err(); err != nil {
	    return err
    }

    return nil
}

func (sqltransactionkv *sqlTransactionKV) getVersionStamp() (int64, error) {
	return time.Since(sqltransactionkv.vst).Microseconds(), nil
}

func (sqltransactionkv *sqlTransactionKV) commit(ctx context.Context) error {
	return sqltransactionkv.tx.Commit()
}

func (sqltransactionkv *sqlTransactionKV) cancel(ctx context.Context) error {
	return sqltransactionkv.tx.Rollback()
}
