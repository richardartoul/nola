package registry

import (
	"context"
	"database/sql"
	"errors"
	"time"

	_ "github.com/lib/pq"
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

func newSQLKV(ctx context.Context, url string) (kv, error) {
    tableName := defaultTableName
    db, err := sql.Open("postgres", url) // TODO support selecting driver at runtime
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

    // TODO I believe there is a way to improve the indexing on this
    _, err = db.Exec("CREATE TABLE IF NOT EXISTS $1 (k BLOB PRIMARY KEY, v BLOB NOT NULL)", tableName)
	if err != nil {
		return nil, err
	}

	setStmt, err := db.Prepare("INSERT INTO $1 (k, v) VALUES ($2, $3) ON CONFLICT (k) DO UPDATE SET v = $3")
	if err != nil {
		return nil, err
	}

	getStmt, err := db.Prepare("SELECT v FROM $1 WHERE k = $2")
	if err != nil {
		return nil, err
	}

	deleteStmt, err := db.Prepare("DELETE FROM $1 where k = $2")
	if err != nil {
		return nil, err
	}

	rangeStmt, err := db.Prepare("SELECT (k, v) FROM $1 WHERE k LIKE $2%")
	if err != nil {
		return nil, err
	}

	return &sqlKV{
		db: db,
		sqlKVStatement: sqlKVStatement{
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
	}}, nil
}

func (sqlkv *sqlKV) transact(txfn func(transaction) (any, error)) (any, error) {
	tx, err := sqlkv.beginTransaction(context.TODO()) // TODO add context
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
	_, err := sqlkv.db.Exec("DELETE FROM $1", sqlkv.tableName)
	return err
}

type sqlTransactionKV struct {
	tx *sql.Tx
	*sqlKVStatement
}

func (sqltransactionkv *sqlKVStatement) put(ctx context.Context, key []byte, value []byte) error {
	_, err := sqltransactionkv.setStmt.ExecContext(ctx, sqltransactionkv.tableName, key, value)
	return err
}

func (sqltransactionkv *sqlKVStatement) get(ctx context.Context, key []byte) ([]byte, bool, error) {
	var data []byte

	err := sqltransactionkv.getStmt.QueryRowContext(ctx, sqltransactionkv.tableName, key).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	} else if err != nil {
		return nil, false, err
	}

	return data, true, nil
}

func (sqltransactionkv *sqlKVStatement) iterPrefix(ctx context.Context, prefix []byte, fn func(k, v []byte) error) error {
	rows, err := sqltransactionkv.rangeStmt.QueryContext(ctx, sqltransactionkv.tableName, prefix)
	if err != nil {
	    return err
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
