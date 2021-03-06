package pg_util

import (
	"context"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"reflect"
)

// Interface required to start a transaction or subtransation via savepoints
type TxStarter interface {
	Begin(context.Context) (pgx.Tx, error)
}

// Data struct encapsulate info for insert or upsert and cache checking
type Data struct {
	table, prefix, suffix, constrain string
	typ                              reflect.Type
}

// InTransaction runs a function inside a transaction and handles commiting
// and rollback on error.
//
// Can also be used for nested pseudotransactions via savepoints.
//
// ctx: Context to bind the query to
// conn: Anything, that can start a new transaction or subtransaction.
// fn: Function to execute on the transaction.
func InTransaction(
	ctx context.Context,
	conn TxStarter,
	fn func(pgx.Tx) error,
) (err error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return
	}
	panicked := true
	defer func() {
		if panicked {
			tx.Rollback(ctx)
		}
	}()

	err = fn(tx)
	if err != nil {
		goto end
	}

	err = tx.Commit(ctx)
end:
	panicked = false
	return
}

// Execute all SQL statement strings and return on first error, if any.
func ExecAll(ctx context.Context, tx pgx.Tx, q ...string) error {
	for _, q := range q {
		if _, err := tx.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// Try to extract an exception message, if err is *pgconn.PgError
func ExtractException(err error) string {
	if err, ok := err.(*pgconn.PgError); ok {
		return err.Message
	}
	return ""
}
