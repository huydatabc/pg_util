package pg_util

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
)

func TestReconnect(t *testing.T) {
	var (
		dbURL                         = getURL(t)
		wg                            sync.WaitGroup
		ctx, cancel                   = context.WithCancel(context.Background())
		msgI                          = 0
		errorFired                    uint64
		connLossFired, reconnectFired uint64
	)
	defer cancel()
	wg.Add(2)
	connOpts, err := pgx.ParseConfig(dbURL)
	if err != nil {
		t.Fatal(err)
	}

	// Test channel is quoted (dots are illegal unquoted)
	const channel = "test.test"

	err = Listen(ListenOpts{
		ConnectionURL: dbURL,
		Channel:       channel,
		Context:       ctx,
		OnError: func(_ error) {
			atomic.StoreUint64(&errorFired, 1)
		},
		OnConnectionLoss: func() {
			atomic.StoreUint64(&connLossFired, 1)
		},
		OnReconnect: func() {
			atomic.StoreUint64(&reconnectFired, 1)
		},
		OnMsg: func(s string) error {
			defer wg.Done()

			std := fmt.Sprintf("message_%d", msgI)
			if s != std {
				t.Fatalf("invalid message: %s != %s", s, std)
			}
			msgI++

			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	conn, err := pgx.ConnectConfig(context.Background(), connOpts)
	if err != nil {
		t.Fatal(err)
	}

	notify := func(t *testing.T, msg string) {
		t.Helper()

		_, err = conn.Exec(
			context.Background(),
			`select pg_notify($1, $2)`,
			channel,
			msg,
		)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Send first message
	notify(t, "message_0")

	// Simulate disconnect
	_, err = conn.Exec(
		context.Background(),
		fmt.Sprintf(
			`SELECT pg_terminate_backend(pg_stat_activity.pid)
			FROM pg_stat_activity
			WHERE pg_stat_activity.datname = '%s'
			  AND pid <> pg_backend_pid();`,
			connOpts.Database,
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Send second message after the client reconnected
	time.Sleep(time.Second * 2)
	notify(t, "message_1")

	// Assert functions fired
	if atomic.LoadUint64(&errorFired) == 0 {
		t.Fatal("error handler did not fire")
	}
	if atomic.LoadUint64(&connLossFired) == 0 {
		t.Fatal("connection loss handler did not fire")
	}
	if atomic.LoadUint64(&reconnectFired) == 0 {
		t.Fatal("reconnection handler did not fire")
	}

	wg.Wait()
}
