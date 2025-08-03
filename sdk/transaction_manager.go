package sdk

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type RWTx func(ctx context.Context, tx *badger.Txn) error
type RTx func(ctx context.Context, tx *badger.Txn) error

type TransactionManager interface {
	ExecuteReadOnly(fn RTx) error
	ExecuteReadWrite(fn RWTx) error
	ExecuteReadOnlyWithContext(ctx context.Context, fn RTx) error
	ExecuteReadWriteWithContext(ctx context.Context, fn RWTx) error
}

type Manager struct {
	store       *Store
	maxRetries  int
	baseBackoff time.Duration
	maxBackoff  time.Duration
}

type TxManagerOptions struct {
	MaxRetries  int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

func NewTransactionManager(store *Store, opts ...TxManagerOptions) *Manager {
	o := TxManagerOptions{
		MaxRetries:  5,
		BaseBackoff: 5 * time.Millisecond,
		MaxBackoff:  150 * time.Millisecond,
	}
	if len(opts) > 0 {
		if opts[0].MaxRetries > 0 {
			o.MaxRetries = opts[0].MaxRetries
		}
		if opts[0].BaseBackoff > 0 {
			o.BaseBackoff = opts[0].BaseBackoff
		}
		if opts[0].MaxBackoff > 0 {
			o.MaxBackoff = opts[0].MaxBackoff
		}
	}
	return &Manager{
		store:       store,
		maxRetries:  o.MaxRetries,
		baseBackoff: o.BaseBackoff,
		maxBackoff:  o.MaxBackoff,
	}
}

// Удобные обёртки без контекста (для совместимости)
func (m *Manager) ExecuteReadOnly(fn RTx) error {
	return m.store.db.View(func(tx *badger.Txn) error {
		return fn(context.Background(), tx)
	})
}

func (m *Manager) ExecuteReadWrite(fn RWTx) error {
	// Один проход без ретраев/контекстов
	tx := m.store.db.NewTransaction(true)
	defer tx.Discard()

	if err := fn(context.Background(), tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (m *Manager) ExecuteReadOnlyWithContext(ctx context.Context, action RTx) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return m.store.db.View(func(tx *badger.Txn) error {
		return action(ctx, tx)
	})
}

func (m *Manager) ExecuteReadWriteWithContext(ctx context.Context, action RWTx) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		tx := m.store.db.NewTransaction(true)

		runErr := func() (err error) {
			defer func() {
				if p := recover(); p != nil {
					err = fmt.Errorf("panic in RW txn: %v", p)
				}
			}()
			return action(ctx, tx)
		}()

		if runErr != nil {
			tx.Discard()
			return runErr
		}

		if err := ctx.Err(); err != nil {
			tx.Discard()
			return err
		}

		if err := tx.Commit(); err != nil {
			if errors.Is(err, badger.ErrConflict) && attempt < m.maxRetries {
				tx.Discard()
				if serr := sleepWithJitter(ctx, m.baseBackoff, m.maxBackoff, attempt+1); serr != nil {
					return serr
				}
				continue
			}
			tx.Discard()
			return err
		}

		return nil
	}
}

func sleepWithJitter(ctx context.Context, base, max time.Duration, attempt int) error {
	if attempt < 1 {
		attempt = 1
	}

	// экспоненциальный рост: base * 2^(attempt-1), с ограничением max
	backoff := base * time.Duration(1<<uint(attempt-1))
	if backoff > max {
		backoff = max
	}
	if backoff < 0 {
		backoff = 0
	}

	// полнодевятный джиттер: [0, backoff)
	j := backoff
	if j <= 0 {
		// сразу проверим контекст и выходим без сна
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	d := time.Duration(rand.Int63n(int64(j)))

	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
