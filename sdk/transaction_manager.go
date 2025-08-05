package sdk

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type RWTx func(ctx context.Context, tx *badger.Txn) error
type RTx func(ctx context.Context, tx *badger.Txn) error

type TransactionManager interface {
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

func (s *Store) TxSetObject(tx *badger.Txn, key []byte, v any) error {
	data, err := s.Marshal(v)
	if err != nil {
		return err
	}
	return tx.Set(key, data)
}

func (s *Store) TxGetObject(tx *badger.Txn, key []byte, v any) error {
	item, err := tx.Get(key)
	if err != nil {
		return err
	}
	return item.Value(func(val []byte) error {
		return s.Unmarshal(val, v)
	})
}

func sleepWithJitter(ctx context.Context, base, max time.Duration, attempt int) error {
	if attempt < 1 {
		attempt = 1
	}

	backoff := base * time.Duration(attempt)
	if max > 0 && backoff > max {
		backoff = max
	}
	if backoff <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	t := time.NewTimer(backoff)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
