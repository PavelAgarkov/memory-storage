package sdk

import (
	"context"
	"time"

	"github.com/dgraph-io/badger/v4"
)

var ErrNotFound = badger.ErrKeyNotFound

type Store struct {
	db *badger.DB
	Codec
	stopGC chan struct{}
}

func (s *Store) DB() *badger.DB {
	return s.db
}

func Open(ctx context.Context, opts Options, limit *MemoryLimit) (*Store, error) {
	bo := badger.DefaultOptions(opts.Dir)

	// Уровень логов
	switch opts.LoggingLevel {
	case LogDebug:
		bo = bo.WithLoggingLevel(badger.DEBUG)
	case LogInfo:
		bo = bo.WithLoggingLevel(badger.INFO)
	case LogWarning:
		bo = bo.WithLoggingLevel(badger.WARNING)
	default:
		bo = bo.WithLoggingLevel(badger.ERROR)
	}

	if opts.WithMetrics {
		bo.WithMetricsEnabled(true)
	}

	if opts.InMemory {
		bo = bo.WithInMemory(true)
	}
	if opts.ReadOnly {
		bo = bo.WithReadOnly(true)
	}
	if opts.ValueDir != "" {
		bo = bo.WithValueDir(opts.ValueDir)
	}
	if opts.SyncWrites {
		bo = bo.WithSyncWrites(true)
	}
	if opts.NumGoroutines > 0 {
		bo = bo.WithNumGoroutines(opts.NumGoroutines)
	}

	// Кеши
	if limit != nil && limit.BlockCacheSize > 0 {
		bo = bo.WithBlockCacheSize(limit.BlockCacheSize)
	} else {
		if opts.BlockCacheSize > 0 {
			bo = bo.WithBlockCacheSize(opts.BlockCacheSize)
		}
	}

	if limit != nil && limit.IndexCacheSize > 0 {
		bo = bo.WithIndexCacheSize(limit.IndexCacheSize)
	} else {
		if opts.IndexCacheSize > 0 {
			bo = bo.WithIndexCacheSize(opts.IndexCacheSize)
		}
	}

	if limit != nil && limit.MemTableSize > 0 {
		bo = bo.WithMemTableSize(limit.MemTableSize)
	} else {
		if opts.MemTableSize > 0 {
			bo = bo.WithMemTableSize(opts.MemTableSize)
		}
	}

	if limit != nil && limit.NumMemtables > 0 {
		bo = bo.WithNumMemtables(limit.NumMemtables)
	} else {
		if opts.NumMemtables > 0 {
			bo = bo.WithNumMemtables(opts.NumMemtables)
		}
	}

	if opts.ValueThreshold > 0 {
		bo = bo.WithValueThreshold(opts.ValueThreshold)
	}
	if opts.ValueLogFileSize > 0 {
		bo = bo.WithValueLogFileSize(opts.ValueLogFileSize)
	}
	if opts.BaseTableSize > 0 {
		bo = bo.WithBaseTableSize(opts.BaseTableSize)
	}

	if opts.NumCompactors > 0 {
		bo = bo.WithNumCompactors(opts.NumCompactors)
	}

	if opts.ZSTDCompressionLevel != 0 {
		bo = bo.WithZSTDCompressionLevel(opts.ZSTDCompressionLevel)
	}
	if opts.DetectConflicts {
		bo = bo.WithDetectConflicts(opts.DetectConflicts)
	}
	if len(opts.EncryptionKey) > 0 {
		bo = bo.WithEncryptionKey(opts.EncryptionKey)
	}

	db, err := badger.Open(bo)
	if err != nil {
		return nil, err
	}

	codec := opts.Codec
	if codec == nil {
		codec = JSONCodec{}
	}

	s := &Store{
		db:     db,
		Codec:  codec,
		stopGC: make(chan struct{}),
	}

	if opts.GCInterval > 0 && !opts.InMemory && !opts.ReadOnly {
		go func() {
			s.runGC(opts.GCInterval)
		}()
	}

	go func() {
		s.runMonitoring(ctx)
	}()

	return s, nil
}

func (s *Store) Close() error {
	close(s.stopGC)
	return s.db.Close()
}

func (s *Store) Set(key, value []byte, ttl time.Duration) error {
	return s.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(key, value)
		if ttl > 0 {
			e = e.WithTTL(ttl)
		}
		return txn.SetEntry(e)
	})
}

func (s *Store) Get(key []byte) ([]byte, error) {
	var out []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			out = append(out[:0], val...)
			return nil
		})
	})
	return out, err
}

func (s *Store) Delete(key []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}
