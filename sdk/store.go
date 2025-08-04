package sdk

import (
	"context"
	"log"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type Store struct {
	db     *badger.DB
	stopGC chan struct{}
}

func (s *Store) DB() *badger.DB {
	return s.db
}

type LogLevel string

const (
	LogDebug   LogLevel = "DEBUG"
	LogInfo    LogLevel = "INFO"
	LogWarning LogLevel = "WARNING"
	LogError   LogLevel = "ERROR"
)

type Options struct {
	Dir         string
	ValueDir    string
	InMemory    bool
	ReadOnly    bool
	WithMetrics bool

	GCInterval    time.Duration
	SyncWrites    bool
	NumGoroutines int
	LoggingLevel  LogLevel

	// Кеши/размеры и прочее (v4)
	BlockCacheSize int64
	IndexCacheSize int64

	// Порог/размеры (все int64 в v4)
	ValueThreshold   int64
	ValueLogFileSize int64
	BaseTableSize    int64 // ← вместо MaxTableSize
	MemTableSize     int64
	NumMemtables     int
	NumCompactors    int

	ZSTDCompressionLevel int
	DetectConflicts      bool
	EncryptionKey        []byte
}

func (s *Store) StartBadgerMemStats() {
	bc := s.db.BlockCacheMetrics()
	ic := s.db.IndexCacheMetrics()

	// текущая загрузка (не уходим в минус)
	blockUsed := int64(0)
	if a, e := int64(bc.CostAdded()), int64(bc.CostEvicted()); a > e {
		blockUsed = a - e
	}
	indexUsed := int64(0)
	if a, e := int64(ic.CostAdded()), int64(ic.CostEvicted()); a > e {
		indexUsed = a - e
	}

	blockCap := s.db.Opts().BlockCacheSize
	indexCap := s.db.Opts().IndexCacheSize
	lsmSize, vlogSize := s.db.Size() // байты

	pct := func(used, cap int64) int {
		if cap <= 0 {
			return 0
		}
		return int((float64(used) / float64(cap)) * 100.0)
	}
	mib := func(b int64) int64 { return b >> 20 }

	log.Printf(
		"[Badger]\n"+
			"  BlockCache: used=%d MiB / %d MiB (%d%%), hits=%d, misses=%d\n"+
			"  IndexCache: used=%d MiB / %d MiB (%d%%), hits=%d, misses=%d\n"+
			"  OnDisk:     LSM=%d MiB, VLog=%d MiB",
		mib(blockUsed), mib(blockCap), pct(blockUsed, blockCap), bc.Hits(), bc.Misses(),
		mib(indexUsed), mib(indexCap), pct(indexUsed, indexCap), ic.Hits(), ic.Misses(),
		mib(lsmSize), mib(vlogSize),
	)
}

func Open(opts Options) (*Store, error) {
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
	if opts.BlockCacheSize > 0 {
		bo = bo.WithBlockCacheSize(opts.BlockCacheSize)
	}
	if opts.IndexCacheSize > 0 {
		bo = bo.WithIndexCacheSize(opts.IndexCacheSize)
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
	if opts.MemTableSize > 0 {
		bo = bo.WithMemTableSize(opts.MemTableSize)
	}
	if opts.NumMemtables > 0 {
		bo = bo.WithNumMemtables(opts.NumMemtables)
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
	s := &Store{db: db, stopGC: make(chan struct{})}
	if opts.GCInterval > 0 {
		go s.runGC(opts.GCInterval)
	}
	return s, nil
}

func (s *Store) Close() error {
	close(s.stopGC)
	return s.db.Close()
}

// Базовые операции
func (s *Store) Set(ctx context.Context, key, value []byte, ttl time.Duration) error {
	return s.db.Update(func(txn *badger.Txn) error {
		e := badger.NewEntry(key, value)
		if ttl > 0 {
			e = e.WithTTL(ttl) // TTL поддерживается на уровне Entry
		}
		return txn.SetEntry(e)
	})
}

var ErrNotFound = badger.ErrKeyNotFound

func (s *Store) Get(ctx context.Context, key []byte) ([]byte, error) {
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

func (s *Store) Delete(ctx context.Context, key []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}
