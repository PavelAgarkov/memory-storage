package memory_storage

import (
	bytes2 "bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
)

type roaringBitmapStorage struct {
	configs BitmapStorageConfigs
	//эффективно справляется до 100+ миллионов элементов без шардирования.
	//Так же нужно будет шардирование, если  будет высокая конкуренция на запись
	bitmap *roaring64.Bitmap
	mu     sync.RWMutex

	replicator MemorySetStorageReplicator // репликатор для репликации данных в запасное хранилище
	warmer     *Warmer                    // функция, которая будет вызвана для заполнения хранилища
}

type BitmapStorageConfigs struct {
	MonitoringTicker  time.Duration
	OptimizingTicker  time.Duration
	ReplicationTicker time.Duration
	ReplicationTtl    time.Duration
	StorageName       string
	DebugLogs         bool   // флаг для включения/отключения отладочных логов
	ReplicationKey    string // ключ для репликации, например, "bitmap_current_goods_ids"
}

func NewBitmapStorage(
	replicator MemorySetStorageReplicator,
	configs BitmapStorageConfigs,
	warmer *Warmer,
) MemorySetStorage {
	if warmer.BatchSize <= 0 {
		panic(fmt.Sprintf("[%s] warmer batch size must be greater than 0", configs.StorageName))
	}
	s := &roaringBitmapStorage{
		bitmap:     roaring64.NewBitmap(),
		configs:    configs,
		replicator: replicator,
		warmer:     warmer,
	}

	return s
}

func (s *roaringBitmapStorage) MustWarmer(ctx context.Context, warmerFunc WarmerFunc) {
	if s.warmer == nil {
		panic(fmt.Sprintf("[%s] warmer function cannot be nil", s.configs.StorageName))
	}
	if s.warmer.WarmCallback != nil {
		panic(fmt.Sprintf("[%s] warmer function already set", s.configs.StorageName))
	}
	s.warmer.WarmCallback = warmerFunc
	s.background(ctx, s.configs)
}

func (s *roaringBitmapStorage) Contains(key uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	hit := s.bitmap.Contains(key)
	if hit && s.withDebugLogs() {
		fmt.Println(fmt.Sprintf("[%s] contains key %d: %t", s.configs.StorageName, key, hit))
	}
	return hit
}

func (s *roaringBitmapStorage) UpsertMany(keys []uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bitmap.AddMany(keys)
	if s.withDebugLogs() {
		fmt.Println(fmt.Sprintf("[%s] upserted %d keys", s.configs.StorageName, len(keys)))
	}
}

func (s *roaringBitmapStorage) RemoveMany(keys []uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range keys {
		s.bitmap.Remove(k)
	}
	if s.withDebugLogs() {
		fmt.Println(fmt.Sprintf("[%s] removed %d keys", s.configs.StorageName, len(keys)))
	}
}

func (s *roaringBitmapStorage) GetCount() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bitmap.GetCardinality()
}

func (s *roaringBitmapStorage) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bitmap.Clear()
	if s.withDebugLogs() {
		fmt.Println(fmt.Sprintf("[%s] cleared roaring64 bitmap storage", s.configs.StorageName))
	}
}

// Warm заполняет хранилище данными. Логика прогревания зависит от того, пустое ли хранилище и решение принимается
// в функции warmer, которая должна быть установлена заранее с помощью MustWarmer
func (s *roaringBitmapStorage) Warm(ctx context.Context) error {
	isEmpty := s.isEmpty()

	if s.warmer == nil {
		panic(fmt.Sprintf("[%s] warmer function cannot be nil", s.configs.StorageName))
	}

	if isEmpty {
		if s.withDebugLogs() {
			fmt.Println(fmt.Sprintf("[%s] warming up roaring64 bitmap storage", s.configs.StorageName))
		}
		data, err := s.warmer.WarmCallback(ctx, s.warmer.BatchSize)
		if err != nil {
			return err
		}
		if s.withDebugLogs() {
			fmt.Println(fmt.Sprintf("[%s] warmer function executed", s.configs.StorageName))
		}

		if s.withDebugLogs() {
			fmt.Println(fmt.Sprintf("[%s] upserting data to roaring64 bitmap storage", s.configs.StorageName))
		}
		s.UpsertMany(data)
		if s.withDebugLogs() {
			fmt.Println(fmt.Sprintf("[%s] upserted data to roaring64 bitmap storage", s.configs.StorageName))
		}
	}
	return nil
}

// ReadFromBuffer считывает данные из буфера и инициализирует этим буфером битовую карту
func (s *roaringBitmapStorage) ReadFromBuffer(ctx context.Context, buffer *bytes2.Buffer) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.bitmap.ReadFrom(buffer)
	if err != nil {
		fmt.Println(fmt.Sprintf("[%s] failed to read from buffer", s.configs.StorageName))
		return 0, err
	}
	if s.withDebugLogs() {
		fmt.Println(fmt.Sprintf("[%s] read from buffer successfully: %d bytes", s.configs.StorageName, p))
	}
	return p, nil
}

// GetBytesFromBitmap возвращает байтовое представление битовой карты
func (s *roaringBitmapStorage) GetBytesFromBitmap() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.bitmap.IsEmpty() {
		return nil, nil
	}

	bitmapBytes, err := s.bitmap.ToBytes()
	if err != nil {
		return nil, err
	}
	if s.withDebugLogs() {
		fmt.Println(fmt.Sprintf("[%s] bitmap converted to bytes successfully: %d bytes", s.configs.StorageName, len(bitmapBytes)))
	}

	return bitmapBytes, nil
}

// Recover восстанавливает хранилище из байтового представления, полученного из репликатора
func (s *roaringBitmapStorage) Recover(ctx context.Context) error {
	err := s.replicator.Recover(ctx, s, s.configs.ReplicationKey)
	if err != nil {
		fmt.Println(fmt.Sprintf("[%s] failed to recover bitmap from bytes", s.configs.StorageName))
		return err
	}
	if s.withDebugLogs() {
		fmt.Println(fmt.Sprintf("[%s] bitmap recovered successfully: %s", s.configs.StorageName, s.printSize()))
	}
	return err
}

func (s *roaringBitmapStorage) Replicate(ctx context.Context) error {
	err := s.replicator.Replicate(ctx, s, s.configs.ReplicationKey, s.configs.ReplicationTtl)
	if err != nil {
		fmt.Println(fmt.Sprintf("[%s] failed replication bitmap", s.configs.StorageName))
	}
	if s.withDebugLogs() {
		if err == nil {
			fmt.Println(fmt.Sprintf("[%s] bitmap replication is done: %s", s.configs.StorageName, s.printSize()))
		}
	}
	return err
}

func (s *roaringBitmapStorage) DropReplicationKey(ctx context.Context) error {
	err := s.replicator.DropReplicationKey(ctx, s.configs.ReplicationKey)
	if err != nil {
		fmt.Println(fmt.Sprintf("[%s] failed to drop replication key", s.configs.StorageName))
		return err
	}

	return nil
}

func (s *roaringBitmapStorage) background(ctx context.Context, configs BitmapStorageConfigs) {
	GoRecover(
		ctx,
		func(localCtx context.Context) {
			monitoringTicker := time.NewTicker(configs.MonitoringTicker)
			defer monitoringTicker.Stop()
			optimizingTicker := time.NewTicker(configs.OptimizingTicker)
			defer optimizingTicker.Stop()
			replicationTicker := time.NewTicker(configs.ReplicationTicker)
			defer replicationTicker.Stop()

			for {
				select {
				case <-localCtx.Done():
					if s.withDebugLogs() {
						fmt.Println(fmt.Sprintf("[%s] context has done", s.configs.StorageName))
					}
					return
				case <-monitoringTicker.C:
					fmt.Println(localCtx, fmt.Sprintf("[%s] monitoring roaring64 bitmap storage", s.configs.StorageName))
				case <-optimizingTicker.C:
					s.optimize(localCtx)
				case <-replicationTicker.C:
					err := s.Replicate(localCtx)
					if err != nil {
						fmt.Println(localCtx, fmt.Sprintf("[%s] failed to replicate bitmap", s.configs.StorageName))
					}
				}
			}
		})
}

func (s *roaringBitmapStorage) optimize(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bitmap.RunOptimize()
	if s.withDebugLogs() {
		fmt.Println(ctx, fmt.Sprintf("[%s] optimized roaring64 bitmap storage", s.configs.StorageName))
	}
}

func (s *roaringBitmapStorage) isEmpty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bitmap.IsEmpty()
}

func (s *roaringBitmapStorage) printSize() string {
	bytes := s.getSizeBytes()
	count := s.GetCount()

	mb := float64(bytes) / 1024 / 1024
	str := fmt.Sprintf(
		"[%s] elements=%d → roaring64 size = %.2f MB (≈ %d bytes)",
		s.configs.StorageName, count, mb, bytes,
	)

	return str
}

func (s *roaringBitmapStorage) getSizeBytes() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bitmap.GetSizeInBytes()
}

func (s *roaringBitmapStorage) withDebugLogs() bool {
	return s.configs.DebugLogs
}
