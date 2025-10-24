package memory_storage

import (
	"bytes"
	"errors"
	"sync"
	"time"

	"github.com/google/btree"
)

type TtlBTree interface {
	UpsertAt(key []byte, ts time.Time) bool
	UpsertManyAt(keys [][]byte, at time.Time) int
	Delete(key []byte) bool
	DeleteMany(keys [][]byte) int
	Has(key []byte) bool
	GetLastWriteUnix(key []byte) (int64, bool)
	ForEach(callback func(key []byte, timestampUnixSeconds int64) bool) error
	Size() int
	Reset()
	PurgeExpiredAt(now time.Time, ttl time.Duration, maxToDelete int) int
	ListExpiredAt(now time.Time, ttl time.Duration, maxCount int) [][]byte
}

// Options — параметры инициализации дерева.
type Options struct {
	// Degree — степень B-дерева (обычно 16..64; по умолчанию 32).
	Degree int
	// Now — источник времени (для тестов/контроля таймстемпа); по умолчанию time.Now.
	Now func() time.Time
	// FreeListCapacity — максимальное количество узлов B-дерева,
	// которое кэшируется во внутреннем списке свободных узлов (free list).
	// Это НЕ байты. Чем больше значение, тем меньше аллокаций/GC при вставках/удалениях
	FreeListCapacity int
}

// ByteKeyBTree — потокобезопасное B-дерево для байтовых ключей.
type ByteKeyBTree struct {
	tree *btree.BTree
	mu   sync.RWMutex
	now  func() time.Time
}

// nodeItem — элемент дерева: ключ и момент последней записи.
type nodeItem struct {
	keyBytes             []byte
	timestampUnixSeconds int64
}

// Less — порядок для байтовых ключей.
func (a nodeItem) Less(b btree.Item) bool {
	return bytes.Compare(a.keyBytes, b.(nodeItem).keyBytes) < 0
}

// NewByteKeyBTree создаёт дерево с указанными опциями.
func NewByteKeyBTree(opts Options) TtlBTree {
	degree := opts.Degree
	if degree <= 0 {
		degree = 32
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	if opts.FreeListCapacity <= 0 {
		opts.FreeListCapacity = 80000
	}
	fl := btree.NewFreeList(opts.FreeListCapacity)

	return &ByteKeyBTree{
		tree: btree.NewWithFreeList(degree, fl),
		now:  nowFn,
	}
}

// cloneBytes — делает копию ключа
func cloneBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

// UpsertAt — вставка/обновление с заданным моментом времени.
// Возвращает true, если ключ был новым.
func (b *ByteKeyBTree) UpsertAt(key []byte, ts time.Time) bool {
	if len(key) == 0 {
		return false
	}
	item := nodeItem{
		keyBytes:             cloneBytes(key),
		timestampUnixSeconds: ts.Unix(),
	}
	b.mu.Lock()
	prev := b.tree.ReplaceOrInsert(item)
	b.mu.Unlock()

	return prev == nil
}

// UpsertManyAt — массовая вставка/обновление с заданным моментом времени.
// Возвращает число реально новых ключей.
func (b *ByteKeyBTree) UpsertManyAt(keys [][]byte, at time.Time) int {
	if len(keys) == 0 {
		return 0
	}
	sec := at.Unix()
	added := 0

	b.mu.Lock()
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		prev := b.tree.ReplaceOrInsert(nodeItem{
			keyBytes:             cloneBytes(key),
			timestampUnixSeconds: sec,
		})
		if prev == nil {
			added++
		}
	}
	b.mu.Unlock()

	return added
}

// Delete — удаление ключа. Возвращает true, если ключ существовал.
func (b *ByteKeyBTree) Delete(key []byte) bool {
	if len(key) == 0 {
		return false
	}
	probe := nodeItem{keyBytes: key}
	b.mu.Lock()
	deleted := b.tree.Delete(probe) != nil
	b.mu.Unlock()
	return deleted
}

// DeleteMany — массовое удаление. Возвращает число реально удалённых ключей.
func (b *ByteKeyBTree) DeleteMany(keys [][]byte) int {
	if len(keys) == 0 {
		return 0
	}
	deleted := 0
	b.mu.Lock()
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		if b.tree.Delete(nodeItem{keyBytes: key}) != nil {
			deleted++
		}
	}
	b.mu.Unlock()
	return deleted
}

// Has — проверка наличия ключа.
func (b *ByteKeyBTree) Has(key []byte) bool {
	if len(key) == 0 {
		return false
	}
	probe := nodeItem{keyBytes: key}
	b.mu.RLock()
	exists := b.tree.Get(probe) != nil
	b.mu.RUnlock()
	return exists
}

// GetLastWriteUnix — получить unix-секунды последней вставки/обновления.
// Возвращает (0,false), если ключ не найден.
func (b *ByteKeyBTree) GetLastWriteUnix(key []byte) (int64, bool) {
	probe := nodeItem{keyBytes: key}
	b.mu.RLock()
	res := b.tree.Get(probe)
	b.mu.RUnlock()
	if res == nil {
		return 0, false
	}
	return res.(nodeItem).timestampUnixSeconds, true
}

// ForEach — полный обход по возрастанию ключей.
// Если callback возвращает false — обход останавливается.
func (b *ByteKeyBTree) ForEach(callback func(key []byte, timestampUnixSeconds int64) bool) error {
	if callback == nil {
		return errors.New("nil callback")
	}
	keepGoing := true
	b.mu.RLock()
	b.tree.Ascend(func(x btree.Item) bool {
		it := x.(nodeItem)
		keyCopy := cloneBytes(it.keyBytes)
		keepGoing = callback(keyCopy, it.timestampUnixSeconds)
		return keepGoing
	})
	b.mu.RUnlock()
	return nil
}

// Size — текущее количество элементов.
func (b *ByteKeyBTree) Size() int {
	b.mu.RLock()
	n := b.tree.Len()
	b.mu.RUnlock()
	return n
}

// Reset — полная очистка дерева.
func (b *ByteKeyBTree) Reset() {
	b.mu.Lock()
	b.tree.Clear(true)
	b.mu.Unlock()
}

// PurgeExpiredAt — удалить ключи, чья последняя запись старше now - ttl.
// Если maxToDelete <= 0 — без лимита. Возвращает число удалённых.
func (b *ByteKeyBTree) PurgeExpiredAt(now time.Time, ttl time.Duration, maxToDelete int) int {
	if ttl <= 0 {
		return 0
	}
	cutoffUnix := now.Add(-ttl).Unix()

	var keysToDelete [][]byte
	b.mu.RLock()
	b.tree.Ascend(func(x btree.Item) bool {
		it := x.(nodeItem)
		if it.timestampUnixSeconds <= cutoffUnix {
			keysToDelete = append(keysToDelete, cloneBytes(it.keyBytes))
			if maxToDelete > 0 && len(keysToDelete) >= maxToDelete {
				return false
			}
		}
		return true
	})
	b.mu.RUnlock()

	if len(keysToDelete) == 0 {
		return 0
	}

	deleted := 0
	b.mu.Lock()
	for _, key := range keysToDelete {
		if b.tree.Delete(nodeItem{keyBytes: key}) != nil {
			deleted++
		}
	}
	b.mu.Unlock()

	return deleted
}

// ListExpiredAt — возвращает КОПИИ всех ключей, чей lastWriteUnix <= (now - ttl).
// Граница включительна. Ничего не удаляет.
// Если ttl <= 0 — возвращает nil.
// maxCount > 0 — ограничивает количество возвращаемых ключей, 0/отрицательное — без лимита.
func (b *ByteKeyBTree) ListExpiredAt(now time.Time, ttl time.Duration, maxCount int) [][]byte {
	if ttl <= 0 {
		return nil
	}
	cutoff := now.Add(-ttl).Unix()

	var out [][]byte
	limit := maxCount > 0

	b.mu.RLock()
	b.tree.Ascend(func(x btree.Item) bool {
		it := x.(nodeItem)
		if it.timestampUnixSeconds <= cutoff {
			k := cloneBytes(it.keyBytes)
			out = append(out, k)
			if limit && len(out) >= maxCount {
				return false
			}
		}
		return true
	})
	b.mu.RUnlock()

	return out
}
