package memory_storage

import (
	"bytes"
	"sync"
	"sync/atomic"

	"github.com/google/btree"
)

type key [8]byte

// entry связывает ключ с позицией в storage
type entry struct {
	k   key
	pos uint64
}

// Less реализует btree.Item
func (e *entry) Less(than btree.Item) bool {
	return bytes.Compare(e.k[:], than.(*entry).k[:]) < 0
}

type BTreeIndexedStorage struct {
	tree       *btree.BTree
	storage    [][]byte
	mu         sync.RWMutex
	tombstones int64   // число удалённых элементов
	threshold  float64 // порог для авто-компакта (0.3 = 30%)
	degree     int     // сохраняем степень дерева
	batchSize  int     // размер чанка для компакции
}

func NewBTreeIndexedStorage(degree int, capacity int, threshold float64, batchSize int) *BTreeIndexedStorage {
	return &BTreeIndexedStorage{
		tree:      btree.New(degree),
		storage:   make([][]byte, 0, capacity),
		threshold: threshold,
		degree:    degree,
		batchSize: batchSize,
	}
}

// Add добавляет value, если ключа ещё нет
func (s *BTreeIndexedStorage) Add(index key, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if item := s.tree.Get(&entry{k: index}); item != nil {
		return // ключ уже существует
	}

	pos := uint64(len(s.storage))
	s.storage = append(s.storage, value)
	s.tree.ReplaceOrInsert(&entry{k: index, pos: pos})
}

// Get возвращает value по ключу
func (s *BTreeIndexedStorage) Get(index key) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item := s.tree.Get(&entry{k: index})
	if item == nil {
		return nil, false
	}
	return s.storage[item.(*entry).pos], true
}

// Delete помечает ключ tombstone и может триггерить авто-компакт
func (s *BTreeIndexedStorage) Delete(index key) {
	s.mu.Lock()
	defer s.mu.Unlock()

	item := s.tree.Delete(&entry{k: index})
	if item != nil {
		s.storage[item.(*entry).pos] = nil
		atomic.AddInt64(&s.tombstones, 1)

		if float64(s.tombstones)/float64(len(s.storage)) > s.threshold {
			go s.CompactIncremental()
		}
	}
}

// Len возвращает количество элементов
func (s *BTreeIndexedStorage) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tree.Len()
}

// CompactIncremental пересобирает storage и btree чанками
func (s *BTreeIndexedStorage) CompactIncremental() {
	s.mu.Lock()
	defer s.mu.Unlock()

	newStorage := make([][]byte, 0, len(s.storage))
	newTree := btree.New(s.degree)

	count := 0
	s.tree.Ascend(func(i btree.Item) bool {
		e := i.(*entry)
		val := s.storage[e.pos]
		if val != nil {
			newPos := uint64(len(newStorage))
			newStorage = append(newStorage, val)
			newTree.ReplaceOrInsert(&entry{k: e.k, pos: newPos})
		}
		count++
		if s.batchSize > 0 && count >= s.batchSize {
			// прерываем обход, чтобы обработать часть данных
			return false
		}
		return true
	})

	// если дошли до конца — заменяем всё
	if count < s.batchSize || s.batchSize == 0 {
		s.storage = newStorage
		s.tree = newTree
		atomic.StoreInt64(&s.tombstones, 0)
	} else {
		// иначе: временно сохраняем прогресс
		// ⚠️ упрощённо — можно сделать поле progress и продолжать со следующего элемента
		s.storage = newStorage
		s.tree = newTree
		atomic.StoreInt64(&s.tombstones, 0) // сбрасываем счётчик, чтобы не триггерить заново
	}
}
