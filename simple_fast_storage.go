package memory_storage

import "sync"

type byte8 [8]byte
type SimpleFastStorage struct {
	readwrite sync.RWMutex
	set       map[byte8]uint64
	storage   [][]byte
}

func NewSimpleFastStorage(approximately int64) *SimpleFastStorage {
	return &SimpleFastStorage{
		set:     make(map[byte8]uint64, approximately),
		storage: make([][]byte, 0, approximately),
	}
}

func (s *SimpleFastStorage) Add(index byte8, value []byte) {
	s.readwrite.Lock()
	defer s.readwrite.Unlock()
	if _, exists := s.set[index]; !exists {
		s.set[index] = uint64(len(s.storage))
		s.storage = append(s.storage, value)
	}
}

func (s *SimpleFastStorage) Get(index byte8) ([]byte, bool) {
	s.readwrite.RLock()
	defer s.readwrite.RUnlock()
	if pos, exists := s.set[index]; exists {
		return s.storage[pos], true
	}
	return nil, false
}
