package memory_storage

import (
	"testing"
	"time"
)

func TestBTreeStorage_AddGetDeleteCompact(t *testing.T) {
	s := NewBTreeIndexedStorage(16, 10, 0.3, 100) // degree=16, capacity=10, threshold=30%, batchSize=100

	// 1. Добавляем 3 записи
	k1 := key{1}
	k2 := key{2}
	k3 := key{3}
	v1 := []byte("foo")
	v2 := []byte("bar")
	v3 := []byte("baz")

	s.Add(k1, v1)
	s.Add(k2, v2)
	s.Add(k3, v3)

	if got, ok := s.Get(k2); !ok || string(got) != "bar" {
		t.Fatalf("expected bar, got %v", got)
	}

	if s.Len() != 3 {
		t.Fatalf("expected Len=3, got %d", s.Len())
	}

	// 2. Удаляем 1 запись
	s.Delete(k2)
	if _, ok := s.Get(k2); ok {
		t.Fatalf("expected k2 to be deleted")
	}

	// 3. Проверяем, что tombstone увеличился
	if s.tombstones == 0 {
		t.Fatalf("expected tombstones > 0, got 0")
	}

	// 4. Делаем Compact вручную
	s.CompactIncremental()
	if _, ok := s.Get(k2); ok {
		t.Fatalf("expected k2 removed after Compact")
	}
	if s.Len() != 2 {
		t.Fatalf("expected Len=2 after Compact, got %d", s.Len())
	}

	// 5. Проверим AutoCompact (удаляем >threshold)
	for i := 10; i < 20; i++ {
		k := key{byte(i)}
		s.Add(k, []byte{byte(i)})
	}
	for i := 10; i < 20; i++ {
		s.Delete(key{byte(i)})
	}
	// ждем пока Compact отработает в фоне
	time.Sleep(100 * time.Millisecond)

	if s.Len() != 2 { // должны остаться только k1 и k3
		t.Fatalf("expected Len=2 after AutoCompact, got %d", s.Len())
	}
}
