package memory_storage

import "testing"

func Test_simple_storage(t *testing.T) {
	storage := NewSimpleFastStorage(1000)

	key1 := byte8{0, 0, 0, 0, 0, 0, 0, 1}
	value1 := []byte("value1")
	storage.Add(key1, value1)

	retrievedValue1, exists1 := storage.Get(key1)
	if !exists1 {
		t.Errorf("Expected key1 to exist")
	}
	if string(retrievedValue1) != "value1" {
		t.Errorf("Expected value 'value1', got '%s'", string(retrievedValue1))
	}

	key2 := byte8{0, 0, 0, 0, 0, 0, 0, 2}
	value2 := []byte("value2")
	storage.Add(key2, value2)

	retrievedValue2, exists2 := storage.Get(key2)
	if !exists2 {
		t.Errorf("Expected key2 to exist")
	}
	if string(retrievedValue2) != "value2" {
		t.Errorf("Expected value 'value2', got '%s'", string(retrievedValue2))
	}

	// Test adding a duplicate key
	storage.Add(key1, []byte("newValue1"))
	retrievedValue3, exists3 := storage.Get(key1)
	if !exists3 {
		t.Errorf("Expected key1 to exist after duplicate add")
	}
	if string(retrievedValue3) != "value1" {
		t.Errorf("Expected value 'value1' after duplicate add, got '%s'", string(retrievedValue3))
	}

	// Test non-existing key
	key3 := byte8{0, 0, 0, 0, 0, 0, 0, 3}
	_, exists4 := storage.Get(key3)
	if exists4 {
		t.Errorf("Did not expect key3 to exist")
	}
}
