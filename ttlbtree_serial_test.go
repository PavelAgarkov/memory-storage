package memory_storage

import (
	"bytes"
	"testing"
	"time"
)

func assertTrue(t *testing.T, cond bool, msg string, args ...any) {
	t.Helper()
	if !cond {
		t.Fatalf(msg, args...)
	}
}
func assertEqualInt(t *testing.T, got, want int, name string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got=%d want=%d", name, got, want)
	}
}
func assertEqualBool(t *testing.T, got, want bool, name string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got=%v want=%v", name, got, want)
	}
}
func assertEqualBytes(t *testing.T, got, want []byte, name string) {
	t.Helper()
	if !bytes.Equal(got, want) {
		t.Fatalf("%s: got=%q want=%q", name, string(got), string(want))
	}
}

// фиксированные метки времени (UTC) для детерминизма
var (
	t0  = time.Unix(1_700_000_000, 0).UTC()
	t1  = t0.Add(10 * time.Second)
	t2  = t0.Add(20 * time.Second)
	now = t0.Add(1 * time.Hour)
)

func newTree() *ByteKeyBTree {
	return NewByteKeyBTree(Options{Now: time.Now}).(*ByteKeyBTree)
}

func TestByteKeyBTree_UpsertAt_InsertAndUpdate(t *testing.T) {
	tree := newTree()
	key := []byte("a")

	isNew := tree.UpsertAt(key, t0)
	assertEqualBool(t, isNew, true, "first UpsertAt must insert")

	isNewAgain := tree.UpsertAt(key, t1)
	assertEqualBool(t, isNewAgain, false, "second UpsertAt must update existing")

	ts, ok := tree.GetLastWriteUnix(key)
	assertEqualBool(t, ok, true, "GetLastWriteUnix(a) must exist")
	assertEqualInt(t, int(ts), int(t1.Unix()), "updated timestamp must equal t1")
}

func TestByteKeyBTree_UpsertManyAt_IgnoresEmptyAndCountsNew(t *testing.T) {
	tree := newTree()
	kb, kc := []byte("b"), []byte("c")

	n := tree.UpsertManyAt([][]byte{kb, kc, nil, {}}, t2)
	assertEqualInt(t, n, 2, "UpsertManyAt must return count of new keys")

	assertEqualInt(t, tree.Size(), 2, "Size after UpsertManyAt([b,c,nil,empty])")
	assertEqualBool(t, tree.Has(kb), true, "Has(b)")
	assertEqualBool(t, tree.Has(kc), true, "Has(c)")
}

func TestByteKeyBTree_Has_And_GetLastWriteUnix(t *testing.T) {
	tree := newTree()
	kx := []byte("x")
	assertEqualBool(t, tree.Has(kx), false, "Has(x) on empty tree")

	ok := tree.UpsertAt(kx, t0)
	assertEqualBool(t, ok, true, "insert x")

	assertEqualBool(t, tree.Has(kx), true, "Has(x) after insert")

	ts, ok2 := tree.GetLastWriteUnix(kx)
	assertEqualBool(t, ok2, true, "GetLastWriteUnix(x) exists")
	assertEqualInt(t, int(ts), int(t0.Unix()), "timestamp(x)==t0")
}

func TestByteKeyBTree_ForEach_OrderIsLexicographic_AndKeyIsCopy(t *testing.T) {
	tree := newTree()
	ka, kb, kc := []byte("a"), []byte("b"), []byte("c")
	tree.UpsertAt(kb, t0)
	tree.UpsertAt(ka, t0)
	tree.UpsertAt(kc, t0)

	// 1) проверяем порядок: a < b < c
	var keys [][]byte
	err := tree.ForEach(func(key []byte, marked int64) bool {
		keys = append(keys, key)
		return true
	})
	assertTrue(t, err == nil, "ForEach error: %v", err)
	assertEqualInt(t, len(keys), 3, "ForEach count")
	assertEqualBytes(t, keys[0], ka, "ForEach[0] must be a")
	assertEqualBytes(t, keys[1], kb, "ForEach[1] must be b")
	assertEqualBytes(t, keys[2], kc, "ForEach[2] must be c")

	// 2) убеждаемся, что наружу отдают КОПИЮ ключа (мутация не влияет на дерево)
	_ = tree.ForEach(func(key []byte, _ int64) bool {
		if len(key) > 0 {
			key[0] ^= 0xFF // портим копию
		}
		return true
	})
	// повторный обход — должен вернуть исходные значения
	keys = keys[:0]
	_ = tree.ForEach(func(key []byte, _ int64) bool {
		keys = append(keys, key)
		return true
	})
	assertEqualBytes(t, keys[0], ka, "ForEach after external mutation [0]")
	assertEqualBytes(t, keys[1], kb, "ForEach after external mutation [1]")
	assertEqualBytes(t, keys[2], kc, "ForEach after external mutation [2]")

	// 3) ранняя остановка (callback=false)
	count := 0
	_ = tree.ForEach(func(_ []byte, _ int64) bool {
		count++
		return count < 2
	})
	assertEqualInt(t, count, 2, "ForEach early stop after 2 items")
}

func TestByteKeyBTree_PurgeExpiredAt_InclusiveBoundary(t *testing.T) {
	tree := newTree()
	ka, kb, kc := []byte("a"), []byte("b"), []byte("c")

	tree.UpsertAt(ka, t1) // older
	tree.UpsertAt(kb, t2) // fresh
	tree.UpsertAt(kc, t2) // fresh

	// Выберем cutoff = t2 - 1s => протухнет только a (t1), b/c останутся.
	cutoff := t2.Add(-1 * time.Second)
	ttl := now.Sub(cutoff)

	deleted := tree.PurgeExpiredAt(now, ttl, 0)
	assertEqualInt(t, deleted, 1, "PurgeExpiredAt must delete only a")

	assertEqualBool(t, tree.Has(ka), false, "Has(a) after purge == false")
	assertEqualBool(t, tree.Has(kb), true, "Has(b) after purge == true")
	assertEqualBool(t, tree.Has(kc), true, "Has(c) after purge == true")
	assertEqualInt(t, tree.Size(), 2, "Size after purge == 2")
}

func TestByteKeyBTree_PurgeExpiredAt_RespectsMaxToDelete(t *testing.T) {
	tree := newTree()
	ka, kb, kc := []byte("a"), []byte("b"), []byte("c")

	// сделаем все три "старше" cutoff
	tree.UpsertAt(ka, t0)
	tree.UpsertAt(kb, t0)
	tree.UpsertAt(kc, t0)

	ttl := now.Sub(t0.Add(1 * time.Second)) // cutoff = t0+1s => все t0 <= cutoff => все кандидаты
	deleted := tree.PurgeExpiredAt(now, ttl, 2)
	assertEqualInt(t, deleted, 2, "respect maxToDelete=2")

	assertEqualInt(t, tree.Size(), 1, "only one key must remain")
}

func TestByteKeyBTree_PurgeExpiredAt_ZeroOrNegativeTTL_NoOp(t *testing.T) {
	tree := newTree()
	k := []byte("k")
	tree.UpsertAt(k, t0)

	deleted1 := tree.PurgeExpiredAt(now, 0, 0)
	assertEqualInt(t, deleted1, 0, "TTL=0 must be no-op")

	deleted2 := tree.PurgeExpiredAt(now, -5*time.Second, 0)
	assertEqualInt(t, deleted2, 0, "negative TTL must be no-op")

	assertEqualBool(t, tree.Has(k), true, "key must remain")
}

func TestByteKeyBTree_Delete_And_DeleteMany(t *testing.T) {
	tree := newTree()
	kb, kc, kd, ke, kz := []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("zzz")

	tree.UpsertAt(kb, t0)
	tree.UpsertAt(kc, t0)

	// Delete existing and then again (should be false)
	ok := tree.Delete(kc)
	assertEqualBool(t, ok, true, "Delete(c) existed")
	ok2 := tree.Delete(kc)
	assertEqualBool(t, ok2, false, "Delete(c) second time must be false")

	// DeleteMany with one missing key
	tree.UpsertAt(kd, t0)
	tree.UpsertAt(ke, t0)

	deleted := tree.DeleteMany([][]byte{kd, ke, kz})
	assertEqualInt(t, deleted, 2, "DeleteMany(d,e,zzz) must delete 2")

	assertEqualBool(t, tree.Has(kd), false, "Has(d) after DeleteMany == false")
	assertEqualBool(t, tree.Has(ke), false, "Has(e) after DeleteMany == false")
	assertEqualBool(t, tree.Has(kb), true, "Has(b) still present")
	assertEqualInt(t, tree.Size(), 1, "only b remains")
}

func TestByteKeyBTree_Reset_ClearsAll(t *testing.T) {
	tree := newTree()
	tree.UpsertAt([]byte("a"), now)
	tree.UpsertAt([]byte("b"), now)

	assertEqualInt(t, tree.Size(), 2, "size before Reset")

	tree.Reset()
	assertEqualInt(t, tree.Size(), 0, "size after Reset == 0")

	isNew := tree.UpsertAt([]byte("a"), now)
	assertEqualBool(t, isNew, true, "UpsertAt after Reset must be new")
	assertEqualInt(t, tree.Size(), 1, "size after reinsert == 1")
}
