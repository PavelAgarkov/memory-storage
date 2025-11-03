package memory_storage

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

func newFilter(key []byte) *FilterNodeItem {
	return &FilterNodeItem{keyBytes: append([]byte(nil), key...)}
}

func newValue(key, val []byte) *ValueNodeItem {
	return &ValueNodeItem{
		keyBytes:   append([]byte(nil), key...),
		valueBytes: append([]byte(nil), val...),
	}
}

func equalStringSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aa := append([]string(nil), a...)
	bb := append([]string(nil), b...)
	sort.Strings(aa)
	sort.Strings(bb)
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}

func TestUpsert_Has_GetLastWriteUnix_Filter(t *testing.T) {
	tr := NewByteKeyBTree(Options{})

	k := []byte("a")
	it := newFilter(k)

	ts1 := time.Unix(1000, 0)
	if added := tr.UpsertAt(it, ts1); !added {
		t.Fatalf("expected added=true for new key")
	}
	if !tr.Has(newFilter(k)) {
		t.Fatalf("expected Has(true) after insert")
	}
	if got, ok := tr.GetLastWriteUnix(newFilter(k)); !ok || got != ts1.Unix() {
		t.Fatalf("GetLastWriteUnix mismatch: got (%d,%v), want (%d,true)", got, ok, ts1.Unix())
	}

	// upsert обновлением (тот же ключ)
	ts2 := time.Unix(2000, 0)
	if added := tr.UpsertAt(newFilter(k), ts2); added {
		t.Fatalf("expected added=false on update")
	}
	if got, ok := tr.GetLastWriteUnix(newFilter(k)); !ok || got != ts2.Unix() {
		t.Fatalf("GetLastWriteUnix after update mismatch: got %d want %d", got, ts2.Unix())
	}
}

func TestUpsertMany_Size_And_EmptyKey(t *testing.T) {
	tr := NewByteKeyBTree(Options{})

	now := time.Unix(5000, 0)
	items := []Item{
		newFilter([]byte("a")),
		newFilter([]byte("b")),
		newFilter([]byte{}),        // пустой — игнор
		newFilter([]byte("b")),     // дубликат
		newValue([]byte("c"), nil), // другой тип
	}

	added := tr.UpsertManyAt(items, now)
	if added != 3 { // a, b, c
		t.Fatalf("added=%d, want 3", added)
	}
	if got := tr.Size(); got != 3 {
		t.Fatalf("size=%d, want 3", got)
	}
}

func TestDelete_And_DeleteMany(t *testing.T) {
	tr := NewByteKeyBTree(Options{})

	tr.UpsertAt(newFilter([]byte("a")), time.Unix(1, 0))
	tr.UpsertAt(newFilter([]byte("b")), time.Unix(1, 0))
	tr.UpsertAt(newFilter([]byte("c")), time.Unix(1, 0))

	if ok := tr.Delete(newFilter([]byte("b"))); !ok {
		t.Fatalf("expected delete b = true")
	}
	if tr.Has(newFilter([]byte("b"))) {
		t.Fatalf("b must be deleted")
	}

	n := tr.DeleteMany([]Item{
		newFilter([]byte("x")), // нет
		newFilter([]byte("a")), // есть
		newFilter([]byte("c")), // есть
	})
	if n != 2 {
		t.Fatalf("DeleteMany deleted=%d, want 2", n)
	}
	if tr.Size() != 0 {
		t.Fatalf("tree must be empty after deletions")
	}
}

func TestGetNodeItem_ReturnsLivePointer(t *testing.T) {
	tr := NewByteKeyBTree(Options{})

	it := newFilter([]byte("k1"))
	tr.UpsertAt(it, time.Unix(100, 0))

	got, ok := tr.GetNodeItem(newFilter([]byte("k1")))
	if !ok {
		t.Fatalf("GetNodeItem: not found")
	}
	// Проверяем, что это тот же объект (указатель)
	if gotPtr, ok2 := got.(*FilterNodeItem); !ok2 || gotPtr != it {
		t.Fatalf("GetNodeItem returns different pointer")
	}
}

func TestForEach_OrderAscending(t *testing.T) {
	tr := NewByteKeyBTree(Options{})

	keys := [][]byte{[]byte("c"), []byte("a"), []byte("b")}
	for i, k := range keys {
		tr.UpsertAt(newFilter(k), time.Unix(int64(i+1), 0))
	}

	var seen [][]byte
	err := tr.ForEach(func(key []byte, ts int64) bool {
		seen = append(seen, key)
		return true
	})
	if err != nil {
		t.Fatalf("ForEach error: %v", err)
	}

	want := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	if len(seen) != len(want) {
		t.Fatalf("ForEach count=%d want=%d", len(seen), len(want))
	}
	for i := range seen {
		if !bytes.Equal(seen[i], want[i]) {
			t.Fatalf("order mismatch at %d: got %q want %q", i, seen[i], want[i])
		}
	}
}

func TestReset(t *testing.T) {
	tr := NewByteKeyBTree(Options{})
	tr.UpsertAt(newFilter([]byte("x")), time.Now())
	if tr.Size() != 1 {
		t.Fatalf("size before reset=%d want=1", tr.Size())
	}
	tr.Reset()
	if tr.Size() != 0 {
		t.Fatalf("size after reset=%d want=0", tr.Size())
	}
}
func TestTTL_ListExpired_PurgeExpired_MaxLimits(t *testing.T) {
	tr := NewByteKeyBTree(Options{})

	base := time.Unix(10_000, 0)
	type pair struct {
		k string
		t int64
	}
	data := []pair{
		{"a", base.Unix() - 100},
		{"b", base.Unix() - 50},
		{"c", base.Unix() - 1}, // НЕ истёк при ttl=30s
		{"d", base.Unix() + 100},
	}
	for _, p := range data {
		it := newFilter([]byte(p.k))
		it.SetExpirationTime(time.Unix(p.t, 0))
		tr.UpsertAt(it, time.Unix(p.t, 0))
	}

	ttl := 30 * time.Second // cutoff = base - 30s

	expired := tr.ListExpiredAt(base, ttl, 0)
	if len(expired) != 2 { // <-- было 3
		t.Fatalf("expired count=%d want=2", len(expired))
	}
	var gotKeys []string
	for _, it := range expired {
		gotKeys = append(gotKeys, string(it.Key()))
	}
	if want := []string{"a", "b"}; !equalStringSets(gotKeys, want) { // <-- было a,b,c
		t.Fatalf("expired keys=%v want=%v", gotKeys, want)
	}

	// maxCount=1 ограничивает выдачу
	exp2 := tr.ListExpiredAt(base, ttl, 1) // <-- было 2
	if len(exp2) != 1 {
		t.Fatalf("expired limited count=%d want=1", len(exp2))
	}

	// Purge с лимитом 1
	deleted := tr.PurgeExpiredAt(base, ttl, 1) // <-- было 2
	if deleted != 1 {
		t.Fatalf("purge deleted=%d want=1", deleted)
	}
	if tr.Size() != 3 { // было 4, удалили 1 => 3
		t.Fatalf("size after partial purge=%d want=3", tr.Size())
	}

	// Дочистим остаток без лимита
	deleted2 := tr.PurgeExpiredAt(base, ttl, 0)
	if deleted2 != 1 { // удалится второй «просроченный»
		t.Fatalf("purge remainder deleted=%d want=1", deleted2)
	}
	if tr.Has(newFilter([]byte("a"))) || tr.Has(newFilter([]byte("b"))) {
		t.Fatalf("expired entries should be gone")
	}
	if !tr.Has(newFilter([]byte("c"))) || !tr.Has(newFilter([]byte("d"))) {
		t.Fatalf("fresh entries c,d must remain")
	}
}

// --- хелперы

func mustKeysFromItems(items []Item) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, string(it.Key()))
	}
	return out
}

// --- доп. функциональные тесты

func TestTTL_BoundaryInclusive(t *testing.T) {
	t.Parallel()

	tr := NewByteKeyBTree(Options{})
	base := time.Unix(20_000, 0)
	ttl := 30 * time.Second
	cutoff := base.Add(-ttl)

	a := newFilter([]byte("a"))
	b := newFilter([]byte("b"))
	c := newFilter([]byte("c"))

	// a — сильно старый, b — ровно на границе (должен попасть), c — свежий
	tr.UpsertAt(a, cutoff.Add(-1*time.Second))
	tr.UpsertAt(b, cutoff)
	tr.UpsertAt(c, cutoff.Add(1*time.Second))

	exp := tr.ListExpiredAt(base, ttl, 0)
	got := mustKeysFromItems(exp)
	sort.Strings(got)

	want := []string{"a", "b"}
	if len(got) != len(want) || got[0] != "a" || got[1] != "b" {
		t.Fatalf("expired=%v want=%v", got, want)
	}
}

func TestTTL_ZeroOrNegativeTTL(t *testing.T) {
	t.Parallel()

	tr := NewByteKeyBTree(Options{})
	now := time.Now()

	tr.UpsertAt(newFilter([]byte("x")), now.Add(-time.Hour))

	if out := tr.ListExpiredAt(now, 0, 10); out != nil {
		t.Fatalf("ttl=0 must return nil slice, got %v", out)
	}
	if del := tr.PurgeExpiredAt(now, -1, 0); del != 0 {
		t.Fatalf("ttl<=0 must delete 0, got %d", del)
	}
}

func TestDeleteMany_Idempotent(t *testing.T) {
	t.Parallel()

	tr := NewByteKeyBTree(Options{})
	tr.UpsertAt(newFilter([]byte("a")), time.Now())
	tr.UpsertAt(newFilter([]byte("b")), time.Now())

	n1 := tr.DeleteMany([]Item{newFilter([]byte("a")), newFilter([]byte("b"))})
	if n1 != 2 {
		t.Fatalf("first deleteMany=%d want=2", n1)
	}

	n2 := tr.DeleteMany([]Item{newFilter([]byte("a")), newFilter([]byte("b"))})
	if n2 != 0 {
		t.Fatalf("second deleteMany=%d want=0", n2)
	}
}

func TestUpsertMany_CountsOnlyNew(t *testing.T) {
	t.Parallel()

	tr := NewByteKeyBTree(Options{})
	now := time.Unix(1, 0)

	items := []Item{
		newFilter([]byte("a")),
		newFilter([]byte("b")),
		newFilter([]byte("a")), // дубликат
		newValue([]byte("c"), []byte("v")),
		newValue([]byte("c"), []byte("v2")), // дубликат
	}

	if added := tr.UpsertManyAt(items, now); added != 3 {
		t.Fatalf("added=%d want=3", added)
	}
	// обновим теми же ключами — добавлений быть не должно
	if added := tr.UpsertManyAt(items, now.Add(time.Hour)); added != 0 {
		t.Fatalf("added on update=%d want=0", added)
	}
}

func TestListExpired_LimitOrder(t *testing.T) {
	t.Parallel()

	tr := NewByteKeyBTree(Options{})
	base := time.Unix(30_000, 0)
	ttl := 1 * time.Hour

	// все истёкшие (час назад и старше)
	for _, k := range []string{"d", "a", "c", "b"} {
		tr.UpsertAt(newFilter([]byte(k)), base.Add(-2*time.Hour))
	}
	out := tr.ListExpiredAt(base, ttl, 2)
	got := mustKeysFromItems(out)

	// должны прийти первые 2 по ключу: a, b
	want := []string{"a", "b"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("limited expired=%v want=%v", got, want)
	}
}

func TestGetNodeItem_LivePointerMutationAffectsTTL(t *testing.T) {
	t.Parallel()

	tr := NewByteKeyBTree(Options{})
	base := time.Unix(40_000, 0)
	ttl := 10 * time.Second

	k := []byte("k")
	tr.UpsertAt(newFilter(k), base.Add(-1*time.Minute)) // точно истёк

	it, ok := tr.GetNodeItem(newFilter(k))
	if !ok {
		t.Fatalf("GetNodeItem not found")
	}

	// поднимем timestamp "извне"
	it.SetExpirationTime(base.Add(1 * time.Minute))

	// теперь запись не должна считаться истёкшей
	exp := tr.ListExpiredAt(base, ttl, 0)
	for _, e := range exp {
		if bytes.Equal(e.Key(), k) {
			t.Fatalf("key %q must NOT be expired after external timestamp bump", k)
		}
	}
}

// --- конкурентные тесты

func TestConcurrent_ReadersWriters_MixedStableSet(t *testing.T) {
	// множество фиксированных ключей; писатели обновляют ts, читатели сканируют/Has/Get
	t.Parallel()

	tr := NewByteKeyBTree(Options{})
	keys := make([][]byte, 200)
	for i := range keys {
		keys[i] = []byte{byte(i)}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// писатели
	writer := func(id int) {
		defer wg.Done()
		r := rand.New(rand.NewSource(int64(1000 + id)))
		for {
			select {
			case <-stop:
				return
			default:
			}
			k := keys[r.Intn(len(keys))]
			tr.UpsertAt(newFilter(k), time.Unix(time.Now().Unix(), 0))
		}
	}

	// читатели
	reader := func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = tr.ForEach(func(_ []byte, _ int64) bool { return true })
			_ = tr.Has(newFilter([]byte{123})) // произвольная проверка
		}
	}

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go writer(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go reader()
	}

	time.Sleep(200 * time.Millisecond) // дать погонять работу
	close(stop)
	wg.Wait()

	// должно быть не ноль (writers отработали)
	if tr.Size() == 0 {
		t.Fatalf("size must be > 0 after concurrent writers")
	}
}

func TestConcurrent_PurgeWhileWriting(t *testing.T) {
	// одновременно upsert'ы и периодические purge; в конце убеждаемся, что оставшиеся записи свежие
	t.Parallel()

	tr := NewByteKeyBTree(Options{})
	now := time.Unix(50_000, 0)
	ttl := 2 * time.Second

	// стартовый набор (часть устаревших, часть свежих)
	for i := 0; i < 100; i++ {
		ts := now.Add(time.Duration(-5+i%10) * time.Second) // часть < cutoff
		tr.UpsertAt(newFilter([]byte{byte(i)}), ts)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// писатели поднимают ts
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(777))
		for {
			select {
			case <-stop:
				return
			default:
			}
			k := []byte{byte(r.Intn(100))}
			tr.UpsertAt(newFilter(k), now.Add(5*time.Second)) // свежий ts
		}
	}()

	// чистильщик
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				tr.PurgeExpiredAt(now, ttl, 10) // чистим пачками
			}
		}
	}()

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	// Проверим, что все оставшиеся записи не просрочены относительно now/ttl
	err := tr.ForEach(func(_ []byte, tsUnix int64) bool {
		if tsUnix <= now.Add(-ttl).Unix() {
			t.Fatalf("found expired entry after purges: ts=%d cutoff=%d", tsUnix, now.Add(-ttl).Unix())
		}
		return true
	})
	if err != nil {
		t.Fatalf("ForEach error: %v", err)
	}
}

func TestConcurrent_ListExpiredDuringWrites_NoDeadlockAndLimits(t *testing.T) {
	t.Parallel()

	tr := NewByteKeyBTree(Options{})
	now := time.Unix(60_000, 0)
	ttl := 1 * time.Second

	// начальный набор старых ключей
	for i := 0; i < 100; i++ {
		tr.UpsertAt(newFilter([]byte{byte(i)}), now.Add(-2*time.Second))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// писатель освежает случайные ключи
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewSource(999))
		for {
			select {
			case <-stop:
				return
			default:
			}
			k := []byte{byte(r.Intn(100))}
			tr.UpsertAt(newFilter(k), now.Add(time.Second))
		}
	}()

	// параллельно дергаем ListExpiredAt с лимитами
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				out := tr.ListExpiredAt(now, ttl, 7)
				if len(out) > 7 {
					t.Fatalf("limit violated: got %d > 7", len(out))
				}
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestConcurrent_DeleteWhileIterating(t *testing.T) {
	t.Parallel()

	tr := NewByteKeyBTree(Options{})
	for i := 0; i < 200; i++ {
		tr.UpsertAt(newFilter([]byte{byte(i)}), time.Unix(int64(i), 0))
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// итератор под RLock
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = tr.ForEach(func(_ []byte, _ int64) bool { return true })
		}
	}()

	// удалятор под Lock
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			tr.Delete(newFilter([]byte{byte(i)}))
		}
	}()

	time.Sleep(100 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// sanity: порядок ключей при смешении типов в дереве
func TestOrderWithMixedTypes(t *testing.T) {
	t.Parallel()

	tr := NewByteKeyBTree(Options{})
	tr.UpsertAt(newFilter([]byte("b")), time.Unix(1, 0))
	tr.UpsertAt(newValue([]byte("a"), []byte("v")), time.Unix(1, 0))
	tr.UpsertAt(newFilter([]byte("c")), time.Unix(1, 0))

	var got [][]byte
	_ = tr.ForEach(func(k []byte, _ int64) bool {
		got = append(got, k)
		return true
	})

	want := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("order mismatch: got %q want %q", got[i], want[i])
		}
	}
}
