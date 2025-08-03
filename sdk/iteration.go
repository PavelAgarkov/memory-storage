package sdk

import "github.com/dgraph-io/badger/v4"

type KV struct {
	Key, Value []byte
}

func (s *Store) ScanPrefix(prefix []byte, limit int, fn func(kv KV) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix // ← ставим префикс через поле
		// опционально: ускорит «ключ-онли» скан
		// opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		count := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			var kv KV
			kv.Key = append(kv.Key[:0], item.Key()...)
			if err := item.Value(func(val []byte) error {
				kv.Value = append(kv.Value[:0], val...)
				return nil
			}); err != nil {
				return err
			}
			if err := fn(kv); err != nil {
				return err
			}
			count++
			if limit > 0 && count >= limit {
				break
			}
		}
		return nil
	})
}
