package main

import (
	"context"
	"errors"
	"fmt"
	user "github.com/PavelAgarkov/memory-storage/protobuf/core"
	"github.com/PavelAgarkov/memory-storage/sdk"
	"github.com/dgraph-io/badger/v4"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"os"
	"time"
)

func ptr[T any](v T) *T { return &v }

func main() {
	key, err := os.ReadFile("badger.key")
	if err != nil {
		fmt.Println("Failed to read Badger encryption key:", err)
		return
	}
	if len(key) != 32 {
		fmt.Println("Badger encryption key must be 32 bytes")
		return
	}

	// Пример: ключ шифрования должен быть ровно 32 байта (AES-256).
	// Прочитай его из секрета/ENV/файла; здесь просто заглушка.
	// var key []byte = mustLoad32ByteKey()
	store, err := sdk.Open(
		sdk.Options{
			Dir:      "./data",      // Каталог для LSM-дерева (SST, MANIFEST). Данные/индексы на диске.
			ValueDir: "./data/vlog", // Каталог для value-log (крупные значения). Разделяем с LSM для более предсказуемого I/O.
			InMemory: false,         // Хранить на диске (прод-режим). true делай только для тестов — файлов не будет.

			// --- Обслуживание и надежность ---
			GCInterval:    10 * time.Minute, // Периодический GC value-log (освобождение места). Реже → меньше фонового I/O.
			SyncWrites:    true,             // fsync на каждом коммите: максимум надежности, выше латентность записи.
			NumGoroutines: 2,                // Внутренний параллелизм Badger: хороший старт для 2–8 vCPU.

			// --- Логирование ---
			LoggingLevel: sdk.LogError, // Пишем только ошибки Badger; для диагностики можно поднять до LogInfo.

			// --- Кеши в RAM (критично для p95/p99 чтений) ---
			BlockCacheSize: 256 << 20, // 256 MiB кеш блоков данных SST: меньше рандомных дисковых чтений при Get/сканах.
			IndexCacheSize: 128 << 20, // 128 MiB кеш индексов/Bloom: быстрее поиск ключей, особенно важно при шифровании.

			// --- Порог/размеры (формируют поведение LSM и vlog) ---
			ValueThreshold:   1024,     // ≤1 KiB значение хранится в LSM: Get читает всё из SST без прыжка в vlog (быстрее чтения).
			ValueLogFileSize: 1 << 30,  // 1 GiB сегмент vlog: реже ротации и обходов при GC, меньше «дробления» файлов.
			BaseTableSize:    64 << 20, // 64 MiB базовый размер SST: меньше мелких L0-таблиц, быстрее стабильные компакции.
			MemTableSize:     96 << 20, // 96 MiB memtable: реже флаш на диск → крупнее SST и меньше их количества.
			NumMemtables:     6,        // До 6 memtable (1 активная + 5 immutable): переживаем всплески записи без стопов.
			NumCompactors:    4,        // 4 фоновых компактора: успевают «съедать» L0, избегая стагнации под записью.

			// --- Сжатие ---
			ZSTDCompressionLevel: 3, // Баланс CPU/диск: заметно экономит место/IO без большого CPU-оверхеда.

			// --- Транзакции/конкурентность/шифрование ---
			DetectConflicts: ptr(true), // Оптимистичный контроль конфликтов (MVCC). Твой TM ретраит ErrConflict.
			EncryptionKey:   key,       // Шифрование на диске (AES-256). Держи ключ вне репозитория; длина ровно 32 байта.
		})

	defer store.Close()

	userKey := []byte("user:v1:123")

	_ = store.Set(context.Background(), userKey, []byte(`{"id":123}`), 24*time.Hour)

	store.Delete(context.Background(), userKey)

	val, err := store.Get(context.Background(), userKey) // []byte(JSON)
	if errors.Is(err, sdk.ErrNotFound) {
		fmt.Println("not found")
	}
	fmt.Println(string(val))

	prefix := []byte("user:v1:")
	_ = store.ScanPrefix(prefix, 1000, func(kv sdk.KV) error {
		fmt.Println(string(kv.Value))
		return nil
	})

	txOpt := sdk.TxManagerOptions{
		MaxRetries:  3,
		BaseBackoff: 50 * time.Millisecond,
		MaxBackoff:  500 * time.Millisecond,
	}
	transactionManager := sdk.NewTransactionManager(store, txOpt)
	err = transactionManager.ExecuteReadWriteWithContext(
		context.Background(),
		func(ctx context.Context, tx *badger.Txn) error {
			userKey := []byte("user:v1:123")

			// читаем текущее значение (если есть)
			var u user.User
			item, err := tx.Get(userKey)
			if err != nil {
				if !errors.Is(err, badger.ErrKeyNotFound) {
					return fmt.Errorf("get user: %w", err)
				}
			} else {
				if err := item.Value(func(val []byte) error {
					return proto.Unmarshal(val, &u)
				}); err != nil {
					return fmt.Errorf("unmarshal user: %w", err)
				}
				fmt.Printf("Current user data: id=%d name=%q\n", u.GetId(), u.GetName())
			}

			// обновляем и сериализуем
			u.Id = 123
			u.Name = "Updated User"

			b, err := proto.MarshalOptions{Deterministic: true}.Marshal(&u)
			if err != nil {
				return fmt.Errorf("marshal user: %w", err)
			}

			if err := tx.Set(userKey, b); err != nil {
				return fmt.Errorf("update user: %w", err)
			}
			return nil
		},
	)

	if err != nil {
		fmt.Println(err)
	}

	prefix = []byte("user:v1:")
	_ = store.ScanPrefix([]byte("user:v1:"), 1000, func(kv sdk.KV) error {
		var u user.User
		if err := proto.Unmarshal(kv.Value, &u); err != nil {
			return fmt.Errorf("unmarshal protobuf: %w", err)
		}
		b, _ := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(&u)
		fmt.Println(string(b))
		return nil
	})

}
