package main

import (
	"context"
	"errors"
	"fmt"
	model "github.com/PavelAgarkov/memory-storage/protobuf/core"
	"github.com/PavelAgarkov/memory-storage/sdk"
	"github.com/dgraph-io/badger/v4"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
	"time"
)

const CurrentUserSchemeVersion = "v3"

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

	memLimits := sdk.ComputeMemoryLimit(16 * sdk.GiB)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	version := "v3"
	dir := filepath.Join(".", "data", version) // ./data/v1
	valueDir := filepath.Join(dir, "vlog")
	store, err := sdk.Open(
		ctx,
		sdk.Options{
			Dir:      dir,      // Каталог для LSM-дерева (SST, MANIFEST). Данные/индексы на диске.
			ValueDir: valueDir, // Каталог для value-log (крупные значения). Разделяем с LSM для более предсказуемого I/O.
			InMemory: false,    // Хранить на диске (прод-режим). true делай только для тестов — файлов не будет.
			ReadOnly: false,

			// --- Обслуживание и надежность ---
			GCInterval:    10 * time.Minute, // Периодический GC value-log (освобождение места). Реже → меньше фонового I/O.
			SyncWrites:    true,             // fsync на каждом коммите: максимум надежности, выше латентность записи.
			NumGoroutines: 2,                // Внутренний параллелизм Badger: хороший старт для 2–8 vCPU.

			// --- Логирование ---
			LoggingLevel: sdk.LogError, // Пишем только ошибки Badger; для диагностики можно поднять до LogInfo.

			// --- Порог/размеры (формируют поведение LSM и vlog) ---
			ValueThreshold:   1024,     // ≤1 KiB значение хранится в LSM: Get читает всё из SST без прыжка в vlog (быстрее чтения).
			ValueLogFileSize: 1 << 30,  // 1 GiB сегмент vlog: реже ротации и обходов при GC, меньше «дробления» файлов.
			BaseTableSize:    64 << 20, // 64 MiB базовый размер SST: меньше мелких L0-таблиц, быстрее стабильные компакции.
			NumCompactors:    4,        // 4 фоновых компактора: успевают «съедать» L0, избегая стагнации под записью.

			// --- Сжатие ---
			ZSTDCompressionLevel: 3, // Баланс CPU/диск: заметно экономит место/IO без большого CPU-оверхеда.

			// --- Транзакции/конкурентность/шифрование ---
			DetectConflicts: true, // Оптимистичный контроль конфликтов (MVCC). Твой TM ретраит ErrConflict.
			EncryptionKey:   key,  // Шифрование на диске (AES-256). Держи ключ вне репозитория; длина ровно 32 байта.
			Codec:           &sdk.ProtoCodec{},
		}, memLimits)

	if err != nil {
		fmt.Println("Failed to open Badger store:", err)
		return
	}
	defer store.Close()

	txOpt := sdk.TxManagerOptions{
		MaxRetries:  3,
		BaseBackoff: 50 * time.Millisecond,
		MaxBackoff:  500 * time.Millisecond,
	}
	transactionManager := sdk.NewTransactionManager(store, txOpt)
	err = transactionManager.ExecuteReadWriteWithContext(
		context.Background(),
		func(ctx context.Context, tx *badger.Txn) error {
			userKey := []byte("user:" + CurrentUserSchemeVersion + ":123")

			// читаем текущее значение (если есть)
			var u model.User
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

			store
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

	prefix := []byte("user:" + CurrentUserSchemeVersion + ":")
	_ = store.ScanPrefix(prefix, 1000, func(kv sdk.KV) error {
		var u model.User
		if err := proto.Unmarshal(kv.Value, &u); err != nil {
			return fmt.Errorf("unmarshal protobuf: %w", err)
		}
		b, _ := protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(&u)
		fmt.Println(string(b))
		return nil
	})

	//store.StartBadgerMemStats()
}
