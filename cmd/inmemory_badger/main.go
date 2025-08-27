package main

import (
	"context"
	"errors"
	"fmt"
	model "github.com/PavelAgarkov/memory-storage/protobuf/core"
	"github.com/PavelAgarkov/memory-storage/sdk"
	"github.com/dgraph-io/badger/v4"
	"time"
)

const CurrentUserSchemeVersion = "v3"

func main() {
	memLimits := sdk.ComputeMemoryLimit(2 * sdk.GiB)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := sdk.Open(
		ctx,
		sdk.Options{
			InMemory:             true,
			ReadOnly:             false,
			WithMetrics:          true,
			GCInterval:           0, // не нужен в режиме in-memory
			NumGoroutines:        2, // Внутренний параллелизм Badger: хороший старт для 2–8 vCPU.
			ValueThreshold:       1024,
			BaseTableSize:        64 * sdk.MiB, // 64 MiB базовый размер SST: меньше мелких L0-таблиц, быстрее стабильные компакции.
			NumCompactors:        4,            // 4 фоновых компактора: успевают «съедать» L0, избегая стагнации под записью.
			ZSTDCompressionLevel: 3,            // Баланс CPU/диск: заметно экономит место/IO без большого CPU-оверхеда.
			DetectConflicts:      true,         // Оптимистичный контроль конфликтов (MVCC). Твой TM ретраит ErrConflict.
			Codec:                &sdk.ProtoCodec{},
			LoggingLevel:         sdk.LogError, // Пишем только ошибки Badger; для диагностики можно поднять до LogInfo.
		},
		memLimits,
	)
	store.StartBadgerMemStats()

	if err != nil {
		fmt.Println("Failed to open Badger store:", err)
		return
	}
	defer store.Close()

	userKey := []byte("user:" + CurrentUserSchemeVersion + ":123")
	user1 := &model.User{
		Id:   22,
		Name: "Updated User 2",
	}
	bytes, _ := store.Marshal(user1)
	_ = store.Set(userKey, bytes, 0)

	txOpt := sdk.TxManagerOptions{
		MaxRetries:  3,
		BaseBackoff: 50 * time.Millisecond,
		MaxBackoff:  150 * time.Millisecond,
	}
	transactionManager := sdk.NewTransactionManager(store, txOpt)
	err = transactionManager.ExecuteReadWriteWithContext(
		context.Background(),
		func(ctx context.Context, tx *badger.Txn) error {
			var u model.User
			item, err := tx.Get(userKey)
			if err != nil {
				if !errors.Is(err, badger.ErrKeyNotFound) {
					return fmt.Errorf("get user: %w", err)
				}
			} else {
				unmarshalFunc := func(val []byte) error {
					return store.Unmarshal(val, &u)
				}
				if err := item.Value(unmarshalFunc); err != nil {
					return fmt.Errorf("unmarshal user: %w", err)
				}
				fmt.Printf("Current user data: id=%d name=%q\n", u.GetId(), u.GetName())
			}

			// обновляем и сериализуем
			u.Id = 123
			u.Name = "Updated User"

			b, err := store.Marshal(&u)
			//b, err := proto.MarshalOptions{Deterministic: true}.Marshal(&u)
			if err != nil {
				return fmt.Errorf("marshal user: %w", err)
			}

			if err := tx.Set(userKey, b); err != nil {
				return fmt.Errorf("update user: %w", err)
			}
			return nil
		},
	)

	_ = store.Delete(userKey)

	if err != nil {
		fmt.Println(err)
	}

	userKey2 := []byte("user:" + CurrentUserSchemeVersion + ":2")
	user2 := &model.User{
		Id:   22,
		Name: "Updated User 2",
	}
	//bytes, _ = store.Marshal(user2)
	//_ = store.Set(userKey2, bytes, 0)
	_ = store.SetObject(userKey2, user2, 0)

	var user3 model.User
	//getUser2Bytes, _ := store.Get(userKey2)
	//_ = store.Unmarshal(getUser2Bytes, &user3)
	_ = store.GetObject(userKey2, &user3)
	out, _ := sdk.ProtoJsonToOutput(&user3)
	fmt.Println(string(out) + "check")

	prefix := []byte("user:" + CurrentUserSchemeVersion + ":")
	_ = store.ScanPrefix(prefix, 1000, func(kv sdk.KV) error {
		var u model.User
		if err := store.Unmarshal(kv.Value, &u); err != nil {
			return fmt.Errorf("unmarshal protobuf: %w", err)
		}
		//b, _ := store.Marshal(&u)
		out, _ := sdk.ProtoJsonToOutput(&u)
		fmt.Println(string(out))
		return nil
	})

	//store.StartBadgerMemStats()
}
