package main

import (
	"compress/gzip"
	"fmt"
	"github.com/PavelAgarkov/memory-storage/sdk"
	"os"
	"path/filepath"
)

func ptr[T any](v T) *T { return &v }

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "usage: restore <targetDir> <full.bak.gz> <incr1.bak.gz> [incr2.bak.gz ...]\n")
		os.Exit(2)
	}

	target := os.Args[1]
	full := os.Args[2]
	incrs := os.Args[3:]

	// подготовить каталоги
	if err := os.MkdirAll(filepath.Join(target, "vlog"), 0o755); err != nil {
		panic(err)
	}

	key, err := os.ReadFile("badger.key")
	if err != nil {
		fmt.Println("Failed to read Badger encryption key:", err)
		return
	}
	if len(key) != 32 {
		fmt.Println("Badger encryption key must be 32 bytes")
		return
	}

	version := "v3"
	dir := filepath.Join(".", "data", version) // ./data/v1
	valueDir := filepath.Join(dir, "vlog")
	// откроем новую пустую БД (с нужным ключом шифрования, кешами и т.д.)
	opts := sdk.Options{
		Dir:      dir,      // Каталог для LSM-дерева (SST, MANIFEST). Данные/индексы на диске.
		ValueDir: valueDir, // Каталог для value-log (крупные значения). Разделяем с LSM для более предсказуемого I/O.
		InMemory: false,    // Хранить на диске (прод-режим). true делай только для тестов — файлов не будет.
		ReadOnly: false,

		// --- Обслуживание и надежность ---
		//GCInterval:    10 * time.Minute, // При восстановлении не нужен
		SyncWrites:    true, // fsync на каждом коммите: максимум надежности, выше латентность записи.
		NumGoroutines: 2,    // Внутренний параллелизм Badger: хороший старт для 2–8 vCPU.

		// --- Логирование ---
		LoggingLevel: sdk.LogError, // Пишем только ошибки Badger; для диагностики можно поднять до LogInfo.

		// --- Кеши в RAM (критично для p95/p99 чтений) ---
		BlockCacheSize: 256 << 20, // 256 MiB кеш-блоков данных SST: меньше рандомных дисковых чтений при Get/сканах.
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
	}
	store, err := sdk.Open(opts)
	if err != nil {
		panic(err)
	}
	defer store.Close()

	// restore full
	if err := restoreOne(store, full); err != nil {
		panic(fmt.Errorf("restore full failed: %w", err))
	}
	// restore incrementals по порядку
	for _, p := range incrs {
		if err := restoreOne(store, p); err != nil {
			panic(fmt.Errorf("restore incr %s failed: %w", p, err))
		}
	}

	fmt.Println("restore done:", target)
}

func restoreOne(store *sdk.Store, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	zr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer zr.Close()

	return store.RestoreFromReader(zr, 256)
}
