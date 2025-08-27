package sdk

import (
	"fmt"
	"time"
)

type LogLevel string

const (
	LogDebug   LogLevel = "DEBUG"
	LogInfo    LogLevel = "INFO"
	LogWarning LogLevel = "WARNING"
	LogError   LogLevel = "ERROR"
)

const (
	MiB   int64 = 1024 * 1024
	GiB   int64 = 1024 * 1024 * 1024
	ALIGN int64 = 64 * MiB
)

type Options struct {
	// Dir — каталог для LSM-части (SST, MANIFEST). Должен существовать или будет создан.
	// При InMemory=true игнорируется.
	Dir string

	// ValueDir — каталог для value log (крупные значения). Может совпадать с Dir.
	// При InMemory=true игнорируется.
	ValueDir string

	// InMemory — полностью в памяти, без файлов на диске. Полезно для тестов/кэшей.
	// При включении Dir/ValueDir игнорируются, данные не переживают перезапуск.
	InMemory bool

	// ReadOnly — открыть БД только для чтения. Записи/компакции/GC невозможны.
	// Требует, чтобы файлы БД уже существовали.
	ReadOnly bool

	// WithMetrics — включить внутренние метрики (hits/misses и пр.) для кешей и т.д.
	// Нужен bo = bo.WithMetricsEnabled(true), иначе счётчики будут нулевыми.
	WithMetrics bool

	// GCInterval — периодический запуск value-log GC (если вы это делаете сами через s.runGC).
	// Badger сам GC «по таймеру» не запускает; эту периодику задаёте вы.
	GCInterval time.Duration

	// SyncWrites — fsync на каждую запись (жертвуем скоростью ради максимальной надёжности).
	// false обычно быстрее, но возможна потеря последних записей при сбое питания/процесса.
	SyncWrites bool

	// NumGoroutines — степень параллелизма фоновых операций Badger (компакции, чтение/запись).
	// Не путать с NumCompactors: это общий «пул» воркеров для разных задач.
	NumGoroutines int

	// LoggingLevel — уровень логирования Badger (DEBUG/INFO/WARNING/ERROR).
	LoggingLevel LogLevel

	// ------------------- ПАМЯТЬ / КЕШИ / BUFFERS -------------------

	// BlockCacheSize — размер кеша «блоков» SST (Ristretto с TinyLFU/SLRU-эвикцией).
	// Держит в памяти часто читаемые блоки таблиц, снижая обращения к диску и повторную декомпрессию.
	// Лимит в байтах; при превышении — эвикция «холодных» элементов.
	BlockCacheSize int64

	// IndexCacheSize — размер кеша индексов SST и Bloom-фильтров (также Ristretto).
	// Ускоряет ответы «ключ точно отсутствует/в каком файле искать» без I/O.
	// Лимит в байтах; при превышении — эвикция.
	IndexCacheSize int64

	// MemTableSize — размер ОДНОЙ memtable (байты). При достижении лимита активная memtable
	// становится immutable и асинхронно сбрасывается в SST. Новая активная memtable создаётся отдельно.
	// Большие memtable улучшают последовательность записи и уменьшают write amplification,
	// но увеличивают пиковое RAM-потребление и задержку флаша.
	MemTableSize int64

	// NumMemtables — общее число memtable-слотов: 1 активная + (NumMemtables-1) immutable в очереди на сброс.
	// Если все слоты заняты (активная заполнена, а immutable ещё не сброшены), запись будет притормаживаться
	// (backpressure) до завершения флаша. Итоговое пиковое RAM-потребление под memtable
	// ≈ NumMemtables * MemTableSize.
	NumMemtables int

	// ------------------- ПРОЧИЕ ПАРАМЕТРЫ ХРАНЕНИЯ -------------------

	// ValueThreshold — порог (байты), выше которого значение кладётся в value log,
	// а в LSM (SST) хранится только указатель. Меньшие значения инлайнатся в LSM.
	// Выше порог — меньше LSM, больше vlog I/O. Ниже порог — больше LSM, меньше vlog.
	ValueThreshold int64

	// ValueLogFileSize — максимальный размер одиночного файла value log (байты), после чего
	// Badger начинает новый vlog-файл. Влияет на частоту GC и количество открытых файлов.
	ValueLogFileSize int64

	// BaseTableSize — целевой базовый размер SST-таблицы (байты). Фактические размеры таблиц
	// на уровнях растут кратно базовому (согласно внутренним коэффициентам), влияя на стратегию компакций.
	BaseTableSize int64

	// NumCompactors — число воркеров компакции. Больше — быстрее переработка уровней при высокой нагрузке,
	// но выше конкуренция за I/O и память.
	NumCompactors int

	// ZSTDCompressionLevel — уровень ZSTD (0 — по умолчанию; <0 — быстрее/хуже; >0 — медленнее/лучше).
	// Влияет на место на диске и CPU-времена (чтение/запись/компакции).
	ZSTDCompressionLevel int

	// DetectConflicts — детект конфликтов транзакций (write-write). true — безопаснее, но дороже.
	// Можно отключать при полном внешнем контроле параллелизма/последовательности.
	DetectConflicts bool

	// EncryptionKey — ключ шифрования (AES-CTR): 16/24/32 байта. Пустой срез — без шифрования.
	// Применяется к данным на диске (SST/vlog).
	EncryptionKey []byte

	// Codec - маршалер для сериализации/десериализации объектов
	Codec Codec
}

// ComputeMemoryLimit вычисляет разумные значения для кешей и memtables по переданнуму лимиту памяти
type MemoryLimit struct {
	BlockCacheSize int64 // например, 8 * 1024 * 1024 * 1024 (8 GiB)
	IndexCacheSize int64 // например, 4 * 1024 * 1024 * 1024 (4 GiB)
	MemTableSize   int64 // размер одной memtable
	NumMemtables   int   // максимум 2
}

// ComputeMemoryLimit вычисляет размеры кешей и memtables под общий лимит памяти (в байтах).
// Лимит зажимается в [1 GiB, 20 GiB]. Все размеры кратны 64 MiB.
func ComputeMemoryLimit(memoryLimit int64) *MemoryLimit {
	const (
		minSafety = 128 * MiB
		maxMTSize = 2 * GiB
		minMTSize = 64 * MiB

		minBlock = 256 * MiB
		minIndex = 128 * MiB
	)

	// clamp общий лимит
	ml := int64(memoryLimit)
	if ml < 1*GiB {
		ml = 1 * GiB
	}
	if ml > 20*GiB {
		ml = 20 * GiB
	}

	// запас под рантайм/фрагментацию: ~10%, но не менее 128 MiB и не более 25%
	safety := ml / 10
	if safety < minSafety {
		safety = minSafety
	}
	if safety > ml/4 {
		safety = ml / 4
	}

	budget := ml - safety
	if budget < 0 {
		budget = ml
	}

	// доля на memtables (зависит от общего лимита)
	var memRatio float64
	switch {
	case ml >= 12*GiB:
		memRatio = 0.25
	case ml >= 6*GiB:
		memRatio = 0.20
	default:
		memRatio = 0.15
	}

	memBudget := int64(float64(budget) * memRatio)
	if memBudget < 0 {
		memBudget = 0
	}

	// количество memtables: максимум 2
	numMemtables := int64(1)
	if memBudget >= 1*GiB {
		numMemtables = 2
	}
	if numMemtables > 2 {
		numMemtables = 2
	}

	// размер одной memtable
	memTableSize := int64(0)
	if numMemtables > 0 {
		memTableSize = memBudget / numMemtables
	}
	if memTableSize < minMTSize {
		memTableSize = minMTSize
	}
	if memTableSize > maxMTSize {
		memTableSize = maxMTSize
	}
	// выравнивание к 64 MiB
	memTableSize = (memTableSize / ALIGN) * ALIGN
	if memTableSize <= 0 {
		memTableSize = minMTSize
	}

	memtableTotal := memTableSize * numMemtables
	if memtableTotal > budget {
		memtableTotal = budget
	}

	// бюджет под кеши
	cacheBudget := budget - memtableTotal
	if cacheBudget < 0 {
		cacheBudget = 0
	}

	// делим кеши 2:1 (Block:Index)
	block := (cacheBudget * 2) / 3
	index := cacheBudget - block

	// минимумы
	if block < minBlock {
		block = minBlock
	}
	if index < minIndex {
		index = minIndex
	}

	// ужимаем, если вышли за бюджет
	totalCache := block + index
	if totalCache > cacheBudget && totalCache > 0 {
		block = (block * cacheBudget) / totalCache
		index = cacheBudget - block
	}

	// выравнивание к 64 MiB
	block = (block / ALIGN) * ALIGN
	index = (index / ALIGN) * ALIGN
	if block < 0 {
		block = 0
	}
	if index < 0 {
		index = 0
	}

	memLimits := &MemoryLimit{
		BlockCacheSize: block,
		IndexCacheSize: index,
		MemTableSize:   memTableSize,
		NumMemtables:   int(numMemtables),
	}
	fmt.Printf("BlockCacheSize: %d MiB\n", memLimits.BlockCacheSize/1024/1024)
	fmt.Printf("IndexCacheSize: %d MiB\n", memLimits.IndexCacheSize/1024/1024)
	fmt.Printf("MemTableSize: %d MiB\n", memLimits.MemTableSize/1024/1024)

	return memLimits
}
