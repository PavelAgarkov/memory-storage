package sdk

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// FullBackupToFile делает полный бэкап в gzip-файл.
// Возвращает lastTs — версию последней выгруженной записи (нужна для инкрементальных).
func (s *Store) FullBackupToFile(ctx context.Context, path string) (lastTs uint64, err error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, fmt.Errorf("create backup file: %w", err)
	}
	defer func() {
		if cerr := f.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	zw := gzip.NewWriter(f)
	defer func() {
		if cerr := zw.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	stream := s.db.NewStream()
	lastTs, err = stream.Backup(zw, 0) // 0 = полный бэкап
	if err != nil {
		return 0, fmt.Errorf("stream backup: %w", err)
	}
	return lastTs, nil
}

// IncrementalBackupToFile ...
func (s *Store) IncrementalBackupToFile(ctx context.Context, path string, sinceTs uint64) (lastTs uint64, err error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, fmt.Errorf("create incr backup file: %w", err)
	}
	defer func() {
		if cerr := f.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	zw := gzip.NewWriter(f)
	defer func() {
		if cerr := zw.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	stream := s.db.NewStream()
	lastTs, err = stream.Backup(zw, sinceTs) // вернёт lastTs; для следующего инкрементала передаём lastTs+1
	if err != nil {
		return 0, fmt.Errorf("stream incremental backup: %w", err)
	}
	return lastTs, nil
}

// RestoreFromReader: загрузка бэкапа в ТЕКУЩУЮ открыту БД.
// Важно: на время Load не должно быть параллельных транзакций.
func (s *Store) RestoreFromReader(r io.Reader, maxPending int) error {
	if maxPending <= 0 {
		maxPending = 256 // разумное значение для параллельной записи
	}
	if err := s.db.Load(r, maxPending); err != nil {
		return fmt.Errorf("load backup: %w", err)
	}
	// После restore стоит "сплющить" уровни, чтобы версии ключей были вместе.
	if err := s.db.Flatten(runtime.NumCPU()); err != nil {
		return fmt.Errorf("flatten after restore: %w", err)
	}
	return nil
}

// Утилита восстановления из файла (gzip).
func (s *Store) RestoreFromFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open backup file: %w", err)
	}
	defer f.Close()

	zr, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("open gzip: %w", err)
	}
	defer zr.Close()

	return s.RestoreFromReader(zr, 256)
}

// RunBackupScheduleWithVersion запускает почасовые инкременталы и ежедневный full,
// добавляя метку версии (например, "v1") в имена файлов бэкапа и файла since.
// Так бэкапы разных версий ключей (user:v1:..., user:v2:...) не перемешаются в одном каталоге.
func RunBackupScheduleWithVersion(ctx context.Context, store *Store, dir, version string) error {
	// Гарантируем существование каталога для бэкапов
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("make backup dir: %w", err)
	}

	// Храним since отдельно по версии, чтобы инкременталы не пересекались
	sincePath := filepath.Join(dir, fmt.Sprintf("since-%s.txt", version))

	// Читаем, с чего начинать (0 => полный бэкап)
	var since uint64 = loadSince(sincePath)

	// helper: полный бэкап + обновление since
	doFull := func() {
		// Имя файла: full-<version>-YYYY-MM-DD.bak.gz
		path := filepath.Join(
			dir,
			fmt.Sprintf("full-%s-%s.bak.gz", version, time.Now().Format("2006-01-02")),
		)
		last, err := store.FullBackupToFile(ctx, path)
		if err != nil {
			// TODO: логирование ошибки
			return
		}
		since = last + 1
		_ = saveSince(sincePath, since)
	}

	// helper: инкрементальный бэкап + обновление since
	doIncr := func() {
		// Имя файла: incr-<version>-YYYY-MM-DD-HH.bak.gz
		path := filepath.Join(
			dir,
			fmt.Sprintf("incr-%s-%s.bak.gz", version, time.Now().Format("2006-01-02-15")),
		)
		last, err := store.IncrementalBackupToFile(ctx, path, since)
		if err != nil {
			// TODO: логирование ошибки
			return
		}
		since = last + 1
		_ = saveSince(sincePath, since)
	}

	// При старте: если since==0, сразу делаем полный
	if since == 0 {
		doFull()
	}

	// Один цикл: каждый час — инкрементал, в полночь — full
	go func() {
		// Выравниваем «следующий час» и «следующий день»
		nextHour := time.Now().Truncate(time.Hour).Add(time.Hour)
		nextDay := time.Now().Truncate(24 * time.Hour).Add(24 * time.Hour)

		timer := time.NewTimer(time.Until(nextHour))
		defer timer.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				now := time.Now()
				// Около полуночи делаем full
				if now.After(nextDay.Add(-1*time.Minute)) && now.Before(nextDay.Add(1*time.Minute)) {
					doFull()
					nextDay = nextDay.Add(24 * time.Hour)
				} else {
					doIncr()
				}
				nextHour = nextHour.Add(time.Hour)
				timer.Reset(time.Until(nextHour))
			}
		}
	}()

	return nil
}

// loadSince читает uint64 из файла (строка в десятичном виде).
// Если файла нет или формат неверный — вернёт 0 (полный бэкап).
func loadSince(path string) uint64 {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0 // файла нет — начинаем с полного бэкапа
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return 0
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0 // битые данные — безопасно откатиться к полному
	}
	return v
}

// saveSince атомарно пишет uint64 в файл (десятичная строка).
// Права 0600, чтобы не светить служебные номера версий.
func saveSince(path string, since uint64) error {
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, werr := f.WriteString(strconv.FormatUint(since, 10))
	cerr := f.Close()
	if werr != nil {
		_ = os.Remove(tmp)
		return werr
	}
	if cerr != nil {
		_ = os.Remove(tmp)
		return cerr
	}
	return os.Rename(tmp, path) // атомарная подмена файла
}
