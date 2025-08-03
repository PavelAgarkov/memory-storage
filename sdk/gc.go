package sdk

import "time"

func (s *Store) runGC(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.stopGC:
			return
		case <-t.C:
			// Badger рекомендует несколькими попытками вызывать GC пока возвращает nil.
		gcLoop:
			for {
				err := s.db.RunValueLogGC(0.5) // 50% reclaim threshold
				if err != nil {
					break gcLoop
				}
			}
		}
	}
}
