package memory_storage

import (
	"testing"
	"time"
)

func Test_bitmam_execute(t *testing.T) {
	NewBitmapStorage(
		NewBitmapStubReplicator(),
		BitmapStorageConfigs{
			StorageName:       "name",
			MonitoringTicker:  time.Duration(10) * time.Second,
			OptimizingTicker:  time.Duration(10) * time.Second,
			ReplicationTicker: time.Duration(20) * time.Second,
			ReplicationTtl:    time.Duration(300) * time.Second,
			DebugLogs:         true,
			ReplicationKey:    "BitmapCurrentGoodsIDs",
		},
		&Warmer{
			BatchSize: 300,
		},
	)
}
