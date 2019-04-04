// Copyright 2019 eBay Inc.
// Primary authors: Simon Fell, Diego Ongaro,
//                  Raymond Kroeker, and Sathish Kandasamy.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rocksdb

import (
	"fmt"

	metricsutil "github.com/ebay/beam/util/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type rocksdbPropertyMetrics struct {
	property string
	name     string
	help     string
	gauge    prometheus.Gauge
}

type rocksdbMetrics struct {
	propertiesCollectionDurationSeconds prometheus.Summary
	// properties is a table of RockDB properties that are exported as
	// Prometheus metrics.
	properties []rocksdbPropertyMetrics
}

var metrics rocksdbMetrics

func init() {
	// Note: The metric descriptions below are based on the rocksdb
	// documentation found at
	// https://github.com/facebook/rocksdb/blob/master/include/rocksdb/db.h

	pm := []rocksdbPropertyMetrics{
		{
			property: "rocksdb.num-immutable-mem-table",
			name:     "num_immutable_mem_table",
			help:     `The number of immutable memtables that have not yet been flushed.`,
		},
		{
			property: "rocksdb.mem-table-flush-pending",
			name:     "mem_table_flush_pending",
			help:     `1 if a memtable flush is pending; otherwise, 0.`,
		},
		{
			property: "rocksdb.num-running-flushes",
			name:     "num_running_flushes",
			help:     `The number of currently running flushes.`,
		},
		{
			property: "rocksdb.compaction-pending",
			name:     "compaction_pending",
			help:     `1 if at least one compaction is pending; otherwise, 0.`,
		},
		{
			property: "rocksdb.num-running-compactions",
			name:     "num_running_compactions",
			help:     `The number of currently running compactions.`,
		},
		{
			property: "rocksdb.background-errors",
			name:     "background_errors",
			help:     `The accumulated number of background errors.`,
		},
		{
			property: "rocksdb.cur-size-active-mem-table",
			name:     "cur_size_active_mem_table",
			help:     `The approximate size of active memtable (bytes).`,
		},
		{
			property: "rocksdb.cur-size-all-mem-tables",
			name:     "cur_size_all_mem_tables",
			help:     `The approximate size of active and unflushed immutable memtables (bytes).`,
		},
		{
			property: "rocksdb.size-all-mem-tables",
			name:     "size_all_mem_tables",
			help:     `Approximate size of active, unflushed and pinned immutable memtables (bytes).`,
		},
		{
			property: "rocksdb.num-entries-active-mem-table",
			name:     "num_entries_active_mem_table",
			help:     `The total number of entries in the active memtable.`,
		},
		{
			property: "rocksdb.num-entries-imm-mem-tables",
			name:     "num_entries_imm_mem_tables",
			help:     `The total number of entries in the unflushed immutable memtables.`,
		},
		{
			property: "rocksdb.num-deletes-active-mem-table",
			name:     "num_deletes_active_mem_table",
			help:     `The total number of delete entries in the active memtable.`,
		},
		{
			property: "rocksdb.num-deletes-imm-mem-tables",
			name:     "num_deletes_imm_mem_tables",
			help:     `The total number of delete entries in the unflushed immutable memtables.`,
		},
		{
			property: "rocksdb.estimate-num-keys",
			name:     "estimate_num_keys",
			help: `The estimated number of total keys.

The estimated number of total keys in the active and unflushed immutable 
memtables and storage.`,
		},
		{
			property: "rocksdb.estimate-table-readers-mem",
			name:     "estimate_table_readers_mem",
			help: `The estimated memory used for reading SST tables.

The estimated memory used for reading SST tables, excluding 
memory used in block cache (e.g., filter and index blocks).`,
		},
		{
			property: "rocksdb.num-snapshots",
			name:     "num_snapshots",
			help:     `The number of unreleased snapshots.`,
		},
		{
			property: "rocksdb.oldest-snapshot-time",
			name:     "oldest_snapshot_time",
			help:     `Unix timestamp of oldest unreleased snapshot.`,
		},
		{
			property: "rocksdb.num-live-versions",
			name:     "num_live_versions",
			help: `The number of live versions.

'Version' is an internal data structure. See version_set.h for details.
More live versions often mean more SST files are held from being deleted, by 
iterators or unfinished compactions.`,
		},
		{
			property: "rocksdb.current-super-version-number",
			name:     "current_super_version_number",
			help:     `Number of changes to the LSM tree since the last restart.`,
		},
		{
			property: "rocksdb.estimate-live-data-size",
			name:     "estimate_live_data_size",
			help:     `An estimate of the amount of live data in bytes.`,
		},
		{
			property: "rocksdb.min-log-number-to-keep",
			name:     "min_log_number_to_keep",
			help:     `The minimum log number of the log files that should be kept.`,
		},
		// rocksdb.min-obsolete-sst-number-to-keep was introduced since v5.10.2
		// in commit d595492 (May 2018).

		{
			property: "rocksdb.total-sst-files-size",
			name:     "total_sst_files_size",
			help:     `The total size (bytes) of all SST files.`,
		},
		// rocksdb.live-sst-files-size  was introduced since v5.10.2 in commit
		// bf937cf (March 2018).

		{
			property: "rocksdb.estimate-pending-compaction-bytes",
			name:     "estimate_pending_compaction_bytes",
			help: `Estimated bytes to write to reach compaction goal.

An estimated total number of bytes compaction needs to rewrite to get all 
levels down to under target size. Not valid for other compactions than 
level-based.`,
		},
		{
			property: "rocksdb.actual-delayed-write-rate",
			name:     "actual_delayed_write_rate",
			help: `The current actual delayed write rate.

0 means no delay.`,
		},
		{
			property: "rocksdb.is-write-stopped",
			name:     "is_write_stopped",
			help:     `1 if write has been stopped.`,
		},
		// rocksdb.block-cache-capacity was introduced since v5.10.2 in commit
		// ad51168 (April 2018).

		// rocksdb.block-cache-usage was introduced since v5.10.2 in commit
		// ad51168 (April 2018).

		// rocksdb.block-cache-pinned-usage was introduced since v5.10.2 in
		// commit ad51168 (April 2018).

	}

	mr := metricsutil.Registry{R: prometheus.DefaultRegisterer}

	for i, p := range pm {
		h := fmt.Sprintf("%s\n\nThis value is periodically copied from the RocksDB property\n'%s'.",
			p.help, p.property)
		pm[i].gauge = mr.NewGauge(prometheus.GaugeOpts{
			Namespace: "beam",
			Subsystem: "rocksdb",
			Name:      p.name,
			Help:      h,
		})
	}

	metrics = rocksdbMetrics{
		propertiesCollectionDurationSeconds: mr.NewSummary(prometheus.SummaryOpts{
			Namespace: "beam",
			Subsystem: "rocksdb",
			Name:      "properties_collection_duration_seconds",
			Help: `The time it takes to read RocksDB properties.

RocksDB properties are read periodically to export it as other metrics.

The higher value indicates getting the properties isn't as cheap as we had
assumed, and we may want to find another approach.
`,
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001},
		}),
		properties: pm,
	}
}
