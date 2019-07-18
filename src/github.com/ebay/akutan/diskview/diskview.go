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

// Package diskview implements a view service that serves facts from an ordered
// key-value store.
package diskview

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ebay/akutan/blog"
	"github.com/ebay/akutan/config"
	"github.com/ebay/akutan/diskview/database"
	"github.com/ebay/akutan/diskview/keys"
	"github.com/ebay/akutan/logentry/logencoder"
	"github.com/ebay/akutan/msg/facts"
	"github.com/ebay/akutan/partitioning"
	"github.com/ebay/akutan/rpc"
	"github.com/ebay/akutan/space"
	"github.com/ebay/akutan/util/clocks"
	grpcserverutil "github.com/ebay/akutan/util/grpc/server"
	"github.com/ebay/akutan/util/parallel"
	"github.com/ebay/akutan/util/profiling"
	proto "github.com/gogo/protobuf/proto"
	"github.com/google/btree"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

var startTime = time.Now()

// DiskView represents an instance of the DiskView service, it reads log entries
// from the log, applies them to a local database, and responds to lookup requests.
// It has both a local go interface, as well as a gRPC API for the API tier to use.
type DiskView struct {
	cfg      *config.Akutan
	aLog     blog.AkutanLog
	carousel carouselConductor

	lock sync.RWMutex
	// the next position in the log that should be read [protected by lock]
	nextPosition rpc.LogPosition
	// pending transactions [protected by lock]
	txns map[blog.Index]*transaction
	// pending keys from txns [protected by lock]
	pending *btree.BTree
	// a Condition that is signaled each time we've applied a chunk of log entries
	updateCond *sync.Cond

	graphCounts countTracker
	db          database.DB
	partition   partitioning.FactPartition
	statsCh     chan localStatsRequest
}

// A pending transaction.
type transaction struct {
	// Position in the log where the transaction should be applied, if committed.
	position rpc.LogPosition
	// The keys to be written into the db, in no particular order.
	keys [][]byte
	// This channel is closed after the transaction is committed into the db or
	// aborted. It's never sent to.
	decided chan struct{}
}

// pendingItem is an item in the view.pending BTree that tracks a single pending
// key from a transaction.
type pendingItem struct {
	// The fact, encoded the same as in the db.
	key []byte
	// The pending transaction that may create key.
	tx *transaction
}

// Less on pendingItem compares the keys lexicographically.
func (k pendingItem) Less(other btree.Item) bool {
	o := other.(pendingItem)
	return string(k.key) < string(o.key)
}

// New constructs a new DiskView instance based on the supplied configuration.
// This won't start processing the log or responding to API requests until you
// subsequently call Start()
func New(cfg *config.Akutan) (*DiskView, error) {
	if cfg.DiskView == nil {
		return nil, fmt.Errorf("diskView field missing in Akutan config")
	}
	diskview := DiskView{
		cfg:     cfg,
		txns:    make(map[blog.Index]*transaction),
		pending: btree.New(16),
		statsCh: make(chan localStatsRequest, 2),
	}
	diskview.updateCond = sync.NewCond(diskview.lock.RLocker())

	switch cfg.DiskView.Space {
	case "hashpo":
		hashes, err := parseHashRange(cfg.DiskView.FirstHash, cfg.DiskView.LastHash)
		if err != nil {
			return nil, fmt.Errorf("bad hash range in diskView config: %v", err)
		}
		diskview.partition = partitioning.NewHashPredicateObject(hashes)

	case "hashsp":
		hashes, err := parseHashRange(cfg.DiskView.FirstHash, cfg.DiskView.LastHash)
		if err != nil {
			return nil, fmt.Errorf("bad hash range in diskView config: %v", err)
		}
		diskview.partition = partitioning.NewHashSubjectPredicate(hashes)

	default:
		return nil, fmt.Errorf("unsupported space in disk view config: %q", cfg.DiskView.Space)
	}

	var err error
	diskview.db, err = database.New(cfg.DiskView.Backend, database.FactoryArgs{
		Partition: diskview.partition,
		Config:    cfg.DiskView,
		Dir:       cfg.DiskView.Dir,
	})
	if err != nil {
		return nil, err
	}
	meta, err := diskview.readMeta()
	if err != nil {
		log.Fatalf("Error reading meta key from db: %v", err)
	}
	if meta.RestartPosition != nil {
		log.Infof("Restart index is %v (with version %d)", meta.RestartPosition.Index, meta.RestartPosition.Version)
		diskview.nextPosition = *meta.RestartPosition
	} else {
		log.Info("No RestartPosition stored, starting at begining of log")
		// no previous meta, and/or new log, set the default starting version
		diskview.nextPosition = logencoder.LogInitialPosition()
	}
	diskview.ensureBaseFactsStored()
	return &diskview, nil
}

// Close will close the underlying database for this DiskView, its not
// valid for further use once closed.
// TODO: this probably needs to cleanly shutdown the log appender first
func (view *DiskView) Close() {
	view.db.Close()
}

// Start will start the processing for this DiskView. First it'll start
// trying to consume the log, starting from its previous persisted log
// index. If that index is not available in the log any more, or if our
// last log position is now greater than the 'Carousel Catchup Lag' we
// start a carousel client to catch up the state.
// Once this is completed, the API server will be started.
func (view *DiskView) Start() error {
	var err error
	view.aLog, err = blog.New(context.TODO(), view.cfg)
	if err != nil {
		return err
	}
	meta, err := view.readMeta()
	if err != nil {
		return err
	}
	if view.cfg.DiskView.CarouselCatchupLag > 0 {
		// see if we should do a carousel catchup first
		info, err := view.aLog.Info(context.TODO())
		if err != nil {
			// TODO: I don't understand this logic or this message.
			log.Warnf("Unable to determine position of the tail of the log, skiping carousel catchup check, will catchup from log: %v", err)
		} else {
			restartIdx := uint64(1)
			if meta.RestartPosition != nil {
				restartIdx = meta.RestartPosition.Index
			}
			lag := info.LastIndex - restartIdx
			if int64(lag) > view.cfg.DiskView.CarouselCatchupLag {
				if err := view.carouselCatchup(); err != nil {
					log.Warnf("Error during startup carousel catchup, will catchup from log: %v", err)
				}
			}
		}
	}
	view.carousel.init(view.db)
	view.graphCounts.init(&view.carousel, clocks.Wall, view, time.Minute*10)

	err = view.startGRPCServer()
	if err != nil {
		return fmt.Errorf("error starting gRPC server: %v", err)
	}

	view.startHTTPServer()
	go func() {
		for {
			view.consumeLog()
		}
	}()
	return nil
}

func (view *DiskView) startHTTPServer() {
	if view.cfg.DiskView.MetricsAddress == "" {
		log.Warnf("Cannot start HTTP server as 'metricsAddress' configuration not set")
		return
	}
	http.Handle("/metrics", promhttp.Handler())
	log.Infof("Starting HTTP server for metrics on %v",
		view.cfg.DiskView.MetricsAddress)
	go func() {
		err := http.ListenAndServe(view.cfg.DiskView.MetricsAddress, nil)
		if err != nil {
			log.WithError(err).Panic("Failed to start HTTP server for Prometheus endpoint")
		}
	}()
}

func (view *DiskView) ensureBaseFactsStored() {
	effects := applyEffects{
		dbWriter: view.db.BulkWrite(),
	}
	view.insertFactsForOurPartition(facts.BaseFacts(), &effects)
	if err := effects.dbWriter.Close(); err != nil {
		panic(fmt.Sprintf("Unable to apply baseFacts: %v", err))
	}
}

func (view *DiskView) readMeta() (MetaValue, error) {
	var value MetaValue
	err := view.readKeyValue(keys.MetaKey{}, &value)
	if err == nil || err == database.ErrKeyNotFound {
		return value, nil
	}
	return value, err
}

func (view *DiskView) writeMeta() error {
	view.lock.RLock()
	restartPos := view.nextPosition
	for _, tx := range view.txns {
		restartPos = rpc.MinPosition(restartPos, tx.position)
	}
	view.lock.RUnlock()
	value := MetaValue{
		RestartPosition: &restartPos,
	}
	if err := view.writeKeyValue(keys.MetaKey{}, &value); err != nil {
		return err
	}
	log.Debugf("Wrote meta key: %+v", value)
	return nil
}

func (view *DiskView) readKeyValue(keySpec keys.Spec, val proto.Unmarshaler) error {
	bytes, err := view.db.Read(keySpec.Bytes())
	if err != nil {
		return err
	}
	return val.Unmarshal(bytes)
}

func (view *DiskView) writeKeyValue(keySpec keys.Spec, val proto.Marshaler) error {
	bytes, err := val.Marshal()
	if err != nil {
		return err
	}
	return view.db.Write(keySpec.Bytes(), bytes)
}

// consumeLog will apply entries from the log to the view, looping indefinitely
func (view *DiskView) consumeLog() {
	ch := make(chan []blog.Entry, 16)
	wait := parallel.Go(func() {
		applyBuff := view.db.BulkWrite()
		var timeoutCh <-chan time.Time
		for {
			select {
			case <-timeoutCh:
				log.Debugf("flushing current replay meta to disk")
				err := view.writeMeta()
				if err != nil {
					log.Errorf("Error writing meta key to disk: %v", err)
				}
				timeoutCh = nil // once we've timedout we don't care again
				// about this until another log message has been processed

			case statsReq := <-view.statsCh:
				view.genLocalStats(&statsReq)

			case entries, open := <-ch:
				if !open {
					log.Info("log read channel closed")
					return
				}

				writeMeta := false
				effects := applyEffects{
					dbWriter: applyBuff,
				}
				view.lock.Lock()
				for _, entry := range entries {
					cmd, nextPos, err := logencoder.Decode(view.nextPosition.Version, entry)
					if err != nil {
						log.Fatalf("Cannot parse entry at index %v", entry.Index)
					}
					view.apply(cmd, &effects)
					view.nextPosition = nextPos
					if effects.changedMetadata || view.nextPosition.Index%100 == 0 {
						writeMeta = true
					}
				}
				if err := effects.dbWriter.Close(); err != nil {
					log.Fatalf("database transaction failed: %v", err)
				}
				view.graphCounts.haveWritten()
				view.lock.Unlock()
				view.updateCond.Broadcast()
				for _, ch := range effects.closeAfterWriting {
					close(ch)
				}
				if writeMeta {
					err := view.writeMeta()
					if err != nil {
						log.Errorf("Error writing meta key to disk: %v", err)
					}
				}
				if timeoutCh == nil {
					timeoutCh = time.After(time.Second * 5)
				}
			}
		}
	})
	err := view.aLog.Read(context.Background(), view.nextPosition.Index, ch)
	wait()
	if err != nil {
		log.Errorf("Error reading from log: %v", err)
		if blog.IsTruncatedError(err) {
			log.Infof("View too far behind, starting carousel catchup")
			if err := view.carouselCatchup(); err != nil {
				log.Errorf("Unable to complete carouselCatchup: %v", err)
			}
		}
		return // consumeLog will get called again by the goroutine in Start()
	}
}

func (view *DiskView) startGRPCServer() error {
	log.Infof("Starting gRPC server on %v", view.cfg.DiskView.GRPCAddress)
	listener, err := net.Listen("tcp", view.cfg.DiskView.GRPCAddress)
	if err != nil {
		return err
	}
	grpcServer := grpcserverutil.NewServer()
	rpc.RegisterCarouselServer(grpcServer, view)
	rpc.RegisterDiagnosticsServer(grpcServer, view)
	switch view.partition.Encoding() {
	case rpc.KeyEncodingSPO:
		rpc.RegisterReadFactsSPServer(grpcServer, view)
	case rpc.KeyEncodingPOS:
		rpc.RegisterReadFactsPOServer(grpcServer, view)
	}
	grpc_prometheus.Register(grpcServer)
	go grpcServer.Serve(listener)
	return nil
}

// FactStats returns the most recently collected statistics about the facts stored in this diskview
func (view *DiskView) FactStats(ctx context.Context, req *rpc.FactStatsRequest) (*rpc.FactStatsResult, error) {
	statsVal := view.graphCounts.current()
	return &statsVal.Stats, nil
}

type localStatsRequest struct {
	res chan localStats
}

type localStats struct {
	txns    int
	atIndex blog.Index
}

func (view *DiskView) genLocalStats(r *localStatsRequest) {
	view.lock.RLock() // TODO, work out if everything accessing txns & atIndex directly are now only on the consumeLog goroutine and remove the lock
	result := localStats{
		txns:    len(view.txns),
		atIndex: view.nextPosition.Index - 1,
	}
	view.lock.RUnlock()
	r.res <- result
	close(r.res)
}

// Stats returns the current state of the common view statistics
func (view *DiskView) Stats(ctx context.Context, req *rpc.StatsRequest) (*rpc.StatsResult, error) {
	statsReq := localStatsRequest{
		res: make(chan localStats, 1),
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case view.statsCh <- statsReq:
	}
	carouselStats := view.carousel.getStats()
	s := rpc.StatsResult{
		ViewType:        "diskview/" + view.cfg.DiskView.Backend,
		Partition:       partitioning.String(view.partition),
		HashPartition:   partitioning.CarouselHashPartition(view.partition),
		UpTime:          time.Since(startTime),
		Hostname:        view.cfg.DiskView.GRPCAddress,
		CarouselRiders:  carouselStats.numRiders,
		LastCarouselKey: []byte(carouselStats.lastKey),
	}
	if strings.HasPrefix(s.Hostname, ":") {
		// Under Kubernetes, the GRPCAddress is usually just a :port.
		osHostname, _ := os.Hostname()
		s.Hostname = osHostname + s.Hostname
	}
	counts := view.graphCounts.current()
	s.Facts, s.FactVersions = counts.FactCount, counts.FactVersions

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case localStats := <-statsReq.res:
		s.Txs = uint32(localStats.txns)
		s.LastIndex = localStats.atIndex
		return &s, nil
	}
}

// UpdateStats allows for a gRPC client to force this disk view to update it statistics about facts
// it should only be used for diagnostics purposes [as this forces a caoursel to run]
func (view *DiskView) UpdateStats(ctx context.Context, req *rpc.UpdateStatsRequest) (*rpc.UpdateStatsResult, error) {
	view.graphCounts.update()
	return &rpc.UpdateStatsResult{}, nil
}

// Profile will run a CPU profile for the requested duration, writing the results
// to the indicated local file. This is primarily for diagnostics & development
func (view *DiskView) Profile(ctx context.Context, req *rpc.ProfileRequest) (*rpc.ProfileResult, error) {
	err := profiling.CPUProfileForDuration(req.Filename, req.Duration)
	return &rpc.ProfileResult{}, err
}

// SetLogLevel allows a gRPC client to dynamically adjust the logrus log level. This does not
// persist across service restarts. This is primarily for diagnostics & development
func (view *DiskView) SetLogLevel(ctx context.Context, req *rpc.LogLevelRequest) (*rpc.LogLevelResult, error) {
	log.SetLevel(log.Level(req.NewLevel))
	log.Printf("Updated log level to %v", log.Level(req.NewLevel))
	return &rpc.LogLevelResult{}, nil
}

// LastLogIndex returns the most recent log index that we can see in the log, this view may
// not have processed this or prior entries yet.
func (view *DiskView) LastLogIndex() (blog.Index, error) {
	info, err := view.aLog.Info(context.TODO())
	if err != nil {
		return 0, err
	}
	return info.LastIndex, nil
}

// LastApplied return the most recent log index that has been processed and applied to the db.
func (view *DiskView) LastApplied() blog.Index {
	view.lock.RLock()
	i := view.nextPosition.Index - 1
	view.lock.RUnlock()
	return i
}

// parseHashRange parses two 64-bit hash values. The inputs must be in the
// format accepted by parseHex64. The inputs are both inclusive (but the
// returned space.Range has an exclusive end hash).
func parseHashRange(first, last string) (space.Range, error) {
	firstHash, err := parseHex64(first)
	if err != nil {
		return space.Range{}, fmt.Errorf("invalid first hash: %v", err)
	}
	lastHash, err := parseHex64(last)
	if err != nil {
		return space.Range{}, fmt.Errorf("invalid last hash: %v", err)
	}
	if firstHash > lastHash {
		return space.Range{}, errors.New("first hash cannot be greater than last hash")
	}
	if lastHash == math.MaxUint64 {
		return space.Range{
			Start: space.Hash64(firstHash),
			End:   space.Infinity,
		}, nil
	}
	return space.Range{
		Start: space.Hash64(firstHash),
		End:   space.Hash64(lastHash + 1),
	}, nil
}

// parseHex64 parses an input in the format 0x%016x. Uppercase A-F digits are
// OK. Any other deviation from this strict format results in an error.
func parseHex64(input string) (uint64, error) {
	if len(input) != 18 || input[:2] != "0x" {
		return 0, errParseHex64
	}
	output, err := strconv.ParseUint(input[2:], 16, 64)
	if err != nil {
		return 0, errParseHex64
	}
	return output, nil
}

var errParseHex64 = errors.New("parseHex64: bad input format (need 0x%016x)")
