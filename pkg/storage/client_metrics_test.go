// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/pkg/errors"
)

func checkGauge(t *testing.T, id string, g *metric.Gauge, e int64) {
	t.Helper()
	if a := g.Value(); a != e {
		t.Error(errors.Errorf("%s for store %s: gauge %d != computed %d", g.GetName(), id, a, e))
	}
}

func verifyStats(t *testing.T, mtc *multiTestContext, storeIdxSlice ...int) {
	t.Helper()
	var stores []*storage.Store
	var wg sync.WaitGroup

	mtc.mu.RLock()
	numStores := len(mtc.stores)
	// We need to stop the stores at the given indexes, while keeping the reference to the
	// store objects. ComputeMVCCStats() still works on a stopped store (it needs
	// only the engine, which is still open), and the most recent stats are still
	// available on the stopped store object; however, no further information can
	// be committed to the store while it is stopped, preventing any races during
	// verification.
	for _, storeIdx := range storeIdxSlice {
		stores = append(stores, mtc.stores[storeIdx])
	}
	mtc.mu.RUnlock()

	// Sanity regression check for bug #4624: ensure intent count is zero.
	// This may not be true immediately due to the asynchronous nature of
	// non-local intent resolution.
	for _, s := range stores {
		m := s.Metrics()
		testutils.SucceedsSoon(t, func() error {
			if a := m.IntentCount.Value(); a != 0 {
				return fmt.Errorf("expected intent count to be zero, was %d", a)
			}
			return nil
		})
	}

	wg.Add(numStores)
	// We actually stop *all* of the Stores. Stopping only a few is riddled
	// with deadlocks since operations can span nodes, but stoppers don't
	// know about this - taking all of them down at the same time is the
	// only sane way of guaranteeing that nothing interesting happens, at
	// least when bringing down the nodes jeopardizes majorities.
	for i := 0; i < numStores; i++ {
		go func(i int) {
			defer wg.Done()
			mtc.stopStore(i)
		}(i)
	}
	wg.Wait()

	for _, s := range stores {
		idString := s.Ident.String()
		m := s.Metrics()

		// Sanity check: LiveBytes is not zero (ensures we don't have
		// zeroed out structures.)
		if liveBytes := m.LiveBytes.Value(); liveBytes == 0 {
			t.Errorf("store %s; got zero live bytes, expected non-zero", idString)
		}

		// Compute real total MVCC statistics from store.
		realStats, err := s.ComputeMVCCStats()
		if err != nil {
			t.Fatal(err)
		}

		// Ensure that real MVCC stats match computed stats.
		checkGauge(t, idString, m.LiveBytes, realStats.LiveBytes)
		checkGauge(t, idString, m.KeyBytes, realStats.KeyBytes)
		checkGauge(t, idString, m.ValBytes, realStats.ValBytes)
		checkGauge(t, idString, m.IntentBytes, realStats.IntentBytes)
		checkGauge(t, idString, m.LiveCount, realStats.LiveCount)
		checkGauge(t, idString, m.KeyCount, realStats.KeyCount)
		checkGauge(t, idString, m.ValCount, realStats.ValCount)
		checkGauge(t, idString, m.IntentCount, realStats.IntentCount)
		checkGauge(t, idString, m.SysBytes, realStats.SysBytes)
		checkGauge(t, idString, m.SysCount, realStats.SysCount)
		// "Ages" will be different depending on how much time has passed. Even with
		// a manual clock, this can be an issue in tests. Therefore, we do not
		// verify them in this test.
	}

	if t.Failed() {
		t.Fatalf("verifyStats failed, aborting test.")
	}

	// Restart all Stores.
	for i := 0; i < numStores; i++ {
		mtc.restartStore(i)
	}
}

func verifyRocksDBStats(t *testing.T, s *storage.Store) {
	if err := s.ComputeMetrics(context.TODO(), 0); err != nil {
		t.Fatal(err)
	}

	m := s.Metrics()
	testcases := []struct {
		gauge *metric.Gauge
		min   int64
	}{
		{m.RdbBlockCacheHits, 10},
		{m.RdbBlockCacheMisses, 0},
		{m.RdbBlockCacheUsage, 0},
		{m.RdbBlockCachePinnedUsage, 0},
		{m.RdbBloomFilterPrefixChecked, 20},
		{m.RdbBloomFilterPrefixUseful, 20},
		{m.RdbMemtableTotalSize, 5000},
		{m.RdbFlushes, 1},
		{m.RdbCompactions, 0},
		{m.RdbTableReadersMemEstimate, 50},
	}
	for _, tc := range testcases {
		if a := tc.gauge.Value(); a < tc.min {
			t.Errorf("gauge %s = %d < min %d", tc.gauge.GetName(), a, tc.min)
		}
	}
}

// TestStoreResolveMetrics verifies that metrics related to intent resolution
// are tracked properly.
func TestStoreResolveMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// First prevent rot that would result from adding fields without handling
	// them everywhere.
	{
		act := fmt.Sprintf("%+v", result.Metrics{})
		exp := "{LeaseRequestSuccess:0 LeaseRequestError:0 LeaseTransferSuccess:0 LeaseTransferError:0 ResolveCommit:0 ResolveAbort:0 ResolvePoison:0}"
		if act != exp {
			t.Errorf("need to update this test due to added fields: %v", act)
		}
	}

	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, 1)

	ctx := context.Background()

	span := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}

	txn := roachpb.MakeTransaction("foo", span.Key, roachpb.MinUserPriority, hlc.Timestamp{WallTime: 123}, 999)

	const resolveCommitCount = int64(200)
	const resolveAbortCount = int64(800)
	const resolvePoisonCount = int64(2400)

	var ba roachpb.BatchRequest
	{
		repl := mtc.stores[0].LookupReplica(keys.MustAddr(span.Key))
		var err error
		if ba.Replica, err = repl.GetReplicaDescriptor(); err != nil {
			t.Fatal(err)
		}
		ba.RangeID = repl.RangeID
	}

	add := func(status roachpb.TransactionStatus, poison bool, n int64) {
		for i := int64(0); i < n; i++ {
			key := span.Key
			endKey := span.EndKey
			if i > n/2 {
				req := &roachpb.ResolveIntentRangeRequest{
					IntentTxn: txn.TxnMeta, Status: status, Poison: poison,
				}
				req.Key, req.EndKey = key, endKey
				ba.Add(req)
				continue
			}
			req := &roachpb.ResolveIntentRequest{
				IntentTxn: txn.TxnMeta, Status: status, Poison: poison,
			}
			req.Key = key
			ba.Add(req)
		}
	}

	add(roachpb.COMMITTED, false, resolveCommitCount)
	add(roachpb.ABORTED, false, resolveAbortCount)
	add(roachpb.ABORTED, true, resolvePoisonCount)

	if _, pErr := mtc.senders[0].Send(ctx, ba); pErr != nil {
		t.Fatal(pErr)
	}

	if exp, act := resolveCommitCount, mtc.stores[0].Metrics().ResolveCommitCount.Count(); act < exp || act > exp+50 {
		t.Errorf("expected around %d intent commits, saw %d", exp, act)
	}
	if exp, act := resolveAbortCount, mtc.stores[0].Metrics().ResolveAbortCount.Count(); act < exp || act > exp+50 {
		t.Errorf("expected around %d intent aborts, saw %d", exp, act)
	}
	if exp, act := resolvePoisonCount, mtc.stores[0].Metrics().ResolvePoisonCount.Count(); act < exp || act > exp+50 {
		t.Errorf("expected arounc %d abort span poisonings, saw %d", exp, act)
	}
}

func TestStoreMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()

	storeCfg := storage.TestStoreConfig(nil /* clock */)
	storeCfg.TestingKnobs.DisableMergeQueue = true
	mtc := &multiTestContext{
		storeConfig: &storeCfg,
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}
	defer mtc.Stop()
	mtc.Start(t, 3)

	// Flush RocksDB memtables, so that RocksDB begins using block-based tables.
	// This is useful, because most of the stats we track don't apply to
	// memtables.
	if err := mtc.stores[0].Engine().Flush(); err != nil {
		t.Fatal(err)
	}
	if err := mtc.stores[1].Engine().Flush(); err != nil {
		t.Fatal(err)
	}

	// Disable the raft log truncation which confuses this test.
	for _, s := range mtc.stores {
		s.SetRaftLogQueueActive(false)
	}

	// Perform a split, which has special metrics handling.
	splitArgs := adminSplitArgs(roachpb.Key("m"))
	if _, err := client.SendWrapped(context.Background(), mtc.stores[0].TestSender(), splitArgs); err != nil {
		t.Fatal(err)
	}

	// Verify range count is as expected
	checkGauge(t, "store 0", mtc.stores[0].Metrics().ReplicaCount, 2)

	// Verify all stats on store0 after split.
	verifyStats(t, mtc, 0)

	// Replicate the "right" range to the other stores.
	replica := mtc.stores[0].LookupReplica(roachpb.RKey("z"))
	mtc.replicateRange(replica.RangeID, 1, 2)

	// Verify stats on store1 after replication.
	verifyStats(t, mtc, 1)

	// Add some data to the "right" range.
	dataKey := []byte("z")
	if _, err := mtc.dbs[0].Inc(context.TODO(), dataKey, 5); err != nil {
		t.Fatal(err)
	}
	mtc.waitForValues(roachpb.Key("z"), []int64{5, 5, 5})

	// Verify all stats on stores after addition.
	verifyStats(t, mtc, 0, 1, 2)

	// Create a transaction statement that fails. Regression test for #4969.
	if err := mtc.dbs[0].Txn(context.TODO(), func(ctx context.Context, txn *client.Txn) error {
		b := txn.NewBatch()
		b.CPutDeprecated(dataKey, 7, 6)
		return txn.Run(ctx, b)
	}); err == nil {
		t.Fatal("Expected transaction error, but none received")
	}

	// Verify stats after addition.
	verifyStats(t, mtc, 0, 1, 2)
	checkGauge(t, "store 0", mtc.stores[0].Metrics().ReplicaCount, 2)

	// Unreplicate range from the first store.
	testutils.SucceedsSoon(t, func() error {
		// This statement can fail if store 0 is not the leaseholder.
		if err := mtc.transferLeaseNonFatal(context.TODO(), replica.RangeID, 0, 1); err != nil {
			t.Log(err)
		}
		// This statement will fail if store 0 IS the leaseholder. This can happen
		// even after the previous statement.
		return mtc.unreplicateRangeNonFatal(replica.RangeID, 0)
	})

	// Force GC Scan on store 0 in order to fully remove range.
	mtc.stores[1].MustForceReplicaGCScanAndProcess()
	mtc.waitForValues(roachpb.Key("z"), []int64{0, 5, 5})

	// Verify range count is as expected.
	checkGauge(t, "store 0", mtc.stores[0].Metrics().ReplicaCount, 1)
	checkGauge(t, "store 1", mtc.stores[1].Metrics().ReplicaCount, 1)

	// Verify all stats on all stores after range is removed.
	verifyStats(t, mtc, 0, 1, 2)

	verifyRocksDBStats(t, mtc.stores[0])
	verifyRocksDBStats(t, mtc.stores[1])
}
