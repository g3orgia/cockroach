// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package blobs

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type benchmarkTestCase struct {
	localNodeID       roachpb.NodeID
	remoteNodeID      roachpb.NodeID
	localExternalDir  string
	remoteExternalDir string

	blobClient BlobClient
	fileSize   int64
	fileName   string
}

var fileSizes = []int64{1<<20, 64 * 1<<20, 265 * 1<<20, 1<<30}
var chunkSizes = []int{0, 8 * 1<<10, 32 * 1<<10, 64 * 1<<10, 128 * 1<<10, 256 * 1<<10}

func writeLargeFile(t testing.TB, file string, size int64) {
	err := os.MkdirAll(filepath.Dir(file), 0755)
	if err != nil {
		t.Fatal(err)
	}
	content := make([]byte, size)
	err = ioutil.WriteFile(file, content, 0600)
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkStreamingReadFile(b *testing.B) {
	localNodeID := roachpb.NodeID(1)
	remoteNodeID := roachpb.NodeID(2)
	localExternalDir, remoteExternalDir, stopper, cleanUpFn := createTestResources(b)
	defer cleanUpFn()

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	factory := setUpService(b, rpcContext, localNodeID, remoteNodeID, localExternalDir, remoteExternalDir)
	blobClient, err := factory(context.TODO(), remoteNodeID)
	if err != nil {
		b.Fatal(err)
	}
	params := &benchmarkTestCase{
		localNodeID:       localNodeID,
		remoteNodeID:      remoteNodeID,
		localExternalDir:  localExternalDir,
		remoteExternalDir: remoteExternalDir,
		blobClient:        blobClient,
		fileSize:          0, // we should set this for every test.
		fileName:	         "test/largefile.csv",
	}

	for _, s := range fileSizes {
		for _, chunk := range chunkSizes {
			withStreaming := true
			if chunk == 0 {
				withStreaming = false
			}
			chunkSize = chunk
			b.Run(fmt.Sprintf("filesize=%d/streaming=%v/chunksize=%d", s, withStreaming, chunk), func(b *testing.B) {
				params.fileSize = s
				params := *params
				if !withStreaming {
					bc := params.blobClient
					params.blobClient = &remoteClientNotStreamed{remoteClient:bc.(*remoteClient)}
				}
				benchmarkStreamingReadFile(b, &params)
			})
		}
	}
}

func benchmarkStreamingReadFile(b *testing.B, tc *benchmarkTestCase) {
	writeLargeFile(b, filepath.Join(tc.remoteExternalDir, tc.fileName), tc.fileSize)
	writeTo := localStorage{ externalIODir: tc.localExternalDir }
	b.ResetTimer()
	b.SetBytes(tc.fileSize)
	for i := 0; i < b.N; i++ {
		reader, err := tc.blobClient.ReadFile(context.TODO(), tc.fileName)
		if err != nil {
			b.Fatal(err)
		}
		err = writeTo.WriteFile(tc.fileName, reader)
		if err != nil {
			b.Fatal(err)
		}
		stat, err := writeTo.Stat(tc.fileName)
		if err != nil {
			b.Fatal(err)
		}
		if stat.Filesize != tc.fileSize {
			b.Fatal("incorrect number of bytes written")
		}
	}
}

func BenchmarkStreamingWriteFile(b *testing.B) {
	localNodeID := roachpb.NodeID(1)
	remoteNodeID := roachpb.NodeID(2)
	localExternalDir, remoteExternalDir, stopper, cleanUpFn := createTestResources(b)
	defer cleanUpFn()

	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)
	rpcContext := rpc.NewInsecureTestingContext(clock, stopper)
	rpcContext.TestingAllowNamedRPCToAnonymousServer = true

	factory := setUpService(b, rpcContext, localNodeID, remoteNodeID, localExternalDir, remoteExternalDir)
	blobClient, err := factory(context.TODO(), remoteNodeID)
	if err != nil {
		b.Fatal(err)
	}
	params := &benchmarkTestCase{
		localNodeID:       localNodeID,
		remoteNodeID:      remoteNodeID,
		localExternalDir:  localExternalDir,
		remoteExternalDir: remoteExternalDir,
		blobClient:        blobClient,
		fileSize:          0, // we should set this for every test.
		fileName:	         "test/largefile.csv",
	}

	for _, s := range fileSizes {
		for _, chunk := range chunkSizes {
			withStreaming := true
			if chunk == 0 {
				withStreaming = false
			}
			chunkSize = chunk
			b.Run(fmt.Sprintf("filesize=%d/streaming=%v/chunksize=%d", s, withStreaming, chunk), func(b *testing.B) {
				params.fileSize = s
				params := *params
				if !withStreaming {
					bc := params.blobClient
					params.blobClient = &remoteClientNotStreamed{remoteClient:bc.(*remoteClient)}
				}
				benchmarkStreamingWriteFile(b, &params)
			})
		}
	}
}

func benchmarkStreamingWriteFile(b *testing.B, tc *benchmarkTestCase) {
	content := make([]byte, tc.fileSize)

	b.ResetTimer()
	b.SetBytes(tc.fileSize)
	for i := 0; i < b.N; i++ {
		err := tc.blobClient.WriteFile(context.TODO(), tc.fileName, bytes.NewReader(content))
		if err != nil {
			b.Fatal(err)
		}
	}
}
