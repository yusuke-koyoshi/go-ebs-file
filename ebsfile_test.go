package ebsfile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ebs"
	"github.com/aws/aws-sdk-go-v2/service/ebs/types"
)

func TestWalkSnapshotBlocks_Pagination(t *testing.T) {
	blockSize := int32(16)
	numBlocks := int32(10)
	volumeSize := int64(numBlocks) * int64(blockSize) // 160 bytes

	data := make([]byte, volumeSize)
	for i := int32(0); i < numBlocks; i++ {
		data[int64(i)*int64(blockSize)] = byte(i)
	}

	mock := &MockEBS{
		Reader:     bytes.NewReader(data),
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(volumeSize),
		PageSize:   3, // 3 blocks per page → 4 pages for 10 blocks
	}

	sr, err := Open("snap-pagination", context.Background(), nil, mock)
	if err != nil {
		t.Fatal(err)
	}

	if sr.Size() != volumeSize {
		t.Fatalf("expected size %d, got %d", volumeSize, sr.Size())
	}

	// Read first byte of each block to verify all pages were collected
	buf := make([]byte, 1)
	for i := int32(0); i < numBlocks; i++ {
		off := int64(i) * int64(blockSize)
		n, err := sr.ReadAt(buf, off)
		if err != nil {
			t.Fatalf("block %d: unexpected error: %v", i, err)
		}
		if n != 1 || buf[0] != byte(i) {
			t.Errorf("block %d: got n=%d, buf[0]=0x%02X, want 0x%02X", i, n, buf[0], byte(i))
		}
	}
}

func TestWalkSnapshotBlocks_PaginationWithUnallocated(t *testing.T) {
	blockSize := int32(16)
	numBlocks := int32(8)
	volumeSize := int64(numBlocks) * int64(blockSize) // 128 bytes

	data := make([]byte, volumeSize)
	allocated := make(map[int32]bool)
	// Allocate only even blocks
	for i := int32(0); i < numBlocks; i++ {
		if i%2 == 0 {
			allocated[i] = true
			data[int64(i)*int64(blockSize)] = 0xAA
		}
	}

	mock := &MockEBS{
		Reader:     bytes.NewReader(data),
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(volumeSize),
		Allocated:  allocated,
		PageSize:   2, // 2 blocks per page → multiple pages
	}

	sr, err := Open("snap-unalloc", context.Background(), nil, mock)
	if err != nil {
		t.Fatal(err)
	}

	// Allocated block 0: should have marker
	buf := make([]byte, 1)
	n, err := sr.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 1 || buf[0] != 0xAA {
		t.Errorf("block 0: got n=%d, buf[0]=0x%02X, want 0xAA", n, buf[0])
	}

	// Unallocated block 1: should be zero
	buf[0] = 0xFF
	n, err = sr.ReadAt(buf, int64(blockSize))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 1 || buf[0] != 0x00 {
		t.Errorf("block 1: got n=%d, buf[0]=0x%02X, want 0x00", n, buf[0])
	}
}

func TestReadAt_UnallocatedBlockZeroFill(t *testing.T) {
	blockSize := int32(16)
	volumeSize := int64(48) // 3 blocks

	data := make([]byte, volumeSize)
	// Fill allocated blocks with non-zero data
	for i := 0; i < int(blockSize); i++ {
		data[i] = 0xAA                  // block 0
		data[int(blockSize)*2+i] = 0xBB // block 2
	}

	mock := &MockEBS{
		Reader:     bytes.NewReader(data),
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(volumeSize),
		Allocated:  map[int32]bool{0: true, 2: true}, // block 1 is unallocated
	}

	sr, err := Open("snap-test", context.Background(), nil, mock)
	if err != nil {
		t.Fatal(err)
	}

	// Read from unallocated block 1: should be all zeros
	buf := make([]byte, blockSize)
	// Pre-fill with garbage to verify zero-fill
	for i := 0; i < len(buf); i++ {
		buf[i] = 0xFF
	}
	n, err := sr.ReadAt(buf, int64(blockSize))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != int(blockSize) {
		t.Fatalf("expected n=%d, got %d", blockSize, n)
	}
	for i, b := range buf {
		if b != 0 {
			t.Errorf("buf[%d] = 0x%02X, want 0x00", i, b)
		}
	}
}

func TestReadAt_CrossBlockBoundary(t *testing.T) {
	blockSize := int32(16)
	volumeSize := int64(48) // 3 blocks

	data := make([]byte, volumeSize)
	// Fill with distinct patterns per block
	for i := 0; i < int(blockSize); i++ {
		data[i] = 0xAA
		data[int(blockSize)+i] = 0xBB
		data[int(blockSize)*2+i] = 0xCC
	}

	mock := &MockEBS{
		Reader:     bytes.NewReader(data),
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(volumeSize),
	}

	sr, err := Open("snap-test", context.Background(), nil, mock)
	if err != nil {
		t.Fatal(err)
	}

	// Read across block 0 and block 1 boundary
	// Start at offset 8 (middle of block 0), read 16 bytes (spans into block 1)
	buf := make([]byte, 16)
	n, err := sr.ReadAt(buf, 8)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 16 {
		t.Fatalf("expected n=16, got %d", n)
	}
	// First 8 bytes should be from block 0 (0xAA)
	for i := 0; i < 8; i++ {
		if buf[i] != 0xAA {
			t.Errorf("buf[%d] = 0x%02X, want 0xAA", i, buf[i])
		}
	}
	// Next 8 bytes should be from block 1 (0xBB)
	for i := 8; i < 16; i++ {
		if buf[i] != 0xBB {
			t.Errorf("buf[%d] = 0x%02X, want 0xBB", i, buf[i])
		}
	}
}

func TestReadAt_CrossBlockBoundaryWithUnallocated(t *testing.T) {
	blockSize := int32(16)
	volumeSize := int64(48) // 3 blocks

	data := make([]byte, volumeSize)
	for i := 0; i < int(blockSize); i++ {
		data[i] = 0xAA
		data[int(blockSize)*2+i] = 0xCC
	}

	mock := &MockEBS{
		Reader:     bytes.NewReader(data),
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(volumeSize),
		Allocated:  map[int32]bool{0: true, 2: true}, // block 1 unallocated
	}

	sr, err := Open("snap-test", context.Background(), nil, mock)
	if err != nil {
		t.Fatal(err)
	}

	// Read across block 0, unallocated block 1, and block 2
	buf := make([]byte, 32)
	for i := 0; i < len(buf); i++ {
		buf[i] = 0xFF
	}
	n, err := sr.ReadAt(buf, 8) // offset 8 in block 0
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 32 {
		t.Fatalf("expected n=32, got %d", n)
	}
	// [0..7]: block 0 tail (0xAA)
	for i := 0; i < 8; i++ {
		if buf[i] != 0xAA {
			t.Errorf("buf[%d] = 0x%02X, want 0xAA", i, buf[i])
		}
	}
	// [8..23]: unallocated block 1 (0x00)
	for i := 8; i < 24; i++ {
		if buf[i] != 0x00 {
			t.Errorf("buf[%d] = 0x%02X, want 0x00", i, buf[i])
		}
	}
	// [24..31]: block 2 head (0xCC)
	for i := 24; i < 32; i++ {
		if buf[i] != 0xCC {
			t.Errorf("buf[%d] = 0x%02X, want 0xCC", i, buf[i])
		}
	}
}

func TestReadAt_EOF(t *testing.T) {
	blockSize := int32(16)
	volumeSize := int64(32) // 2 blocks

	data := make([]byte, volumeSize)
	for i := 0; i < len(data); i++ {
		data[i] = 0xDD
	}

	mock := &MockEBS{
		Reader:     bytes.NewReader(data),
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(volumeSize),
	}

	sr, err := Open("snap-test", context.Background(), nil, mock)
	if err != nil {
		t.Fatal(err)
	}

	// Read beyond EOF: offset at volumeSize
	buf := make([]byte, 8)
	n, err := sr.ReadAt(buf, volumeSize)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected n=0, got %d", n)
	}

	// Read that extends past EOF
	buf = make([]byte, 24)
	n, err = sr.ReadAt(buf, int64(blockSize))
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != int(blockSize) {
		t.Fatalf("expected n=%d, got %d", blockSize, n)
	}
	for i := 0; i < int(blockSize); i++ {
		if buf[i] != 0xDD {
			t.Errorf("buf[%d] = 0x%02X, want 0xDD", i, buf[i])
		}
	}
}

// countingEBS counts GetSnapshotBlock calls and records the largest number of
// calls in flight at once. If release is set, each call signals entered and
// blocks until release is closed. If mutate is set, it can change the output
// of each call; call is 1 for the first call. If blockErr is set and returns
// an error for a block, the call fails with it.
type countingEBS struct {
	EBSAPI

	calls       atomic.Int64
	inFlight    atomic.Int64
	maxInFlight atomic.Int64
	entered     chan struct{}
	release     chan struct{}
	err         error
	blockErr    func(index int32) error
	latency     time.Duration
	mutate      func(call int64, out *ebs.GetSnapshotBlockOutput)
}

func (c *countingEBS) GetSnapshotBlock(ctx context.Context, params *ebs.GetSnapshotBlockInput, optFns ...func(*ebs.Options)) (*ebs.GetSnapshotBlockOutput, error) {
	call := c.calls.Add(1)
	inFlight := c.inFlight.Add(1)
	defer c.inFlight.Add(-1)
	for {
		prev := c.maxInFlight.Load()
		if inFlight <= prev || c.maxInFlight.CompareAndSwap(prev, inFlight) {
			break
		}
	}
	if c.entered != nil {
		select {
		case c.entered <- struct{}{}:
		default:
		}
	}
	if c.release != nil {
		<-c.release
	}
	if c.latency > 0 {
		time.Sleep(c.latency)
	}
	if c.err != nil {
		return nil, c.err
	}
	if c.blockErr != nil {
		if err := c.blockErr(*params.BlockIndex); err != nil {
			return nil, err
		}
	}
	out, err := c.EBSAPI.GetSnapshotBlock(ctx, params, optFns...)
	if err == nil && c.mutate != nil {
		c.mutate(call, out)
	}
	return out, err
}

type mapCache struct {
	mu     sync.Mutex
	values map[string][]byte
}

func newMapCache() *mapCache {
	return &mapCache{values: make(map[string][]byte)}
}

func (c *mapCache) Add(key string, value []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values[key] = value
	return true
}

func (c *mapCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.values[key]
	return value, ok
}

// newPatternMock fills each offset with a distinct-enough byte so that data
// copied to the wrong place is detected.
func newPatternMock(blockSize, numBlocks int32) (*MockEBS, []byte) {
	data := make([]byte, int(blockSize)*int(numBlocks))
	for i := range data {
		data[i] = byte(i % 251)
	}
	return &MockEBS{
		Reader:     bytes.NewReader(data),
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(int64(len(data))),
	}, data
}

// waitForFetch releases the in-flight fetch after giving the other goroutines
// time to request the same block; without coalescing, each would fetch it.
func waitForFetch(t *testing.T, api *countingEBS) {
	t.Helper()
	select {
	case <-api.entered:
	case <-time.After(5 * time.Second):
		close(api.release)
		t.Fatal("GetSnapshotBlock was not called")
	}
	time.Sleep(50 * time.Millisecond)
	close(api.release)
}

func TestReadAt_SingleflightSameBlock(t *testing.T) {
	mock, data := newPatternMock(16, 2)
	api := &countingEBS{
		EBSAPI:  mock,
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	sr, err := Open("snap-test", context.Background(), newMapCache(), api)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 32
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := make([]byte, 4)
			if _, err := sr.ReadAt(buf, 2); err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(buf, data[2:6]) {
				errs <- errors.New("ReadAt returned wrong data")
			}
		}()
	}
	waitForFetch(t, api)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	if got := api.calls.Load(); got != 1 {
		t.Fatalf("GetSnapshotBlock calls = %d, want 1", got)
	}
}

func TestReadAt_FetchErrorIsReturnedAndNotCached(t *testing.T) {
	errFetch := errors.New("fetch failed")
	mock, data := newPatternMock(16, 1)
	api := &countingEBS{
		EBSAPI:  mock,
		entered: make(chan struct{}, 1),
		release: make(chan struct{}),
		err:     errFetch,
	}
	sr, err := Open("snap-test", context.Background(), newMapCache(), api)
	if err != nil {
		t.Fatal(err)
	}

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := sr.ReadAt(make([]byte, 4), 0)
			errs <- err
		}()
	}
	waitForFetch(t, api)
	wg.Wait()
	close(errs)
	for err := range errs {
		if !errors.Is(err, errFetch) {
			t.Errorf("ReadAt error = %v, want %v", err, errFetch)
		}
	}

	api.err = nil
	callsBefore := api.calls.Load()
	buf := make([]byte, 4)
	if _, err := sr.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt after recovery: %v", err)
	}
	if !bytes.Equal(buf, data[0:4]) {
		t.Fatalf("ReadAt after recovery = %v, want %v", buf, data[0:4])
	}
	if got := api.calls.Load() - callsBefore; got != 1 {
		t.Fatalf("GetSnapshotBlock calls after recovery = %d, want 1", got)
	}
}

func TestReadAt_ConcurrentReads(t *testing.T) {
	const blockSize, numBlocks = int32(16), int32(8)
	mock, data := newPatternMock(blockSize, numBlocks)
	mock.Allocated = map[int32]bool{0: true, 1: true, 2: true, 4: true, 5: true, 6: true, 7: true} // block 3 is unallocated
	for i := 3 * int(blockSize); i < 4*int(blockSize); i++ {
		data[i] = 0
	}

	sr, err := Open("snap-test", context.Background(), nil, mock)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			off := int64((i * 7) % (len(data) - 24))
			buf := make([]byte, 24)
			n, err := sr.ReadAt(buf, off)
			if err != nil || n != len(buf) || !bytes.Equal(buf, data[off:off+int64(len(buf))]) {
				t.Errorf("ReadAt(%d): n=%d err=%v", off, n, err)
			}
		}(i)
	}
	wg.Wait()
}

// BenchmarkReadAt_ConcurrentSameBlock reports GetSnapshotBlock calls per round
// of 8 goroutines reading the same uncached block.
func BenchmarkReadAt_ConcurrentSameBlock(b *testing.B) {
	const goroutines = 8
	mock, _ := newPatternMock(512*1024, 1)
	api := &countingEBS{EBSAPI: mock, latency: 100 * time.Microsecond}
	sr, err := Open("benchmark-snapshot", context.Background(), nil, api)
	if err != nil {
		b.Fatal(err)
	}

	bufs := make([][]byte, goroutines)
	for i := range bufs {
		bufs[i] = make([]byte, 4*1024)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(buf []byte) {
				defer wg.Done()
				if _, err := sr.ReadAt(buf, 0); err != nil {
					b.Error(err)
				}
			}(bufs[g])
		}
		wg.Wait()
	}
	b.StopTimer()
	b.ReportMetric(float64(api.calls.Load())/float64(b.N), "requests/op")
}

func newLatencyMock(latency time.Duration) *MockEBS {
	const blockSize = int32(16)
	return &MockEBS{
		Reader:     bytes.NewReader(make([]byte, blockSize)),
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(int64(blockSize)),
		Latency:    latency,
	}
}

func TestMockEBS_GetSnapshotBlockCount(t *testing.T) {
	mock := newLatencyMock(0)
	params := &ebs.GetSnapshotBlockInput{BlockIndex: aws.Int32(0)}

	if got := mock.GetSnapshotBlockCount(); got != 0 {
		t.Fatalf("initial count = %d, want 0", got)
	}
	for i := 0; i < 3; i++ {
		if _, err := mock.GetSnapshotBlock(context.Background(), params); err != nil {
			t.Fatal(err)
		}
	}
	if got := mock.GetSnapshotBlockCount(); got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}
}

func TestMockEBS_GetSnapshotBlockCount_Concurrent(t *testing.T) {
	mock := newLatencyMock(0)
	params := &ebs.GetSnapshotBlockInput{BlockIndex: aws.Int32(0)}

	const goroutines = 8
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if _, err := mock.GetSnapshotBlock(context.Background(), params); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	if got := mock.GetSnapshotBlockCount(); got != goroutines {
		t.Fatalf("count = %d, want %d", got, goroutines)
	}
}

func TestMockEBS_Latency(t *testing.T) {
	const latency = 50 * time.Millisecond
	mock := newLatencyMock(latency)

	start := time.Now()
	if _, err := mock.ListSnapshotBlocks(context.Background(), &ebs.ListSnapshotBlocksInput{}); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < latency {
		t.Errorf("ListSnapshotBlocks took %v, want at least %v", elapsed, latency)
	}

	start = time.Now()
	params := &ebs.GetSnapshotBlockInput{BlockIndex: aws.Int32(0)}
	if _, err := mock.GetSnapshotBlock(context.Background(), params); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < latency {
		t.Errorf("GetSnapshotBlock took %v, want at least %v", elapsed, latency)
	}
}

func TestMockEBS_LatencyUnset(t *testing.T) {
	mock := newLatencyMock(0)
	params := &ebs.GetSnapshotBlockInput{BlockIndex: aws.Int32(0)}

	start := time.Now()
	if _, err := mock.GetSnapshotBlock(context.Background(), params); err != nil {
		t.Fatal(err)
	}
	// Loose bound so that GC or scheduler pauses under -race cannot fail the test.
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("GetSnapshotBlock took %v, want no delay", elapsed)
	}
}

func TestMockEBS_GetSnapshotBlockCount_ReadAt(t *testing.T) {
	blockSize := int32(16)
	numBlocks := int32(4)
	mock := &MockEBS{
		Reader:     bytes.NewReader(make([]byte, blockSize*numBlocks)),
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(int64(blockSize * numBlocks)),
		Allocated:  map[int32]bool{0: true, 1: true, 3: true}, // block 2 is unallocated
		Latency:    time.Millisecond,
	}

	// A nil cache never hits, so every allocated block is fetched.
	sr, err := Open("snap-test", context.Background(), nil, mock)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, blockSize*numBlocks)
	if _, err := sr.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if got := mock.GetSnapshotBlockCount(); got != 3 {
		t.Fatalf("count = %d, want 3", got)
	}
}

func TestMockEBS_ContextCanceledBeforeCall(t *testing.T) {
	// Latency is unset: the context is checked even without a delay.
	mock := newLatencyMock(0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	params := &ebs.GetSnapshotBlockInput{BlockIndex: aws.Int32(0)}
	if _, err := mock.GetSnapshotBlock(ctx, params); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetSnapshotBlock error = %v, want context.Canceled", err)
	}
	if _, err := mock.ListSnapshotBlocks(ctx, &ebs.ListSnapshotBlocksInput{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListSnapshotBlocks error = %v, want context.Canceled", err)
	}

	// The call was issued, so it is counted even though it failed.
	if got := mock.GetSnapshotBlockCount(); got != 1 {
		t.Fatalf("count = %d, want 1", got)
	}
}

func TestMockEBS_ContextCanceledDuringLatency(t *testing.T) {
	mock := newLatencyMock(30 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	defer cancel()

	start := time.Now()
	params := &ebs.GetSnapshotBlockInput{BlockIndex: aws.Int32(0)}
	if _, err := mock.GetSnapshotBlock(ctx, params); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetSnapshotBlock error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed >= mock.Latency {
		t.Errorf("GetSnapshotBlock took %v, want it to abort well before %v", elapsed, mock.Latency)
	}
}

func newChecksumFixture(t *testing.T, mutate func(int64, *ebs.GetSnapshotBlockOutput), options OpenOptions) (*countingEBS, *io.SectionReader, []byte) {
	t.Helper()
	mock, data := newPatternMock(16, 1)
	api := &countingEBS{EBSAPI: mock, mutate: mutate}
	sr, err := OpenWithOptions("snap-test", context.Background(), newMapCache(), api, options)
	if err != nil {
		t.Fatal(err)
	}
	return api, sr, data
}

// corruptBody flips a byte of the block data and keeps the original checksum.
func corruptBody(out *ebs.GetSnapshotBlockOutput) {
	body, _ := io.ReadAll(out.BlockData)
	body[0] ^= 0xFF
	out.BlockData = io.NopCloser(bytes.NewReader(body))
}

func TestMockEBS_GetSnapshotBlockReturnsChecksum(t *testing.T) {
	mock, _ := newPatternMock(16, 1)
	out, err := mock.GetSnapshotBlock(context.Background(), &ebs.GetSnapshotBlockInput{BlockIndex: aws.Int32(0)})
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(out.BlockData)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	if out.ChecksumAlgorithm != types.ChecksumAlgorithmChecksumAlgorithmSha256 {
		t.Errorf("ChecksumAlgorithm = %q, want SHA256", out.ChecksumAlgorithm)
	}
	if got, want := aws.ToString(out.Checksum), base64.StdEncoding.EncodeToString(sum[:]); got != want {
		t.Errorf("Checksum = %q, want %q", got, want)
	}
}

func TestReadAt_ChecksumMismatchIsRefetched(t *testing.T) {
	api, sr, data := newChecksumFixture(t, func(call int64, out *ebs.GetSnapshotBlockOutput) {
		if call == 1 {
			corruptBody(out)
		}
	}, OpenOptions{})

	buf := make([]byte, 4)
	if _, err := sr.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, data[:4]) {
		t.Errorf("ReadAt = %v, want %v", buf, data[:4])
	}
	if got := api.calls.Load(); got != 2 {
		t.Errorf("GetSnapshotBlock calls = %d, want 2", got)
	}
}

func TestReadAt_PersistentChecksumMismatchIsNotCached(t *testing.T) {
	var corrupt atomic.Bool
	corrupt.Store(true)
	api, sr, data := newChecksumFixture(t, func(_ int64, out *ebs.GetSnapshotBlockOutput) {
		if corrupt.Load() {
			corruptBody(out)
		}
	}, OpenOptions{})

	if _, err := sr.ReadAt(make([]byte, 4), 0); !errors.Is(err, errChecksumMismatch) {
		t.Fatalf("ReadAt error = %v, want %v", err, errChecksumMismatch)
	}
	if got := api.calls.Load(); got != DefaultBlockFetchAttempts {
		t.Fatalf("GetSnapshotBlock calls = %d, want %d", got, DefaultBlockFetchAttempts)
	}

	corrupt.Store(false)
	buf := make([]byte, 4)
	if _, err := sr.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt after recovery: %v", err)
	}
	if !bytes.Equal(buf, data[:4]) {
		t.Errorf("ReadAt after recovery = %v, want %v", buf, data[:4])
	}
	if got := api.calls.Load(); got != DefaultBlockFetchAttempts+1 {
		t.Errorf("GetSnapshotBlock calls = %d, want %d", got, DefaultBlockFetchAttempts+1)
	}
}

func TestReadAt_BlockFetchAttemptsOneDisablesRefetch(t *testing.T) {
	api, sr, _ := newChecksumFixture(t, func(_ int64, out *ebs.GetSnapshotBlockOutput) {
		corruptBody(out)
	}, OpenOptions{BlockFetchAttempts: 1})

	if _, err := sr.ReadAt(make([]byte, 4), 0); !errors.Is(err, errChecksumMismatch) {
		t.Fatalf("ReadAt error = %v, want %v", err, errChecksumMismatch)
	}
	if got := api.calls.Load(); got != 1 {
		t.Errorf("GetSnapshotBlock calls = %d, want 1", got)
	}
}

func TestReadAt_MissingChecksum(t *testing.T) {
	removeChecksum := func(_ int64, out *ebs.GetSnapshotBlockOutput) {
		out.Checksum = nil
	}

	t.Run("is an error by default", func(t *testing.T) {
		api, sr, _ := newChecksumFixture(t, removeChecksum, OpenOptions{})
		if _, err := sr.ReadAt(make([]byte, 4), 0); !errors.Is(err, errMissingChecksum) {
			t.Fatalf("ReadAt error = %v, want %v", err, errMissingChecksum)
		}
		if got := api.calls.Load(); got != 1 {
			t.Errorf("GetSnapshotBlock calls = %d, want 1", got)
		}
	})

	t.Run("is accepted with AllowMissingChecksum", func(t *testing.T) {
		api, sr, data := newChecksumFixture(t, removeChecksum, OpenOptions{AllowMissingChecksum: true})
		buf := make([]byte, 4)
		if _, err := sr.ReadAt(buf, 0); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf, data[:4]) {
			t.Errorf("ReadAt = %v, want %v", buf, data[:4])
		}
		if got := api.calls.Load(); got != 1 {
			t.Errorf("GetSnapshotBlock calls = %d, want 1", got)
		}
	})
}

func TestReadAt_UnsupportedChecksumAlgorithm(t *testing.T) {
	api, sr, _ := newChecksumFixture(t, func(_ int64, out *ebs.GetSnapshotBlockOutput) {
		out.ChecksumAlgorithm = "MD5"
	}, OpenOptions{AllowMissingChecksum: true})

	_, err := sr.ReadAt(make([]byte, 4), 0)
	if err == nil || !strings.Contains(err.Error(), "unsupported checksum algorithm") {
		t.Fatalf("ReadAt error = %v, want unsupported checksum algorithm", err)
	}
	if got := api.calls.Load(); got != 1 {
		t.Errorf("GetSnapshotBlock calls = %d, want 1", got)
	}
}

func TestReadAt_BodyReadErrorIsRefetched(t *testing.T) {
	api, sr, data := newChecksumFixture(t, func(call int64, out *ebs.GetSnapshotBlockOutput) {
		if call == 1 {
			body, _ := io.ReadAll(out.BlockData)
			out.BlockData = io.NopCloser(io.MultiReader(bytes.NewReader(body[:8]), iotest.ErrReader(errors.New("connection reset"))))
		}
	}, OpenOptions{})

	buf := make([]byte, 4)
	if _, err := sr.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, data[:4]) {
		t.Errorf("ReadAt = %v, want %v", buf, data[:4])
	}
	if got := api.calls.Load(); got != 2 {
		t.Errorf("GetSnapshotBlock calls = %d, want 2", got)
	}
}

func TestReadAt_ShortBodyIsRefetchedThenFails(t *testing.T) {
	api, sr, _ := newChecksumFixture(t, func(_ int64, out *ebs.GetSnapshotBlockOutput) {
		body, _ := io.ReadAll(out.BlockData)
		out.BlockData = io.NopCloser(bytes.NewReader(body[:8]))
	}, OpenOptions{})

	_, err := sr.ReadAt(make([]byte, 4), 0)
	if err == nil || !strings.Contains(err.Error(), "invalid block size") {
		t.Fatalf("ReadAt error = %v, want invalid block size", err)
	}
	if got := api.calls.Load(); got != DefaultBlockFetchAttempts {
		t.Errorf("GetSnapshotBlock calls = %d, want %d", got, DefaultBlockFetchAttempts)
	}
}

func TestReadAt_MultiBlockReadFetchesConcurrently(t *testing.T) {
	const numBlocks = 4
	mock, data := newPatternMock(16, numBlocks)
	api := &countingEBS{
		EBSAPI:  mock,
		entered: make(chan struct{}, numBlocks),
		release: make(chan struct{}),
	}
	sr, err := Open("snap-test", context.Background(), nil, api)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, len(data)-8)
	done := make(chan error, 1)
	go func() {
		_, err := sr.ReadAt(buf, 4)
		done <- err
	}()
	// Each call blocks until release is closed, so all blocks enter only if
	// they are fetched concurrently.
	for i := 0; i < numBlocks; i++ {
		select {
		case <-api.entered:
		case <-time.After(5 * time.Second):
			close(api.release)
			t.Fatalf("%d of %d blocks were being fetched at once", i, numBlocks)
		}
	}
	close(api.release)

	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, data[4:len(data)-4]) {
		t.Error("ReadAt returned wrong data")
	}
}

func TestReadAt_MaxConcurrentBlocks(t *testing.T) {
	mock, data := newPatternMock(16, 4)
	api := &countingEBS{EBSAPI: mock, latency: 20 * time.Millisecond}
	sr, err := OpenWithOptions("snap-test", context.Background(), nil, api, OpenOptions{MaxConcurrentBlocks: 2})
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, len(data))
	if _, err := sr.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, data) {
		t.Error("ReadAt returned wrong data")
	}
	if got := api.maxInFlight.Load(); got != 2 {
		t.Errorf("GetSnapshotBlock calls in flight = %d, want 2", got)
	}
}

func TestReadAt_MultiBlockReadUsesCachedBlocks(t *testing.T) {
	mock, data := newPatternMock(16, 3)
	api := &countingEBS{EBSAPI: mock}
	sr, err := Open("snap-test", context.Background(), newMapCache(), api)
	if err != nil {
		t.Fatal(err)
	}

	for round := 0; round < 2; round++ {
		buf := make([]byte, len(data)-8)
		if _, err := sr.ReadAt(buf, 4); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf, data[4:len(data)-4]) {
			t.Errorf("round %d: ReadAt returned wrong data", round)
		}
	}
	if got := api.calls.Load(); got != 3 {
		t.Errorf("GetSnapshotBlock calls = %d, want 3", got)
	}
}

func TestReadAt_SharedCacheSeparatesSnapshots(t *testing.T) {
	cache := newMapCache()
	mockA, dataA := newPatternMock(16, 1)
	dataB := bytes.Repeat([]byte{0xAB}, 16)
	mockB := &MockEBS{
		Reader:     bytes.NewReader(dataB),
		BlockSize:  aws.Int32(16),
		VolumeSize: aws.Int64(16),
	}

	srA, err := Open("snap-a", context.Background(), cache, mockA)
	if err != nil {
		t.Fatal(err)
	}
	srB, err := Open("snap-b", context.Background(), cache, mockB)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		sr   *io.SectionReader
		want []byte
	}{{srA, dataA}, {srB, dataB}} {
		buf := make([]byte, 16)
		if _, err := tc.sr.ReadAt(buf, 0); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf, tc.want) {
			t.Errorf("ReadAt = %v, want %v", buf, tc.want)
		}
	}
}

func TestReadAt_ErrorInMiddleBlockReturnsPrecedingBytes(t *testing.T) {
	errFetch := errors.New("fetch failed")
	mock, data := newPatternMock(16, 3)
	api := &countingEBS{
		EBSAPI: mock,
		blockErr: func(index int32) error {
			if index == 1 {
				return errFetch
			}
			return nil
		},
	}
	sr, err := Open("snap-test", context.Background(), nil, api)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 40)
	n, err := sr.ReadAt(buf, 4)
	if !errors.Is(err, errFetch) {
		t.Fatalf("ReadAt error = %v, want %v", err, errFetch)
	}
	if n != 12 {
		t.Fatalf("ReadAt n = %d, want 12", n)
	}
	if !bytes.Equal(buf[:n], data[4:16]) {
		t.Errorf("ReadAt = %v, want %v", buf[:n], data[4:16])
	}
	if got := api.calls.Load(); got != 3 {
		t.Errorf("GetSnapshotBlock calls = %d, want 3", got)
	}
}

// recordBlocks makes api record the index of each GetSnapshotBlock call.
func recordBlocks(api *countingEBS) func() []int32 {
	var mu sync.Mutex
	var indices []int32
	api.blockErr = func(index int32) error {
		mu.Lock()
		defer mu.Unlock()
		indices = append(indices, index)
		return nil
	}
	return func() []int32 {
		mu.Lock()
		defer mu.Unlock()
		return append([]int32(nil), indices...)
	}
}

// waitForCalls waits for prefetches running in the background.
func waitForCalls(t *testing.T, api *countingEBS, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for api.calls.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("GetSnapshotBlock calls = %d, want %d", api.calls.Load(), want)
		}
		time.Sleep(time.Millisecond)
	}
	// Give an unexpected extra prefetch time to show up.
	time.Sleep(50 * time.Millisecond)
	if got := api.calls.Load(); got != want {
		t.Fatalf("GetSnapshotBlock calls = %d, want %d", got, want)
	}
}

func TestReadAt_PrefetchIsOffByDefault(t *testing.T) {
	mock, _ := newPatternMock(16, 4)
	api := &countingEBS{EBSAPI: mock}
	sr, err := Open("snap-test", context.Background(), newMapCache(), api)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := sr.ReadAt(make([]byte, 16), 0); err != nil {
		t.Fatal(err)
	}
	waitForCalls(t, api, 1)
}

func TestReadAt_PrefetchFollowsSequentialReads(t *testing.T) {
	mock, data := newPatternMock(16, 3)
	api := &countingEBS{EBSAPI: mock}
	sr, err := OpenWithOptions("snap-test", context.Background(), newMapCache(), api, OpenOptions{PrefetchBlocks: 2})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := sr.ReadAt(make([]byte, 16), 0); err != nil {
		t.Fatal(err)
	}
	waitForCalls(t, api, 3)

	// Blocks 1 and 2 were prefetched, and there is nothing after block 2.
	buf := make([]byte, 32)
	if _, err := sr.ReadAt(buf, 16); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, data[16:48]) {
		t.Error("ReadAt returned wrong data")
	}
	waitForCalls(t, api, 3)
}

func TestReadAt_PrefetchWithHugePrefetchBlocks(t *testing.T) {
	mock, _ := newPatternMock(16, 3)
	api := &countingEBS{EBSAPI: mock}
	sr, err := OpenWithOptions("snap-test", context.Background(), newMapCache(), api, OpenOptions{PrefetchBlocks: math.MaxInt})
	if err != nil {
		t.Fatal(err)
	}

	// Block 1 is read and block 2, the last one, is prefetched.
	if _, err := sr.ReadAt(make([]byte, 16), 16); err != nil {
		t.Fatal(err)
	}
	waitForCalls(t, api, 2)
}

func TestReadAt_PrefetchDoesNotFollowRandomAccess(t *testing.T) {
	mock, _ := newPatternMock(16, 8)
	api := &countingEBS{EBSAPI: mock}
	sr, err := OpenWithOptions("snap-test", context.Background(), newMapCache(), api, OpenOptions{PrefetchBlocks: 4})
	if err != nil {
		t.Fatal(err)
	}

	// The first read counts as sequential, so start on the last block, after
	// which there is nothing to prefetch. No later read starts where the
	// previous one ended.
	offsets := []int64{112, 0, 64, 32, 96}
	for _, off := range offsets {
		if _, err := sr.ReadAt(make([]byte, 16), off); err != nil {
			t.Fatalf("ReadAt(%d): %v", off, err)
		}
	}
	waitForCalls(t, api, int64(len(offsets)))
}

func TestReadAt_PrefetchRestartsForEarlierSequentialRun(t *testing.T) {
	mock, _ := newPatternMock(16, 6)
	api := &countingEBS{EBSAPI: mock}
	sr, err := OpenWithOptions("snap-test", context.Background(), newMapCache(), api, OpenOptions{PrefetchBlocks: 1})
	if err != nil {
		t.Fatal(err)
	}

	read := func(block int64) {
		t.Helper()
		if _, err := sr.ReadAt(make([]byte, 16), block*16); err != nil {
			t.Fatal(err)
		}
	}
	read(4) // the first read prefetches block 5
	waitForCalls(t, api, 2)
	read(0) // not sequential: no prefetch
	waitForCalls(t, api, 3)
	read(1) // a new sequential run before block 5 prefetches block 2
	waitForCalls(t, api, 5)
}

func TestReadAt_PrefetchSkipsUnallocatedBlocks(t *testing.T) {
	mock, _ := newPatternMock(16, 4)
	mock.Allocated = map[int32]bool{0: true, 2: true, 3: true}
	api := &countingEBS{EBSAPI: mock}
	requested := recordBlocks(api)
	sr, err := OpenWithOptions("snap-test", context.Background(), newMapCache(), api, OpenOptions{PrefetchBlocks: 2})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := sr.ReadAt(make([]byte, 16), 0); err != nil {
		t.Fatal(err)
	}
	waitForCalls(t, api, 2)
	for _, index := range requested() {
		if index == 1 {
			t.Errorf("unallocated block 1 was requested: %v", requested())
		}
	}
}

func TestOpenWithOptions_PrefetchRequiresCache(t *testing.T) {
	mock, _ := newPatternMock(16, 1)
	if _, err := OpenWithOptions("snap-test", context.Background(), nil, mock, OpenOptions{PrefetchBlocks: 1}); err == nil {
		t.Fatal("OpenWithOptions with PrefetchBlocks and no Cache succeeded")
	}
}

func TestReadAt_PrefetchWithConcurrentSequentialReaders(t *testing.T) {
	const blockSize, numBlocks = 16, 8
	mock, data := newPatternMock(blockSize, numBlocks)
	sr, err := OpenWithOptions("snap-test", context.Background(), newMapCache(), mock, OpenOptions{PrefetchBlocks: 2})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for off := start; off < start+len(data)/2; off += 8 {
				buf := make([]byte, 8)
				if _, err := sr.ReadAt(buf, int64(off)); err != nil || !bytes.Equal(buf, data[off:off+8]) {
					t.Errorf("ReadAt(%d): err=%v", off, err)
					return
				}
			}
		}(r * len(data) / 2)
	}
	wg.Wait()
}

// BenchmarkReadAt_Prefetch reads the whole volume into an empty cache: in
// order one block or 4 KiB at a time, or one block at a time in an order where
// no read follows the previous one.
func BenchmarkReadAt_Prefetch(b *testing.B) {
	const blockSize, numBlocks = 512 * 1024, 16
	const volumeSize = blockSize * numBlocks
	for _, bench := range []struct {
		name     string
		readSize int
		offset   func(i int) int
	}{
		{name: "sequential", readSize: blockSize, offset: func(i int) int { return i * blockSize }},
		{name: "sequential-4KiB", readSize: 4 * 1024, offset: func(i int) int { return i * 4 * 1024 }},
		// 5 is coprime with 16, so this visits every block, never next to the previous one.
		{name: "random", readSize: blockSize, offset: func(i int) int { return (i * 5) % numBlocks * blockSize }},
	} {
		for _, prefetch := range []int{0, 4} {
			b.Run(fmt.Sprintf("%s/prefetch=%d", bench.name, prefetch), func(b *testing.B) {
				mock, _ := newPatternMock(blockSize, numBlocks)
				api := &countingEBS{EBSAPI: mock, latency: time.Millisecond}
				buf := make([]byte, bench.readSize)
				for i := 0; i < b.N; i++ {
					sr, err := OpenWithOptions("benchmark-snapshot", context.Background(), newMapCache(), api, OpenOptions{PrefetchBlocks: prefetch})
					if err != nil {
						b.Fatal(err)
					}
					for j := 0; j < volumeSize/bench.readSize; j++ {
						if _, err := sr.ReadAt(buf, int64(bench.offset(j))); err != nil {
							b.Fatal(err)
						}
					}
					// Wait for prefetches so that they do not overlap the next
					// iteration. Every block is requested once per iteration.
					for api.calls.Load() < int64(numBlocks*(i+1)) || api.inFlight.Load() > 0 {
						time.Sleep(10 * time.Microsecond)
					}
				}
				b.ReportMetric(float64(api.calls.Load())/float64(b.N), "requests/op")
			})
		}
	}
}
