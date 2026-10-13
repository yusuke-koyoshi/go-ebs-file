package ebsfile

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ebs"
	"github.com/aws/aws-sdk-go-v2/service/ebs/types"
	"golang.org/x/sync/singleflight"
)

var _ EBSAPI = (*MockEBS)(nil)

type MockEBS struct {
	EBSAPI

	Reader     io.ReaderAt
	BlockSize  *int32
	NextToken  *string
	VolumeSize *int64         // volume size in bytes (unlike the real EBS API which returns GiB)
	Allocated  map[int32]bool // nil means all blocks are allocated
	PageSize   int            // 0 means all blocks in a single page (no pagination)
}

func (m *MockEBS) ListSnapshotBlocks(_ context.Context, params *ebs.ListSnapshotBlocksInput, _ ...func(*ebs.Options)) (*ebs.ListSnapshotBlocksOutput, error) {
	numBlocks := int32(*m.VolumeSize / int64(*m.BlockSize))

	startIndex := int32(0)
	if params.NextToken != nil {
		if _, err := fmt.Sscanf(*params.NextToken, "token-%d", &startIndex); err != nil {
			return nil, fmt.Errorf("invalid next token %q: %w", *params.NextToken, err)
		}
	}

	pageSize := m.PageSize
	if pageSize <= 0 {
		pageSize = int(numBlocks)
	}

	var blocks []types.Block
	count := 0
	nextStart := int32(-1)
	for i := startIndex; i < numBlocks; i++ {
		if m.Allocated != nil && !m.Allocated[i] {
			continue
		}
		if count >= pageSize {
			nextStart = i
			break
		}
		blocks = append(blocks, types.Block{
			BlockIndex: aws.Int32(i),
			BlockToken: aws.String(fmt.Sprintf("block-token-%d", i)),
		})
		count++
	}

	output := &ebs.ListSnapshotBlocksOutput{
		Blocks:     blocks,
		BlockSize:  m.BlockSize, // 512 KB
		VolumeSize: m.VolumeSize,
	}
	if nextStart >= 0 {
		output.NextToken = aws.String(fmt.Sprintf("token-%d", nextStart))
	}
	return output, nil
}

func (m *MockEBS) WalkSnapshotBlocks(ctx context.Context, input *ebs.ListSnapshotBlocksInput, table map[int32]string) (*ebs.ListSnapshotBlocksOutput, map[int32]string, error) {
	output, err := m.ListSnapshotBlocks(ctx, input)
	if err != nil {
		return nil, nil, err
	}
	for _, block := range output.Blocks {
		table[*block.BlockIndex] = *block.BlockToken
	}
	if output.NextToken != nil {
		nextInput := *input
		nextInput.NextToken = output.NextToken
		return m.WalkSnapshotBlocks(ctx, &nextInput, table)
	}
	return output, table, nil
}

func (m *MockEBS) GetSnapshotBlock(_ context.Context, params *ebs.GetSnapshotBlockInput, _ ...func(*ebs.Options)) (*ebs.GetSnapshotBlockOutput, error) {
	buf := make([]byte, *m.BlockSize)

	offset := int64(*params.BlockIndex) * int64(*m.BlockSize)
	n, err := m.Reader.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	sum := sha256.Sum256(buf)
	return &ebs.GetSnapshotBlockOutput{
		BlockData:         io.NopCloser(bytes.NewReader(buf)),
		DataLength:        aws.Int32(int32(n)),
		Checksum:          aws.String(base64.StdEncoding.EncodeToString(sum[:])),
		ChecksumAlgorithm: types.ChecksumAlgorithmChecksumAlgorithmSha256,
	}, nil
}

func NewMockEBS(filePath string, blockSize int32, volumeSize int64) EBSAPI {
	f, err := os.Open(filePath)
	if err != nil {
		return nil
	}
	return &MockEBS{
		Reader:     f,
		BlockSize:  aws.Int32(blockSize),
		VolumeSize: aws.Int64(volumeSize),
	}
}

var _ EBSAPI = (*EBS)(nil)

type EBS struct {
	*ebs.Client
}

type EBSAPI interface {
	ListSnapshotBlocks(ctx context.Context, params *ebs.ListSnapshotBlocksInput, optFns ...func(*ebs.Options)) (*ebs.ListSnapshotBlocksOutput, error)
	GetSnapshotBlock(ctx context.Context, params *ebs.GetSnapshotBlockInput, optFns ...func(*ebs.Options)) (*ebs.GetSnapshotBlockOutput, error)
	WalkSnapshotBlocks(ctx context.Context, input *ebs.ListSnapshotBlocksInput, table map[int32]string) (*ebs.ListSnapshotBlocksOutput, map[int32]string, error)
}

type File struct {
	snapshotID string
	size       int64
	blockSize  int32
	blockTable map[int32]string

	ebsclient EBSAPI
	cache     Cache[string, []byte]
	ctx       context.Context
	group     singleflight.Group
	fetchSem  chan struct{} // nil means unlimited

	allowMissingChecksum bool
	fetchAttempts        int

	prefetchBlocks int
	prefetchMu     sync.Mutex
	lastReadEnd    int64 // -1 until the first successful read
	// prefetchedUpTo is the last block prefetched in the current sequential
	// run, so that small reads within a block do not prefetch the same blocks
	// again while they are in flight. -1 when no run is in progress.
	prefetchedUpTo int64
}

// cacheKey includes the snapshot ID so that readers of different snapshots
// can share one Cache.
func (f *File) cacheKey(index int32) string {
	return "ebs:" + f.snapshotID + ":" + strconv.Itoa(int(index))
}

// DefaultBlockFetchAttempts is the number of times a block is fetched when its
// body cannot be read or verified, unless OpenOptions.BlockFetchAttempts is set.
const DefaultBlockFetchAttempts = 2

// OpenOptions configures OpenWithOptions. The zero value gives the behavior of
// Open.
type OpenOptions struct {
	// AllowMissingChecksum accepts GetSnapshotBlock responses without a
	// checksum. By default such a response is an error. A checksum that does
	// not match the block data is an error regardless of this option. Blocks
	// accepted without a checksum are cached like verified ones, so do not
	// share the Cache with a reader that does not set this option.
	AllowMissingChecksum bool

	// BlockFetchAttempts is the maximum number of GetSnapshotBlock calls for a
	// block whose body fails to read, has the wrong length, or does not match
	// its checksum. 1 disables refetching, and a non-positive value uses
	// DefaultBlockFetchAttempts. Errors returned by GetSnapshotBlock itself are
	// not refetched, because the AWS SDK has already retried them.
	BlockFetchAttempts int

	// MaxConcurrentBlocks limits the number of blocks this reader fetches, and
	// so holds outside the Cache, at once. A non-positive value leaves it
	// unlimited, so a ReadAt spanning n uncached blocks issues n
	// GetSnapshotBlock requests at once.
	MaxConcurrentBlocks int

	// PrefetchBlocks fetches this many blocks in the background after a read
	// that starts where the previous successful read ended, or after the first
	// read. It requires a Cache. Keep it small relative to the Cache: each
	// prefetched block holds a goroutine until it is fetched and may be evicted
	// before it is read, and a sequential read that stops soon after wastes up
	// to this many requests. Prefetches count toward MaxConcurrentBlocks, and
	// a failed one is dropped. A non-positive value, the default, disables it.
	PrefetchBlocks int
}

var (
	errMissingChecksum  = errors.New("response has no checksum; set OpenOptions.AllowMissingChecksum to accept it")
	errChecksumMismatch = errors.New("checksum mismatch")
)

// NewFromConfig creates a new EBS from an existing aws.Config.
func NewFromConfig(cfg aws.Config, optFns ...func(*ebs.Options)) *EBS {
	return &EBS{ebs.NewFromConfig(cfg, optFns...)}
}

func New(ctx context.Context, optFns ...func(*config.LoadOptions) error) (*EBS, error) {
	cfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, err
	}

	ebsClient := ebs.NewFromConfig(cfg)
	return &EBS{ebsClient}, nil
}

func (e EBS) WalkSnapshotBlocks(ctx context.Context, input *ebs.ListSnapshotBlocksInput, table map[int32]string) (*ebs.ListSnapshotBlocksOutput, map[int32]string, error) {
	output, err := e.ListSnapshotBlocks(ctx, input)
	if err != nil {
		return nil, nil, err
	}
	for _, block := range output.Blocks {
		table[*block.BlockIndex] = *block.BlockToken
	}
	if output.NextToken != nil {
		input.NextToken = output.NextToken
		return e.WalkSnapshotBlocks(ctx, input, table)
	}
	output.VolumeSize = aws.Int64(*output.VolumeSize << 30)
	return output, table, nil
}

func Open(snapID string, ctx context.Context, cache Cache[string, []byte], e EBSAPI) (*io.SectionReader, error) {
	return OpenWithOptions(snapID, ctx, cache, e, OpenOptions{})
}

// OpenWithOptions is Open with options. Each block read from the snapshot is
// verified against the SHA256 checksum returned by GetSnapshotBlock before it
// is cached.
func OpenWithOptions(snapID string, ctx context.Context, cache Cache[string, []byte], e EBSAPI, options OpenOptions) (*io.SectionReader, error) {
	if options.BlockFetchAttempts <= 0 {
		options.BlockFetchAttempts = DefaultBlockFetchAttempts
	}
	if options.PrefetchBlocks > 0 && cache == nil {
		return nil, errors.New("ebsfile: OpenOptions.PrefetchBlocks requires a Cache")
	}
	input := &ebs.ListSnapshotBlocksInput{
		SnapshotId: &snapID,
	}
	output, table, err := e.WalkSnapshotBlocks(ctx, input, make(map[int32]string))
	if err != nil {
		return nil, err
	}
	if cache == nil {
		cache = &mockCache[string, []byte]{}
	}

	f := &File{
		size:       *output.VolumeSize,
		snapshotID: snapID,
		blockSize:  *output.BlockSize,
		blockTable: table,
		cache:      cache,
		ebsclient:  e,
		ctx:        ctx,

		allowMissingChecksum: options.AllowMissingChecksum,
		fetchAttempts:        options.BlockFetchAttempts,
	}
	if options.MaxConcurrentBlocks > 0 {
		f.fetchSem = make(chan struct{}, options.MaxConcurrentBlocks)
	}
	if options.PrefetchBlocks > 0 {
		f.prefetchBlocks = options.PrefetchBlocks
	}
	f.lastReadEnd, f.prefetchedUpTo = -1, -1
	return io.NewSectionReader(f, 0, f.Size()), nil
}

// ReadAt implements io.ReaderAt and is safe for concurrent use. The uncached
// blocks in [off, off+len(p)) are fetched concurrently, and concurrent reads
// of the same uncached block share a single GetSnapshotBlock request.
func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, fmt.Errorf("ebsfile: negative offset %d", off)
	}
	if off >= f.size {
		return 0, io.EOF
	}
	end := off + int64(len(p))
	if end > f.size {
		end = f.size
	}
	if end == off {
		return 0, nil
	}

	blockSize := int64(f.blockSize)
	first, last := int32(off/blockSize), int32((end-1)/blockSize)
	errs := f.readBlocks(p[:end-off], off, first, last)

	for i, err := range errs {
		if err != nil {
			// Report only the bytes before the failing block; later blocks
			// may have been copied, but p is scratch space on error.
			if blockStart := int64(first+int32(i)) * blockSize; blockStart > off {
				n = int(blockStart - off)
			}
			return n, err
		}
	}

	n = int(end - off)
	if n < len(p) {
		return n, io.EOF
	}
	if f.prefetchBlocks > 0 {
		f.prefetch(off, end, last)
	}
	return n, nil
}

// prefetch fetches the blocks after last in the background if the read
// [off, end) starts where the previous successful read ended. Concurrent
// readers make this miss sequential reads, which costs prefetches but never
// returns wrong data.
func (f *File) prefetch(off, end int64, last int32) {
	from := int64(last) + 1
	// Clamp before adding so that a huge PrefetchBlocks does not overflow.
	limit := (f.size - 1) / int64(f.blockSize)
	if n := int64(f.prefetchBlocks); n < limit-int64(last) {
		limit = int64(last) + n
	}

	f.prefetchMu.Lock()
	sequential := f.lastReadEnd == off || f.lastReadEnd == -1
	f.lastReadEnd = end
	if !sequential {
		f.prefetchedUpTo = -1
	} else if f.prefetchedUpTo >= from {
		from = f.prefetchedUpTo + 1
	}
	if sequential && from <= limit {
		f.prefetchedUpTo = limit
	}
	f.prefetchMu.Unlock()

	if !sequential || from > limit || f.ctx.Err() != nil {
		return
	}
	for i := from; i <= limit; i++ {
		index := int32(i)
		token, ok := f.blockTable[index]
		if !ok {
			continue
		}
		// fetchShared skips cached blocks, so the Cache is not consulted on
		// the caller's path.
		go func() {
			_, _ = f.fetchShared(index, f.cacheKey(index), token)
		}()
	}
}

// readBlocks copies the blocks first to last into p, which holds the bytes
// from off, and returns the error of each block. Fetched blocks are copied as
// soon as they arrive, so only the blocks in flight are held at once.
func (f *File) readBlocks(p []byte, off int64, first, last int32) []error {
	errs := make([]error, last-first+1)

	var missing []int
	var lastKey string
	for i := range errs {
		index := first + int32(i)
		if _, ok := f.blockTable[index]; !ok {
			// Blocks not in blockTable are unallocated in the EBS snapshot,
			// which represent zero-filled disk regions.
			dst, _ := f.blockSpan(p, off, index)
			for j := range dst {
				dst[j] = 0
			}
			continue
		}
		key := f.cacheKey(index)
		if buf, ok := f.cache.Get(key); ok {
			errs[i] = f.copyBlock(p, off, index, buf)
			continue
		}
		missing = append(missing, i)
		lastKey = key
	}
	if len(missing) == 1 {
		// Most reads fall within one block; skip the goroutine for them.
		i := missing[0]
		errs[i] = f.fetchInto(p, off, first+int32(i), lastKey)
		return errs
	}

	var wg sync.WaitGroup
	for _, i := range missing {
		wg.Add(1)
		go func(i int, index int32) {
			defer wg.Done()
			errs[i] = f.fetchInto(p, off, index, f.cacheKey(index))
		}(i, first+int32(i))
	}
	wg.Wait()
	return errs
}

func (f *File) fetchInto(p []byte, off int64, index int32, key string) error {
	buf, err := f.fetchShared(index, key, f.blockTable[index])
	if err != nil {
		return err
	}
	return f.copyBlock(p, off, index, buf)
}

func (f *File) copyBlock(p []byte, off int64, index int32, buf []byte) error {
	if len(buf) != int(f.blockSize) {
		return fmt.Errorf("invalid block size: len(buf): %d != f.blockSize: %d", len(buf), f.blockSize)
	}
	dst, from := f.blockSpan(p, off, index)
	copy(dst, buf[from:])
	return nil
}

// blockSpan returns the part of p, which holds the bytes from off, that block
// index covers, and where that part starts within the block.
func (f *File) blockSpan(p []byte, off int64, index int32) ([]byte, int64) {
	blockStart := int64(index) * int64(f.blockSize)
	start, stop := blockStart, blockStart+int64(f.blockSize)
	if start < off {
		start = off
	}
	if end := off + int64(len(p)); stop > end {
		stop = end
	}
	return p[start-off : stop-off], start - blockStart
}

// fetchShared fetches the block at index and caches it under key, coalescing
// concurrent fetches of it. A block already in the cache is returned from it.
func (f *File) fetchShared(index int32, key, token string) ([]byte, error) {
	v, err, _ := f.group.Do(key, func() (any, error) {
		// Prefetches do not look up the cache first, and another fetch may
		// have cached the block since a reader's Get.
		if buf, ok := f.cache.Get(key); ok {
			return buf, nil
		}
		// Acquire inside Do so that callers waiting on this fetch do not
		// hold a slot.
		if f.fetchSem != nil {
			select {
			case f.fetchSem <- struct{}{}:
				defer func() { <-f.fetchSem }()
			case <-f.ctx.Done():
				return nil, fmt.Errorf("ebsfile: block %d: %w", index, f.ctx.Err())
			}
		}
		buf, err := f.read(index, token)
		if err != nil {
			return nil, err
		}
		f.cache.Add(key, buf)
		return buf, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

func (f *File) Size() int64 {
	return f.size
}

func (f *File) read(index int32, token string) ([]byte, error) {
	for attempt := 1; ; attempt++ {
		buf, retryable, err := f.fetchBlock(index, token)
		if err == nil {
			return buf, nil
		}
		if !retryable || attempt >= f.fetchAttempts || f.ctx.Err() != nil {
			if attempt > 1 {
				return nil, fmt.Errorf("ebsfile: block %d after %d attempts: %w", index, attempt, err)
			}
			return nil, fmt.Errorf("ebsfile: block %d: %w", index, err)
		}
	}
}

// fetchBlock fetches and verifies one block. It reports whether the failure
// may go away on another fetch. A missing checksum or an unsupported algorithm
// will not, so they are not retryable.
func (f *File) fetchBlock(index int32, token string) (buf []byte, retryable bool, err error) {
	input := &ebs.GetSnapshotBlockInput{
		SnapshotId: &f.snapshotID,
		BlockIndex: &index,
		BlockToken: &token,
	}
	o, err := f.ebsclient.GetSnapshotBlock(f.ctx, input)
	if err != nil {
		return nil, false, err
	}
	defer o.BlockData.Close()

	buf, err = io.ReadAll(o.BlockData)
	if err != nil {
		return nil, true, err
	}
	if len(buf) != int(f.blockSize) {
		return nil, true, fmt.Errorf("invalid block size: got %d bytes, want %d", len(buf), f.blockSize)
	}

	checksum := aws.ToString(o.Checksum)
	if checksum == "" {
		if f.allowMissingChecksum {
			return buf, false, nil
		}
		return nil, false, errMissingChecksum
	}
	if o.ChecksumAlgorithm != types.ChecksumAlgorithmChecksumAlgorithmSha256 {
		return nil, false, fmt.Errorf("unsupported checksum algorithm %q", o.ChecksumAlgorithm)
	}
	sum := sha256.Sum256(buf)
	if base64.StdEncoding.EncodeToString(sum[:]) != checksum {
		return nil, true, errChecksumMismatch
	}
	return buf, false, nil
}
