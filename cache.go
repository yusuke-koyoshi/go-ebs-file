package ebsfile

var (
	_ Cache[string, []byte] = &mockCache[string, []byte]{}
)

// Cache stores the blocks read from a snapshot. Its methods must be safe for
// concurrent use, because a single ReadAt spanning several blocks calls them
// from several goroutines.
type Cache[K string, V any] interface {
	// Add cache data
	Add(key K, value V) bool

	// Get returns key's value from the cache
	Get(key K) (value V, ok bool)
}

type mockCache[K string, V []byte] struct{}

func (c *mockCache[K, V]) Add(_ K, _ V) bool {
	return false
}

func (c *mockCache[K, V]) Get(_ K) (v V, evicted bool) {
	return
}
