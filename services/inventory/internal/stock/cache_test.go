package stock

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// fakeDB is a Catalog that counts how often it is read.
type fakeDB struct {
	products map[string]Product
	reads    int
}

func (f *fakeDB) GetProduct(_ context.Context, id string) (*Product, error) {
	f.reads++
	p, ok := f.products[id]
	if !ok {
		return nil, ErrNotFound
	}
	return &p, nil
}

func (f *fakeDB) ListProducts(context.Context) ([]Product, error) {
	f.reads++
	out := make([]Product, 0, len(f.products))
	for _, p := range f.products {
		out = append(out, p)
	}
	slices.SortFunc(out, func(a, b Product) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

// fakeCache is an in-memory Cache. Setting err makes every call fail, like
// a Redis that is down.
type fakeCache struct {
	values map[string][]byte
	ttls   map[string]time.Duration
	calls  int // every Get, Set, and Del, failed or not
	sets   int
	err    error
}

func newFakeCache() *fakeCache {
	return &fakeCache{values: map[string][]byte{}, ttls: map[string]time.Duration{}}
}

func (f *fakeCache) Get(_ context.Context, key string) ([]byte, bool, error) {
	f.calls++
	if f.err != nil {
		return nil, false, f.err
	}
	v, ok := f.values[key]
	return v, ok, nil
}

func (f *fakeCache) Set(_ context.Context, key string, value []byte, ttl time.Duration) error {
	f.calls++
	if f.err != nil {
		return f.err
	}
	f.sets++
	f.values[key], f.ttls[key] = value, ttl
	return nil
}

func (f *fakeCache) Del(_ context.Context, keys ...string) error {
	f.calls++
	if f.err != nil {
		return f.err
	}
	for _, k := range keys {
		delete(f.values, k)
	}
	return nil
}

type cacheHarness struct {
	db    *fakeDB
	cache *fakeCache
	cat   *CachedCatalog
}

const testTTL = 30 * time.Second

func newCacheHarness() *cacheHarness {
	db := &fakeDB{products: map[string]Product{
		"sku-cable": {ID: "sku-cable", Name: "USB-C cable", UnitPriceCents: 999, Currency: "USD", Available: 10},
		"sku-mouse": {ID: "sku-mouse", Name: "Wireless mouse", UnitPriceCents: 2999, Currency: "USD", Available: 5},
	}}
	cache := newFakeCache()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return &cacheHarness{
		db:    db,
		cache: cache,
		cat:   NewCachedCatalog(db, cache, testTTL, prometheus.NewRegistry(), logger),
	}
}

// counts returns the hit, miss, and error counters.
func (h *cacheHarness) counts() [3]float64 {
	return [3]float64{sumKinds(h.cat.hits), sumKinds(h.cat.misses), testutil.ToFloat64(h.cat.errs)}
}

// sumKinds adds a counter's product and list series.
func sumKinds(v *prometheus.CounterVec) float64 {
	return testutil.ToFloat64(v.WithLabelValues(kindProduct)) + testutil.ToFloat64(v.WithLabelValues(kindList))
}

func TestGetProductMissThenHit(t *testing.T) {
	h := newCacheHarness()
	ctx := context.Background()

	for i := range 3 {
		p, err := h.cat.GetProduct(ctx, "sku-cable")
		if err != nil || p.Available != 10 || p.Name != "USB-C cable" {
			t.Fatalf("read %d: GetProduct = %+v, %v", i, p, err)
		}
	}
	if h.db.reads != 1 {
		t.Errorf("database reads = %d, want 1 (then served from cache)", h.db.reads)
	}
	if ttl := h.cache.ttls["product:sku-cable"]; ttl != testTTL {
		t.Errorf("cached with TTL %v, want %v", ttl, testTTL)
	}
	if got := h.counts(); got != [3]float64{2, 1, 0} {
		t.Errorf("hits, misses, errors = %v, want [2 1 0]", got)
	}
}

func TestListProductsMissThenHit(t *testing.T) {
	h := newCacheHarness()
	ctx := context.Background()

	for range 2 {
		ps, err := h.cat.ListProducts(ctx)
		if err != nil || len(ps) != 2 || ps[0].ID != "sku-cable" {
			t.Fatalf("ListProducts = %+v, %v", ps, err)
		}
	}
	if h.db.reads != 1 {
		t.Errorf("database reads = %d, want 1", h.db.reads)
	}
	if _, ok := h.cache.values[keyAllProducts]; !ok {
		t.Errorf("%s not cached", keyAllProducts)
	}
	// List reads are counted under key="list", not "product".
	if hit, miss := testutil.ToFloat64(h.cat.hits.WithLabelValues(kindList)), testutil.ToFloat64(h.cat.misses.WithLabelValues(kindList)); hit != 1 || miss != 1 {
		t.Errorf("list hits, misses = %v, %v; want 1, 1", hit, miss)
	}
	if n := sumKinds(h.cat.hits) + sumKinds(h.cat.misses); n != 2 {
		t.Errorf("all hits and misses = %v, want 2 (none under product)", n)
	}
}

// Invalidate deletes rather than rewrites: the next read misses and loads
// the committed value.
func TestInvalidateMakesNextReadFresh(t *testing.T) {
	h := newCacheHarness()
	ctx := context.Background()
	if _, err := h.cat.GetProduct(ctx, "sku-cable"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.cat.ListProducts(ctx); err != nil {
		t.Fatal(err)
	}

	// A reservation commits: the database changes, then Store calls Invalidate.
	p := h.db.products["sku-cable"]
	p.Available = 7
	h.db.products["sku-cable"] = p
	h.cat.Invalidate(ctx, []string{"sku-cable"})

	if _, ok := h.cache.values["product:sku-cable"]; ok {
		t.Error("product:sku-cable still cached after Invalidate")
	}
	if _, ok := h.cache.values[keyAllProducts]; ok {
		t.Errorf("%s still cached after Invalidate", keyAllProducts)
	}
	got, err := h.cat.GetProduct(ctx, "sku-cable")
	if err != nil || got.Available != 7 {
		t.Errorf("GetProduct after invalidate = %+v, %v; want available 7", got, err)
	}
	list, err := h.cat.ListProducts(ctx)
	if err != nil || list[0].Available != 7 {
		t.Errorf("ListProducts after invalidate = %+v, %v; want sku-cable available 7", list, err)
	}
}

// With the cache down, every read still succeeds from the database. The
// first failure is counted and starts a bypass, so the following calls do
// not touch the cache at all (no write after a failed read, no delete).
func TestCacheDownFallsBackToDatabase(t *testing.T) {
	h := newCacheHarness()
	h.cache.err = errors.New("dial tcp redis:6379: connection refused")
	ctx := context.Background()

	p, err := h.cat.GetProduct(ctx, "sku-mouse")
	if err != nil || p.Available != 5 {
		t.Fatalf("GetProduct = %+v, %v; want the database row", p, err)
	}
	ps, err := h.cat.ListProducts(ctx)
	if err != nil || len(ps) != 2 {
		t.Fatalf("ListProducts = %+v, %v; want the database rows", ps, err)
	}
	h.cat.Invalidate(ctx, []string{"sku-mouse"})

	if h.db.reads != 2 {
		t.Errorf("database reads = %d, want 2", h.db.reads)
	}
	if h.cache.calls != 1 {
		t.Errorf("cache calls = %d, want 1 (the rest bypassed)", h.cache.calls)
	}
	if got := h.counts(); got != [3]float64{0, 0, 1} {
		t.Errorf("hits, misses, errors = %v, want [0 0 1]", got)
	}
}

// After the bypass window the cache is tried again, and once it answers,
// reads are cached as usual.
func TestBypassEndsAndCacheRecovers(t *testing.T) {
	h := newCacheHarness()
	clock := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	h.cat.now = func() time.Time { return clock }
	ctx := context.Background()

	h.cache.err = errors.New("i/o timeout")
	if _, err := h.cat.GetProduct(ctx, "sku-cable"); err != nil {
		t.Fatal(err)
	}
	h.cache.err = nil // Redis is back, but the bypass is still running

	clock = clock.Add(DefaultBypass - time.Millisecond)
	if _, err := h.cat.GetProduct(ctx, "sku-cable"); err != nil {
		t.Fatal(err)
	}
	if h.cache.calls != 1 {
		t.Errorf("cache calls during the bypass = %d, want 1", h.cache.calls)
	}

	clock = clock.Add(time.Millisecond)
	for range 2 {
		if _, err := h.cat.GetProduct(ctx, "sku-cable"); err != nil {
			t.Fatal(err)
		}
	}
	// Miss and set, then a hit: the cache is in use again.
	if got := h.counts(); got != [3]float64{1, 1, 1} {
		t.Errorf("hits, misses, errors = %v, want [1 1 1]", got)
	}
	if h.db.reads != 3 {
		t.Errorf("database reads = %d, want 3", h.db.reads)
	}
}

func TestNotFoundIsNotCached(t *testing.T) {
	h := newCacheHarness()
	ctx := context.Background()
	for range 2 {
		if _, err := h.cat.GetProduct(ctx, "sku-missing"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("GetProduct = %v, want ErrNotFound", err)
		}
	}
	if h.db.reads != 2 || h.cache.sets != 0 {
		t.Errorf("database reads %d, cache sets %d; want 2 and 0", h.db.reads, h.cache.sets)
	}
}

func TestCorruptEntryFallsBackToDatabase(t *testing.T) {
	h := newCacheHarness()
	h.cache.values["product:sku-cable"] = []byte("{not json")

	p, err := h.cat.GetProduct(context.Background(), "sku-cable")
	if err != nil || p.Available != 10 {
		t.Fatalf("GetProduct = %+v, %v; want the database row", p, err)
	}
	if got := h.counts(); got[2] != 1 {
		t.Errorf("errors = %v, want 1", got[2])
	}
}
