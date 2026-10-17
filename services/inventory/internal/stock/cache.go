package stock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

// Catalog is the read side of the inventory: what the gRPC API serves.
// *Store and *CachedCatalog both implement it.
type Catalog interface {
	GetProduct(ctx context.Context, id string) (*Product, error)
	ListProducts(ctx context.Context) ([]Product, error)
}

// Cache is the small slice of a key-value store that CachedCatalog needs.
// RedisCache is the real one; tests use an in-memory fake.
type Cache interface {
	// Get returns the value for key, and false if the key does not exist.
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Del(ctx context.Context, keys ...string) error
}

// Cache keys. Values are JSON.
const keyAllProducts = "products:all"

func productKey(id string) string { return "product:" + id }

// CachedCatalog serves catalog reads cache-aside: it reads the cache first,
// and on a miss reads the database and stores the result with a TTL.
//
// The cache is for display reads only. Reserve never reads it: the
// authoritative stock check is the SELECT ... FOR UPDATE inside the reserve
// transaction. A stale cached count can therefore show a customer the wrong
// number for a few seconds, but it can never cause an oversell.
//
// Staleness is bounded two ways. After every committed stock change, the
// changed products' keys and the list key are deleted (Invalidate). And
// every key has a TTL, which covers the cases deletion cannot: a delete that
// failed because the cache was down, and the cache-aside race where a reader
// that missed reads the old row, the writer commits and deletes, and only
// then the reader stores the old row. In those cases a value is stale for at
// most one TTL.
//
// If the cache fails, CachedCatalog logs and counts the error and reads the
// database instead, so a cache outage costs latency, never availability. To
// keep that latency to one slow call rather than one per request, a failure
// also starts a bypass: for BypassFor, reads skip the cache entirely and go
// straight to Postgres, and invalidations are skipped (a Redis that comes
// back holding old entries serves them for at most their TTL). The first
// call after the bypass ends tries the cache again.
type CachedCatalog struct {
	db     Catalog
	cache  Cache
	ttl    time.Duration
	logger *slog.Logger

	// BypassFor is how long the cache is skipped after a failure.
	BypassFor time.Duration
	// now is the clock, replaceable in tests.
	now func() time.Time
	// bypassUntil holds UnixNano; reads and writes are atomic because
	// gRPC handlers and the consumer call the catalog concurrently.
	bypassUntil atomic.Int64

	hits, misses, errs prometheus.Counter
}

// DefaultBypass is the default CachedCatalog.BypassFor.
const DefaultBypass = 5 * time.Second

// errBypassed means the cache was skipped because it failed recently.
var errBypassed = errors.New("cache bypassed after a recent failure")

// NewCachedCatalog returns a Catalog that caches db's reads in cache for
// ttl. Its hit, miss, and error counters are registered with reg.
func NewCachedCatalog(db Catalog, cache Cache, ttl time.Duration, reg prometheus.Registerer, logger *slog.Logger) *CachedCatalog {
	c := &CachedCatalog{
		db:        db,
		cache:     cache,
		ttl:       ttl,
		logger:    logger.With("component", "cache"),
		BypassFor: DefaultBypass,
		now:       time.Now,
		hits: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "inventory_cache_hits_total",
			Help: "Catalog reads answered from the cache.",
		}),
		misses: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "inventory_cache_misses_total",
			Help: "Catalog reads not in the cache, answered from Postgres and then cached.",
		}),
		errs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "inventory_cache_errors_total",
			Help: "Cache operations (get, set, delete) that failed; reads fell back to Postgres.",
		}),
	}
	reg.MustRegister(c.hits, c.misses, c.errs)
	return c
}

// GetProduct returns one product, from the cache when it can.
func (c *CachedCatalog) GetProduct(ctx context.Context, id string) (*Product, error) {
	var p Product
	cached, err := c.lookup(ctx, productKey(id), &p)
	if cached {
		return &p, nil
	}
	fromDB, dbErr := c.db.GetProduct(ctx, id)
	if dbErr != nil {
		return nil, dbErr // ErrNotFound is not cached
	}
	if err == nil {
		c.store(ctx, productKey(id), fromDB)
	}
	return fromDB, nil
}

// ListProducts returns the whole catalog, from the cache when it can.
func (c *CachedCatalog) ListProducts(ctx context.Context) ([]Product, error) {
	var ps []Product
	cached, err := c.lookup(ctx, keyAllProducts, &ps)
	if cached {
		return ps, nil
	}
	fromDB, dbErr := c.db.ListProducts(ctx)
	if dbErr != nil {
		return nil, dbErr
	}
	if err == nil {
		c.store(ctx, keyAllProducts, fromDB)
	}
	return fromDB, nil
}

// Invalidate deletes the cached entries for productIDs and the cached list.
// Store calls it after a transaction that changed their stock commits.
// Deleting (rather than writing the new values) means a concurrent reader
// can never overwrite a newer value with an older one through this path;
// the next read simply misses and loads the committed row.
func (c *CachedCatalog) Invalidate(ctx context.Context, productIDs []string) {
	if c.bypassed() {
		return
	}
	keys := make([]string, 0, len(productIDs)+1)
	for _, id := range productIDs {
		keys = append(keys, productKey(id))
	}
	keys = append(keys, keyAllProducts)
	if err := c.cache.Del(ctx, keys...); err != nil {
		c.failed()
		c.logger.Warn("cache invalidation failed; entries stay stale until their TTL",
			"keys", keys, "ttl", c.ttl.String(), "error", err)
	}
}

// lookup reads key into v. It returns true on a hit. A non-nil error means
// the cache failed (it has been logged and counted) or is being bypassed, so
// the caller should not try to write to it either.
func (c *CachedCatalog) lookup(ctx context.Context, key string, v any) (bool, error) {
	if c.bypassed() {
		return false, errBypassed
	}
	raw, ok, err := c.cache.Get(ctx, key)
	if err == nil && ok {
		err = json.Unmarshal(raw, v)
		if err == nil {
			c.hits.Inc()
			return true, nil
		}
		err = fmt.Errorf("decode cached %s: %w", key, err)
	}
	if err != nil {
		c.failed()
		c.logger.Warn("cache read failed; reading Postgres", "key", key,
			"bypass", c.BypassFor.String(), "error", err)
		return false, err
	}
	c.misses.Inc()
	return false, nil
}

func (c *CachedCatalog) store(ctx context.Context, key string, v any) {
	raw, err := json.Marshal(v)
	if err == nil {
		err = c.cache.Set(ctx, key, raw, c.ttl)
	}
	if err != nil {
		c.failed()
		c.logger.Warn("cache write failed", "key", key, "error", err)
	}
}

// failed counts a cache failure and starts a bypass.
func (c *CachedCatalog) failed() {
	c.errs.Inc()
	c.bypassUntil.Store(c.now().Add(c.BypassFor).UnixNano())
}

func (c *CachedCatalog) bypassed() bool {
	return c.now().UnixNano() < c.bypassUntil.Load()
}

// RedisCache adapts a go-redis client to Cache.
type RedisCache struct {
	rdb     *redis.Client
	timeout time.Duration
}

// NewRedisCache returns a Cache backed by rdb. Every call is bounded by
// timeout through its context, which also bounds the DNS lookup and dial
// when Redis is unreachable (the client's own dial timeout does not cover a
// slow "no such host" answer).
func NewRedisCache(rdb *redis.Client, timeout time.Duration) *RedisCache {
	return &RedisCache{rdb: rdb, timeout: timeout}
}

// Get returns the value for key. redis.Nil, go-redis's "no such key" error,
// is a miss, not a failure.
func (r *RedisCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	b, err := r.rdb.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// Set stores value under key with a TTL.
func (r *RedisCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	return r.rdb.Set(ctx, key, value, ttl).Err()
}

// Del deletes keys in one command.
func (r *RedisCache) Del(ctx context.Context, keys ...string) error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	return r.rdb.Del(ctx, keys...).Err()
}
