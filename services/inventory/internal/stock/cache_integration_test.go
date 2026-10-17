//go:build integration

package stock

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"

	"github.com/omnomkar/event-driven-orders/pkg/health"
	"github.com/omnomkar/event-driven-orders/pkg/testinfra"
	"github.com/omnomkar/event-driven-orders/services/inventory/migrations"
)

// cachedStack is a Store on real Postgres with a CachedCatalog on real
// Redis, wired the way cmd/inventory wires them.
type cachedStack struct {
	pool    *pgxpool.Pool
	store   *Store
	catalog *CachedCatalog
	rdb     *redis.Client
}

func newCachedStack(t *testing.T, redisAddr string) *cachedStack {
	t.Helper()
	pool := testinfra.Postgres(t, migrations.FS)
	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		DialTimeout:  250 * time.Millisecond,
		ReadTimeout:  250 * time.Millisecond,
		WriteTimeout: 250 * time.Millisecond,
		MaxRetries:   -1,
	})
	t.Cleanup(func() { _ = rdb.Close() })
	store := NewStore(pool)
	catalog := NewCachedCatalog(store, NewRedisCache(rdb, 250*time.Millisecond), time.Minute, prometheus.NewRegistry(), testinfra.Logger())
	store.OnStockChange(catalog.Invalidate)
	return &cachedStack{pool: pool, store: store, catalog: catalog, rdb: rdb}
}

func (s *cachedStack) cached(t *testing.T, key string) bool {
	t.Helper()
	n, err := s.rdb.Exists(context.Background(), key).Result()
	if err != nil {
		t.Fatalf("EXISTS %s: %v", key, err)
	}
	return n == 1
}

func (s *cachedStack) listed(t *testing.T, productID string) int32 {
	t.Helper()
	ps, err := s.catalog.ListProducts(context.Background())
	if err != nil {
		t.Fatalf("ListProducts: %v", err)
	}
	for _, p := range ps {
		if p.ID == productID {
			return p.Available
		}
	}
	t.Fatalf("ListProducts has no %s", productID)
	return 0
}

// A committed reservation deletes the product's key and the list key, so
// the next read shows the new count instead of the cached one (the TTL here
// is a minute, so an unchanged stale entry would still be served).
// Releasing the stock invalidates again.
func TestReserveInvalidatesCachedStock(t *testing.T) {
	_, addr := testinfra.Redis(t)
	s := newCachedStack(t, addr)
	ctx := context.Background()

	before, err := s.catalog.GetProduct(ctx, "sku-keyboard")
	if err != nil {
		t.Fatal(err)
	}
	listedBefore := s.listed(t, "sku-keyboard")
	if !s.cached(t, "product:sku-keyboard") || !s.cached(t, keyAllProducts) {
		t.Fatal("reads did not populate the cache")
	}
	ttl, err := s.rdb.TTL(ctx, "product:sku-keyboard").Result()
	if err != nil || ttl <= 0 || ttl > time.Minute {
		t.Errorf("TTL = %v, %v; want a TTL of at most 1m", ttl, err)
	}
	// A second read is a hit.
	if _, err := s.catalog.GetProduct(ctx, "sku-keyboard"); err != nil {
		t.Fatal(err)
	}
	if hits := testutil.ToFloat64(s.catalog.hits); hits != 1 {
		t.Errorf("hits = %v, want 1", hits)
	}

	orderID := uuid.NewString()
	if _, rej, err := s.store.Reserve(ctx, uuid.NewString(), orderID, []Line{{ProductID: "sku-keyboard", Quantity: 3}}); err != nil || rej != nil {
		t.Fatalf("Reserve: %v, %v", rej, err)
	}
	if s.cached(t, "product:sku-keyboard") || s.cached(t, keyAllProducts) {
		t.Error("reserve did not delete the cached entries")
	}
	after, err := s.catalog.GetProduct(ctx, "sku-keyboard")
	if err != nil {
		t.Fatal(err)
	}
	if after.Available != before.Available-3 {
		t.Errorf("GetProduct after reserve: available %d, want %d", after.Available, before.Available-3)
	}
	if got := s.listed(t, "sku-keyboard"); got != listedBefore-3 {
		t.Errorf("ListProducts after reserve: available %d, want %d", got, listedBefore-3)
	}

	if _, err := s.store.Release(ctx, uuid.NewString(), orderID); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if got, err := s.catalog.GetProduct(ctx, "sku-keyboard"); err != nil || got.Available != before.Available {
		t.Errorf("GetProduct after release = %+v, %v; want available %d", got, err, before.Available)
	}

	// A rejected reservation changes no stock, so it leaves the cache alone.
	if _, rej, err := s.store.Reserve(ctx, uuid.NewString(), uuid.NewString(), []Line{{ProductID: "sku-soldout", Quantity: 1}}); err != nil || rej == nil {
		t.Fatalf("Reserve sku-soldout: %v, %v; want a rejection", rej, err)
	}
	if !s.cached(t, "product:sku-keyboard") {
		t.Error("a rejected reservation invalidated an unrelated product")
	}
}

// With Redis stopped, catalog reads come from Postgres, stock changes still
// commit, and the readiness report says degraded but ready.
func TestReadsSurviveRedisDown(t *testing.T) {
	ctr, addr := testinfra.Redis(t)
	s := newCachedStack(t, addr)
	ctx := context.Background()

	// Warm the cache, then take Redis away.
	if _, err := s.catalog.GetProduct(ctx, "sku-mouse"); err != nil {
		t.Fatal(err)
	}
	stopTimeout := 5 * time.Second
	if err := ctr.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop redis: %v", err)
	}

	start := time.Now()
	p, err := s.catalog.GetProduct(ctx, "sku-mouse")
	if err != nil {
		t.Fatalf("GetProduct with Redis down: %v", err)
	}
	if want := available(t, s.pool, "sku-mouse"); p.Available != want {
		t.Errorf("GetProduct available %d, want %d from Postgres", p.Available, want)
	}
	if got := s.listed(t, "sku-mouse"); got != p.Available {
		t.Errorf("ListProducts available %d, want %d", got, p.Available)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("two reads with Redis down took %v; the cache should fail fast", elapsed)
	}

	// The stock change commits even though its invalidation fails.
	if _, rej, err := s.store.Reserve(ctx, uuid.NewString(), uuid.NewString(), []Line{{ProductID: "sku-mouse", Quantity: 1}}); err != nil || rej != nil {
		t.Fatalf("Reserve with Redis down: %v, %v", rej, err)
	}
	if got, err := s.catalog.GetProduct(ctx, "sku-mouse"); err != nil || got.Available != p.Available-1 {
		t.Errorf("GetProduct after reserve = %+v, %v; want available %d", got, err, p.Available-1)
	}
	// Only the first read touched the dead Redis; every later call within
	// the bypass window went straight to Postgres.
	if errs := testutil.ToFloat64(s.catalog.errs); errs != 1 {
		t.Errorf("cache errors = %v, want 1", errs)
	}

	// Readiness as cmd/inventory registers it: Postgres required, Redis optional.
	checks := health.NewRegistry()
	checks.Add("postgres", 2*time.Second, s.pool.Ping)
	checks.AddOptional("redis", 2*time.Second, func(ctx context.Context) error { return s.rdb.Ping(ctx).Err() })
	report := checks.Run(ctx)
	if !report.Ready() || report.Status != health.StatusDegraded || report.Checks["redis"].Status != health.StatusDegraded {
		t.Errorf("readiness = %+v; want ready and degraded, with redis degraded", report)
	}
}
