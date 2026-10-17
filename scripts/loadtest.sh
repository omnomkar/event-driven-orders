#!/usr/bin/env bash
# Load test: run loadtest/order-flow.js against the Compose stack with the
# catalog cache on or off, and save the raw results.
#
#   scripts/loadtest.sh on|off
#
#   1. Start the stack with CACHE_ENABLED set accordingly (this recreates the
#      inventory container if the mode changed) and confirm the mode from
#      inventory's /readyz (the redis check exists only with the cache on).
#   2. Reset the deep-stock products to 10,000,000 units through psql, so the
#      run never sells out, and empty Redis so no entry predates the reset.
#   3. Wait until the gateway answers product reads (after inventory is
#      recreated, the gateway's gRPC connection needs a few seconds to
#      reconnect).
#   4. Snapshot the Prometheus counters, run k6 in its pinned container on
#      the Compose network, and wait until every order of the run is final.
#   5. Snapshot again and save, under loadtest/results/:
#        <timestamp>-cache-<on|off>.json         k6 summary export (raw)
#        <timestamp>-cache-<on|off>.txt          k6 console output (raw)
#        <timestamp>-cache-<on|off>-server.json  server-side snapshot:
#          Prometheus (saga duration quantiles, max consumer lag, saga
#          outcome counts, cache hits and misses, DLQ records) and
#          Postgres (exact saga durations and statuses of the run's orders)
#
# Settings (environment, all optional), passed to k6:
#   LOADTEST_VUS (50), LOADTEST_RAMP_UP (1m), LOADTEST_HOLD (2m),
#   LOADTEST_RAMP_DOWN (30s), LOADTEST_READ_RATIO (0.8),
#   LOADTEST_THINK_SECONDS (0.1)
# and for the script:
#   LOADTEST_DRAIN_TIMEOUT  seconds to wait for the run's orders (default 300)
#
# Exit status is k6's (99 means a threshold failed; the results are saved
# either way), or 1 if the stack or the drain wait fails.
set -euo pipefail

cd "$(dirname "$0")/.."

mode=${1:-}
case "$mode" in
on) cache_enabled=true ;;
off) cache_enabled=false ;;
*)
	echo "usage: $0 on|off" >&2
	exit 2
	;;
esac

k6_image=grafana/k6:2.3.0
network=eventorders_default
prometheus=http://localhost:9090
gateway=http://localhost:8080
drain_timeout=${LOADTEST_DRAIN_TIMEOUT:-300}

stamp=$(date -u +%Y%m%dT%H%M%SZ)
name="${stamp}-cache-${mode}"
run_id="k6-${stamp}-${mode}"
results=loadtest/results
compose=(docker compose -f deploy/compose/docker-compose.yml)

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }

sql() {
	"${compose[@]}" exec -T postgres psql -U postgres -d "$1" -tA -v ON_ERROR_STOP=1 -c "$2"
}

# prom prints the first value of an instant query evaluated at time $2 (Unix
# seconds), or nothing if the result is empty (for example the cache
# counters, which do not exist with the cache off; they are saved as null).
prom() {
	curl -s --max-time 10 --get --data-urlencode "query=$1" --data-urlencode "time=$2" \
		"${prometheus}/api/v1/query" |
		{ grep -o '"value":\[[^]]*\]' || true; } | head -1 | sed -E 's/.*,"([^"]*)"\]$/\1/'
}

# json_num prints $1 as a JSON number, or null if it is empty or not finite.
json_num() {
	if [[ "$1" =~ ^-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?$ ]]; then
		printf '%s' "$1"
	else
		printf 'null'
	fi
}

log "starting the stack with CACHE_ENABLED=${cache_enabled}"
CACHE_ENABLED=$cache_enabled make up >&2

readyz=$(curl -s --max-time 5 http://localhost:8083/readyz)
if [[ "$cache_enabled" == true && "$readyz" != *'"redis"'* ]] ||
	[[ "$cache_enabled" == false && "$readyz" == *'"redis"'* ]]; then
	echo "inventory is not in cache mode ${mode}: ${readyz}" >&2
	exit 1
fi

log "resetting stock and emptying the cache"
sql inventory "UPDATE products SET available = 10000000, updated_at = now()
	WHERE id IN ('sku-keyboard', 'sku-mouse', 'sku-monitor', 'sku-headset', 'sku-cable')" >/dev/null
"${compose[@]}" exec -T redis redis-cli FLUSHDB >/dev/null

log "waiting for the gateway to serve product reads"
ok=0
deadline=$(($(date +%s) + 60))
while ((ok < 5)); do
	if [[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "${gateway}/products/sku-cable")" == 200 ]]; then
		ok=$((ok + 1))
	else
		ok=0
	fi
	if (($(date +%s) > deadline)); then
		echo "gateway did not serve product reads within 60s" >&2
		exit 1
	fi
	sleep 0.5
done

# Counters read at the start and end of the run. Prometheus scrapes every
# 5s, so wait one interval for the warm-up reads to be scraped.
counters=(
	'hits_product=sum(inventory_cache_hits_total{key="product"})'
	'hits_list=sum(inventory_cache_hits_total{key="list"})'
	'misses_product=sum(inventory_cache_misses_total{key="product"})'
	'misses_list=sum(inventory_cache_misses_total{key="list"})'
	'cache_errors=sum(inventory_cache_errors_total)'
	'saga_confirmed=sum(saga_outcomes_total{outcome="confirmed"})'
	'saga_cancelled_out_of_stock=sum(saga_outcomes_total{outcome="cancelled",reason="out_of_stock"})'
	'saga_cancelled_payment_failed=sum(saga_outcomes_total{outcome="cancelled",reason="payment_failed"})'
	'saga_dead_lettered=sum(saga_outcomes_total{outcome="dead_lettered"})'
	'dlq_records=sum(kafka_dlq_records_total) or vector(0)'
)
sleep 6
start=$(date +%s)
declare -A before
for c in "${counters[@]}"; do
	before[${c%%=*}]=$(prom "${c#*=}" "$start")
done

log "running k6 (${k6_image}), results in ${results}/${name}.*"
k6_status=0
docker run --rm --network "$network" \
	-u "$(id -u):$(id -g)" \
	-v "$PWD/loadtest:/loadtest:ro" -v "$PWD/${results}:/results" \
	"$k6_image" run --quiet --no-color \
	--summary-trend-stats 'avg,min,med,p(90),p(95),p(99),max' \
	--summary-export "/results/${name}.json" \
	-e RUN_ID="$run_id" \
	-e TARGET_VUS="${LOADTEST_VUS:-50}" \
	-e RAMP_UP="${LOADTEST_RAMP_UP:-1m}" \
	-e HOLD="${LOADTEST_HOLD:-2m}" \
	-e RAMP_DOWN="${LOADTEST_RAMP_DOWN:-30s}" \
	-e READ_RATIO="${LOADTEST_READ_RATIO:-0.8}" \
	-e THINK_SECONDS="${LOADTEST_THINK_SECONDS:-0.1}" \
	/loadtest/order-flow.js 2>&1 | tee "${results}/${name}.txt" || k6_status=${PIPESTATUS[0]}
k6_end=$(date +%s)

log "waiting for every order of the run to be final"
pending() {
	sql orders "SELECT count(*) FROM orders WHERE customer_id = '${run_id}' AND status NOT IN ('CONFIRMED', 'CANCELLED')"
}
deadline=$(($(date +%s) + drain_timeout))
while [[ "$(pending)" != 0 ]]; do
	if (($(date +%s) > deadline)); then
		echo "orders still not final after ${drain_timeout}s: $(pending)" >&2
		exit 1
	fi
	sleep 1
done
drained=$(date +%s)
sleep 6 # one more scrape, so the last outcomes are in Prometheus
end=$(date +%s)
window=$((end - start))

log "saving the server-side snapshot"
declare -A after
for c in "${counters[@]}"; do
	after[${c%%=*}]=$(prom "${c#*=}" "$end")
done

# Saga duration quantiles over the whole run, estimated by Prometheus from
# histogram buckets (linear interpolation inside a bucket).
quantile() {
	prom "histogram_quantile($1, sum by (le) (increase(saga_duration_seconds_bucket[${window}s])))" "$end"
}
# Highest total lag of a group (summed over its partitions) at any
# evaluation step during the run. The gauge itself refreshes every 10s.
max_lag() {
	prom "max_over_time((sum(kafka_consumer_lag{group=\"$1\"}))[${window}s:5s])" "$end"
}

# Exact saga durations of the run's orders, from the database: created_at
# to the last status change.
IFS='|' read -r db_orders db_confirmed db_cancelled db_p50 db_p95 db_p99 db_max < <(sql orders "
	SELECT count(*),
	       count(*) FILTER (WHERE status = 'CONFIRMED'),
	       count(*) FILTER (WHERE status = 'CANCELLED'),
	       percentile_cont(0.50) WITHIN GROUP (ORDER BY d),
	       percentile_cont(0.95) WITHIN GROUP (ORDER BY d),
	       percentile_cont(0.99) WITHIN GROUP (ORDER BY d),
	       max(d)
	FROM (SELECT status, EXTRACT(EPOCH FROM updated_at - created_at)::float8 AS d
	      FROM orders WHERE customer_id = '${run_id}') o")
db_payments=$(sql payments "SELECT count(*) FROM payments p WHERE p.created_at >= to_timestamp(${start})")

{
	printf '{\n'
	printf '  "run_id": "%s",\n  "cache": "%s",\n' "$run_id" "$mode"
	printf '  "window": {"start_unix": %s, "k6_end_unix": %s, "drained_unix": %s, "end_unix": %s, "seconds": %s},\n' \
		"$start" "$k6_end" "$drained" "$end" "$window"
	printf '  "k6_exit_status": %s,\n' "$k6_status"
	printf '  "prometheus": {\n'
	printf '    "saga_duration_seconds": {"p50": %s, "p95": %s, "p99": %s, "method": "histogram_quantile over increase(saga_duration_seconds_bucket[window])"},\n' \
		"$(json_num "$(quantile 0.50)")" "$(json_num "$(quantile 0.95)")" "$(json_num "$(quantile 0.99)")"
	printf '    "max_consumer_lag": {"order-saga": %s, "inventory": %s, "payment": %s, "method": "max_over_time of the per-group sum of kafka_consumer_lag, 5s steps; the gauge refreshes every 10s"},\n' \
		"$(json_num "$(max_lag order-saga)")" "$(json_num "$(max_lag inventory)")" "$(json_num "$(max_lag payment)")"
	printf '    "counters_increase": {\n'
	first=1
	for c in "${counters[@]}"; do
		key=${c%%=*}
		delta=$(awk -v a="${after[$key]:-}" -v b="${before[$key]:-}" 'BEGIN { if (a == "" || b == "") print ""; else print a - b }')
		[[ $first == 1 ]] || printf ',\n'
		printf '      "%s": %s' "$key" "$(json_num "$delta")"
		first=0
	done
	printf '\n    }\n  },\n'
	printf '  "postgres": {\n'
	printf '    "orders": %s, "confirmed": %s, "cancelled": %s, "payments_since_start": %s,\n' \
		"$(json_num "$db_orders")" "$(json_num "$db_confirmed")" "$(json_num "$db_cancelled")" "$(json_num "$db_payments")"
	printf '    "saga_duration_seconds": {"p50": %s, "p95": %s, "p99": %s, "max": %s, "method": "exact, updated_at - created_at of the run'"'"'s orders"}\n' \
		"$(json_num "$db_p50")" "$(json_num "$db_p95")" "$(json_num "$db_p99")" "$(json_num "$db_max")"
	printf '  }\n}\n'
} >"${results}/${name}-server.json"

cat "${results}/${name}-server.json"
log "done: k6 exit status ${k6_status}"
exit "$k6_status"
