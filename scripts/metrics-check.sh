#!/usr/bin/env bash
# Metrics check, against a running stack (make up):
#   1. Place one order with the good test card and wait until it is CONFIRMED.
#   2. Check on the order service's /metrics that the confirmed saga counter
#      and the saga duration histogram both went up.
#   3. Check that Prometheus scrapes all four services and has the metrics
#      the stack promises: HTTP and gRPC latency, saga outcomes and duration,
#      consumer lag, outbox backlog, and cache counters.
#
# Settings (environment, all optional):
#   GATEWAY_URL     gateway base URL (default http://localhost:8080)
#   ORDER_ADMIN     order service admin URL (default http://localhost:8081)
#   PROMETHEUS_URL  Prometheus URL (default http://localhost:9090)
#   METRICS_TIMEOUT seconds to wait for each condition (default 60)
#
# Exit status is 0 only if every check passes.
set -euo pipefail

gateway=${GATEWAY_URL:-http://localhost:8080}
order_admin=${ORDER_ADMIN:-http://localhost:8081}
prometheus=${PROMETHEUS_URL:-http://localhost:9090}
timeout=${METRICS_TIMEOUT:-60}

failures=0
pass() { printf '  PASS  %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; failures=$((failures + 1)); }

# scrape prints the value of one exact series (name plus labels, as the
# service prints it) from the order service's /metrics, or 0 if absent.
scrape() {
	curl -s --max-time 5 "${order_admin}/metrics" |
		awk -v series="$1" '$1 == series { print $2; found = 1 } END { if (!found) print 0 }'
}

# prom prints the first value of an instant query, or nothing if the result
# is empty. The API answers JSON; the value is the last string in the first
# "value" pair, e.g. "value":[1760000000.123,"4"].
prom() {
	curl -s --max-time 5 --get --data-urlencode "query=$1" "${prometheus}/api/v1/query" |
		grep -o '"value":\[[^]]*\]' | head -1 | sed -E 's/.*,"([^"]*)"\]$/\1/'
}

# gt reports whether $1 > $2, as numbers.
gt() { awk -v a="$1" -v b="$2" 'BEGIN { exit !(a + 0 > b + 0) }'; }

# wait_for runs a command until it succeeds or the timeout passes.
wait_for() {
	local deadline=$(($(date +%s) + timeout))
	until "$@"; do
		if (($(date +%s) >= deadline)); then
			return 1
		fi
		sleep 1
	done
}

confirmed_series='saga_outcomes_total{outcome="confirmed",reason="none"}'
duration_series='saga_duration_seconds_count{outcome="confirmed"}'

echo "Saga metrics on the order service (${order_admin}/metrics):"
confirmed_before=$(scrape "$confirmed_series")
duration_before=$(scrape "$duration_series")

# Standard test card only.
run_id="metrics-$(date -u +%Y%m%dT%H%M%SZ)"
body=$(printf '{"customer_id": "%s", "items": [{"product_id": "sku-cable", "quantity": 1}], "card": {"number": "4242 4242 4242 4242", "exp_month": 12, "exp_year": 2030}}' "$run_id")
resp=$(curl -s --max-time 10 -X POST -H 'Content-Type: application/json' --data "$body" "${gateway}/orders")
order_id=$(grep -o '"order_id":"[^"]*"' <<<"$resp" | cut -d'"' -f4 || true)
if [[ -z "$order_id" ]]; then
	fail "POST /orders did not return an order: ${resp}"
	echo "1 check(s) failed"
	exit 1
fi

order_confirmed() {
	curl -s --max-time 5 "${gateway}/orders/${order_id}" | grep -q '"status":"CONFIRMED"'
}
if wait_for order_confirmed; then
	pass "order ${order_id} reached CONFIRMED"
else
	fail "order ${order_id} not CONFIRMED within ${timeout}s"
fi

confirmed_after=$(scrape "$confirmed_series")
duration_after=$(scrape "$duration_series")
if gt "$confirmed_after" "$confirmed_before"; then
	pass "${confirmed_series}: ${confirmed_before} -> ${confirmed_after}"
else
	fail "${confirmed_series} did not increase: ${confirmed_before} -> ${confirmed_after}"
fi
if gt "$duration_after" "$duration_before"; then
	pass "${duration_series}: ${duration_before} -> ${duration_after}"
else
	fail "${duration_series} did not increase: ${duration_before} -> ${duration_after}"
fi

echo "Prometheus (${prometheus}):"
targets_up() { [[ "$(prom 'sum(up{job="services"})')" == "4" ]]; }
if wait_for targets_up; then
	pass "all 4 services scraped and up"
else
	fail "services up: $(prom 'sum(up{job="services"})' || true), want 4"
fi

# Prometheus has seen the new confirmed count (it scrapes every 5s).
scraped_confirmed() {
	local v
	v=$(prom 'sum(saga_outcomes_total{outcome="confirmed"})')
	[[ -n "$v" ]] && ! gt "$confirmed_after" "$v"
}
if wait_for scraped_confirmed; then
	pass "Prometheus has saga_outcomes_total{outcome=\"confirmed\"} >= ${confirmed_after}"
else
	fail "Prometheus saga_outcomes_total{outcome=\"confirmed\"} = $(prom 'sum(saga_outcomes_total{outcome="confirmed"})' || true), want >= ${confirmed_after}"
fi

# Each expected metric has at least one series, from the named service.
for check in \
	'gateway:http_server_request_duration_seconds_count{route="/orders",method="POST"}' \
	'order:grpc_server_handling_seconds_count{method="/eventorders.order.v1.OrderService/CreateOrder"}' \
	'order:saga_duration_seconds_count' \
	'order:kafka_consumer_lag{group="order-saga"}' \
	'payment:kafka_consumer_lag{group="payment"}' \
	'inventory:kafka_consumer_lag{group="inventory"}' \
	'order:outbox_pending_rows' \
	'payment:outbox_pending_rows' \
	'inventory:outbox_pending_rows' \
	'inventory:inventory_cache_misses_total'; do
	service=${check%%:*}
	selector=${check#*:}
	# Add the service label inside the selector's braces (or add braces).
	if [[ "$selector" == *"}" ]]; then
		query="count(${selector%\}},service=\"${service}\"})"
	else
		query="count(${selector}{service=\"${service}\"})"
	fi
	has_series() { [[ -n "$(prom "$query")" ]]; }
	if wait_for has_series; then
		pass "${service}: ${selector}"
	else
		fail "${service}: no series for ${selector}"
	fi
done

if ((failures > 0)); then
	echo "${failures} check(s) failed"
	exit 1
fi
echo "All metrics checks passed"
