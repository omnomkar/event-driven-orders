#!/usr/bin/env bash
# Chaos test: kill the payment service while orders are in flight, restart
# it, and check that every order still finishes with exactly one charge.
#
#   1. Start the stack (make up) and place N orders concurrently through the
#      gateway, all with the good test card and products with deep stock.
#   2. As soon as the payment service has recorded its first charge for this
#      run, kill its container (SIGKILL, no graceful shutdown), keep it down,
#      then start it again. The kill counts as mid-flow if any order was not
#      final at that moment, in either (or both) of two ways:
#        - not yet charged: its ChargePayment is consumed, or redelivered if
#          it was in progress, after the restart;
#        - charged but not yet confirmed: its PaymentSucceeded reaches the
#          order service late, from payment's outbox if the relay had not
#          published it before the kill.
#      The run fails if every order was already final at the kill.
#   3. Wait until every order is CONFIRMED or CANCELLED.
#   4. Check in Postgres that every order is final, no order has more than
#      one payment, successful payments equal confirmed orders, and no record
#      was added to any dead-letter topic.
#
# The payment service must be killed after the requests return, not during
# them: the gateway tokenizes cards through payment, so requests sent while
# it is down fail at the gateway and never become orders.
#
# Settings (environment, all optional):
#   CHAOS_ORDERS        orders to place (default 50)
#   CHAOS_CONCURRENCY   requests in flight at once (default 10)
#   CHAOS_DOWN_SECONDS  how long payment stays down (default 5)
#   CHAOS_KILL_AFTER    kill once this many of the run's charges exist (default 1)
#   CHAOS_TIMEOUT       seconds to wait for every order to finish (default 180)
#   GATEWAY_URL         gateway base URL (default http://localhost:8080)
#
# The report is printed and saved to scripts/results/chaos-<timestamp>.txt.
# Exit status is 0 only if every check passes.
set -euo pipefail

cd "$(dirname "$0")/.."

orders=${CHAOS_ORDERS:-50}
concurrency=${CHAOS_CONCURRENCY:-10}
down_seconds=${CHAOS_DOWN_SECONDS:-5}
kill_after=${CHAOS_KILL_AFTER:-1}
timeout=${CHAOS_TIMEOUT:-180}
gateway=${GATEWAY_URL:-http://localhost:8080}

run_id="chaos-$(date -u +%Y%m%dT%H%M%SZ)"
report="scripts/results/${run_id}.txt"
compose=(docker compose -f deploy/compose/docker-compose.yml)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }

# sql runs a query inside the Postgres container, so no database port needs
# to be published, and prints the rows unaligned.
sql() {
	"${compose[@]}" exec -T postgres psql -U postgres -d "$1" -tA -v ON_ERROR_STOP=1 -c "$2"
}

dlq_count() { "${compose[@]}" exec -T order /order dlq count; }

count_orders() {
	sql orders "SELECT count(*) FROM orders WHERE customer_id = '${run_id}' $1"
}

log "starting the stack"
make up >&2
dlq_before=$(dlq_count)

# Standard test card only. Every order uses the same items and a unique
# Idempotency-Key, and the run ID as customer_id so its orders can be found.
body=$(printf '{"customer_id": "%s", "items": [{"product_id": "sku-keyboard", "quantity": 1}, {"product_id": "sku-cable", "quantity": 2}], "card": {"number": "4242 4242 4242 4242", "exp_month": 12, "exp_year": 2030}}' "$run_id")

# Database clock, so payments of this run can be told apart from older ones.
run_start=$(sql payments "SELECT now()")

log "placing ${orders} orders, ${concurrency} at a time"
seq 1 "$orders" | xargs -P "$concurrency" -I{} \
	curl -s -o /dev/null -w '%{http_code}\n' --max-time 10 -X POST \
	-H 'Content-Type: application/json' -H "Idempotency-Key: ${run_id}-{}" \
	--data "$body" "${gateway}/orders" >"$tmp/codes" || true
accepted=$(grep -c '^202$' "$tmp/codes" || true)

# Kill payment mid-run: once it has started charging, before it finishes.
charges_since_start() {
	sql payments "SELECT count(*) FROM payments WHERE created_at >= '${run_start}'"
}
wait_deadline=$(($(date +%s) + 30))
while [[ $(charges_since_start) -lt $kill_after && $(date +%s) -lt $wait_deadline ]]; do
	sleep 0.1
done
log "killing payment"
"${compose[@]}" kill payment >&2
kill_epoch=$(date +%s)
kill_time=$(date -u +%Y-%m-%dT%H:%M:%SZ)
# Snapshot the orders first: the order service keeps applying replies that
# were already in Kafka, so these counts can only move toward final. The
# payment counts are stable once payment is dead.
at_kill=$(sql orders "SELECT status || '=' || count(*) FROM orders WHERE customer_id = '${run_id}' GROUP BY status ORDER BY status" | paste -sd ' ')
in_flight=$(count_orders "AND status NOT IN ('CONFIRMED', 'CANCELLED')")
confirmed_at_kill=$(count_orders "AND status = 'CONFIRMED'")
charged_at_kill=$(charges_since_start)
unpublished_at_kill=$(sql payments "SELECT count(*) FROM outbox WHERE sent_at IS NULL AND created_at >= '${run_start}'")
uncharged_at_kill=$((accepted - charged_at_kill))
charged_unconfirmed_at_kill=$((charged_at_kill - confirmed_at_kill))
if [[ $charged_unconfirmed_at_kill -lt 0 ]]; then
	charged_unconfirmed_at_kill=0
fi
log "orders at kill: ${at_kill} (${in_flight} not final)"

sleep "$down_seconds"
log "starting payment again"
# compose start also re-runs the one-shot kafka-init dependency first, so
# the real downtime is longer than CHAOS_DOWN_SECONDS; it is measured here.
"${compose[@]}" start payment >&2
restart_epoch=$(date +%s)
down_actual=$((restart_epoch - kill_epoch))

log "waiting up to ${timeout}s for every order to finish"
pending=$in_flight
while :; do
	pending=$(count_orders "AND status NOT IN ('CONFIRMED', 'CANCELLED')")
	elapsed=$(($(date +%s) - restart_epoch))
	if [[ $pending -eq 0 || $elapsed -ge $timeout ]]; then
		break
	fi
	sleep 1
done
finish_seconds=$(($(date +%s) - restart_epoch))

total=$(count_orders "")
confirmed=$(count_orders "AND status = 'CONFIRMED'")
cancelled=$(count_orders "AND status = 'CANCELLED'")
ids=$(sql orders "SELECT '{' || coalesce(string_agg(id::text, ','), '') || '}' FROM orders WHERE customer_id = '${run_id}'")
payments=$(sql payments "SELECT count(*) FROM payments WHERE order_id = ANY('${ids}'::uuid[])")
succeeded=$(sql payments "SELECT count(*) FROM payments WHERE order_id = ANY('${ids}'::uuid[]) AND status = 'SUCCEEDED'")
multi=$(sql payments "SELECT count(*) FROM (SELECT order_id FROM payments WHERE order_id = ANY('${ids}'::uuid[]) GROUP BY order_id HAVING count(*) > 1) AS d")
dlq_after=$(dlq_count)
dlq_added=$((dlq_after - dlq_before))
# Redeliveries the restarted service skipped by message_id. Zero is possible:
# it means the kill landed between commits.
skipped=$("${compose[@]}" logs --no-color --since "$kill_time" payment | grep -c 'duplicate command skipped' || true)

failures=()
check() {
	if [[ $1 != "$2" ]]; then
		failures+=("$3: got $1, want $2")
	fi
}
check "$accepted" "$orders" "orders accepted (202)"
check "$total" "$accepted" "orders stored"
check "$pending" 0 "orders not final"
check "$confirmed" "$accepted" "orders confirmed (good card, stock available)"
check "$multi" 0 "orders with more than one payment"
check "$succeeded" "$confirmed" "successful payments vs confirmed orders"
check "$dlq_added" 0 "records added to dead-letter topics"
cases=()
if [[ $uncharged_at_kill -gt 0 ]]; then
	cases+=("not yet charged: ${uncharged_at_kill} order(s); their ChargePayment was handled after the restart")
fi
if [[ $charged_unconfirmed_at_kill -gt 0 ]]; then
	cases+=("charged but not yet confirmed: ${charged_unconfirmed_at_kill} order(s); ${unpublished_at_kill} of their PaymentSucceeded replies were still in payment's outbox and were sent by the relay after the restart")
fi
if [[ $in_flight -eq 0 ]]; then
	failures+=("every order was already final when payment was killed; raise CHAOS_ORDERS")
fi
result=PASS
if [[ ${#failures[@]} -gt 0 ]]; then
	result=FAIL
fi

mkdir -p scripts/results
{
	echo "Chaos run ${run_id}"
	echo
	echo "Setup"
	echo "  orders placed:                 ${orders} (concurrency ${concurrency})"
	echo "  accepted by the gateway (202): ${accepted}"
	echo "  payment killed at:             ${kill_time} (SIGKILL)"
	echo "  payment down for:              ${down_actual}s (kill until restart)"
	echo "  orders at kill:                ${at_kill:-none}"
	echo "  payments recorded at kill:     ${charged_at_kill}"
	echo "  not final at kill:             ${in_flight}"
	echo
	echo "Mid-flow cases at the kill"
	if [[ ${#cases[@]} -eq 0 ]]; then
		echo "  none: every order was already final"
	fi
	for c in "${cases[@]}"; do
		echo "  - ${c}"
	done
	echo
	echo "Outcome"
	echo "  all final within:              ${finish_seconds}s of restart"
	echo "  confirmed:                     ${confirmed}"
	echo "  cancelled:                     ${cancelled}"
	echo "  not final:                     ${pending}"
	echo "  payment rows:                  ${payments} (${succeeded} succeeded)"
	echo "  orders with >1 payment:        ${multi}"
	echo "  redeliveries skipped by dedup: ${skipped} (payment service, after restart)"
	echo "  dead-letter records added:     ${dlq_added}"
	echo
	echo "Result: ${result}"
	for f in "${failures[@]}"; do
		echo "  - ${f}"
	done
} | tee "$report"
log "report saved to ${report}"

[[ $result == PASS ]]
