#!/usr/bin/env bash
# Proves the local stack is up:
#   1. every service answers 200 on /readyz
#   2. each service's database role can connect only to its own database
# Exits non-zero if any check fails.
set -euo pipefail

cd "$(dirname "$0")" || exit 1
compose=(docker compose -f docker-compose.yml)

if [[ ! -f .env ]]; then
  echo "missing deploy/compose/.env (copy .env.example)" >&2
  exit 1
fi
# shellcheck disable=SC1091
source .env

failures=0
pass() { printf '  PASS  %s\n' "$1"; }
fail() { printf '  FAIL  %s\n' "$1"; failures=$((failures + 1)); }

echo "Readiness (/readyz):"
for entry in gateway:8080 order:8081 payment:8082 inventory:8083; do
  name="${entry%%:*}"
  port="${entry##*:}"
  body="$(curl -s --max-time 5 -w '\n%{http_code}' "http://localhost:${port}/readyz" || true)"
  code="${body##*$'\n'}"
  if [[ "$code" == "200" ]]; then
    pass "$name ($port)"
  else
    fail "$name ($port) status=${code:-none} body=${body%$'\n'*}"
  fi
done

# Tries to connect as role to db over TCP (password auth). Prints psql's error
# output, or nothing on success.
try_connect() {
  local role="$1" password="$2" db="$3"
  # Order matters: stderr goes to the capture, then stdout is discarded.
  # shellcheck disable=SC2069
  "${compose[@]}" exec -T -e PGPASSWORD="$password" postgres \
    psql -h 127.0.0.1 -U "$role" -d "$db" -tAc 'SELECT 1' 2>&1 >/dev/null
}

echo "Database role isolation:"
roles=(order_svc payment_svc inventory_svc)
passwords=("$ORDER_DB_PASSWORD" "$PAYMENT_DB_PASSWORD" "$INVENTORY_DB_PASSWORD")
dbs=(orders payments inventory)
for i in "${!roles[@]}"; do
  for j in "${!dbs[@]}"; do
    role="${roles[$i]}" db="${dbs[$j]}"
    if err="$(try_connect "$role" "${passwords[$i]}" "$db")" && [[ -z "$err" ]]; then
      connected=yes
    else
      connected=no
    fi

    if [[ "$i" == "$j" ]]; then
      if [[ "$connected" == yes ]]; then
        pass "$role can connect to $db"
      else
        fail "$role cannot connect to its own database $db: $err"
      fi
    elif [[ "$connected" == no && "$err" == *"permission denied for database"* ]]; then
      pass "$role is denied on $db"
    else
      fail "$role on $db: expected permission denied, got: ${err:-connected}"
    fi
  done
done

if (( failures > 0 )); then
  echo "$failures check(s) failed"
  exit 1
fi
echo "All checks passed"
