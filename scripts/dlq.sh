#!/usr/bin/env bash
# Inspect and replay dead-letter topics on the local Compose stack.
#
#   scripts/dlq.sh list            print every record in every <topic>.dlq
#   scripts/dlq.sh count           print the total number of dead-letter records
#   scripts/dlq.sh replay <topic>  publish <topic>'s unreplayed records back to
#                                  its source topic (for example
#                                  inventory.commands.dlq -> inventory.commands)
#
# Kafka is reachable only on the Compose network, and the service images are
# distroless, so the work is done by the order binary's "dlq" subcommand
# inside the running order container. Replay is safe to repeat: consumers
# deduplicate by message_id, and replay progress is committed under the
# dlq-replay consumer group so the same records are not sent twice.
set -euo pipefail

cd "$(dirname "$0")/.."
compose=(docker compose -f deploy/compose/docker-compose.yml)

usage() {
	echo "usage: $0 list | count | replay <topic>.dlq" >&2
	exit 2
}

case "${1:-}" in
list | count)
	[[ $# -eq 1 ]] || usage
	"${compose[@]}" exec -T order /order dlq "$1"
	;;
replay)
	[[ $# -eq 2 && -n "$2" ]] || usage
	"${compose[@]}" exec -T order /order dlq replay "$2"
	;;
*)
	usage
	;;
esac
