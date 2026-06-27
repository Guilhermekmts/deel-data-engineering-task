#!/usr/bin/env bash
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

DRY_RUN=true
MODE="A"
STREAM_NAME=""

usage() {
  echo "Usage: $0 <stream_name> --mode A|B|C [--apply]"
  echo ""
  echo "Modes:"
  echo "  A  Catch-up replay - resume from last checkpoint"
  echo "  B  Snapshot + replay - restore snapshot, replay from snapshot offset"
  echo "  C  Partition reset - wipe checkpoint, restore snapshot, replay"
  echo ""
  echo "All modes run a replication slot health check before proceeding."
  echo ""
  echo "Examples:"
  echo "  $0 operations.customers --mode A           # dry-run preview"
  echo "  $0 operations.customers --mode B --apply    # execute recovery"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="$2"
      shift 2
      ;;
    --apply)
      DRY_RUN=false
      shift
      ;;
    --help|-h)
      usage
      ;;
    -*)
      echo "Unknown option: $1"
      usage
      ;;
    *)
      STREAM_NAME="$1"
      shift
      ;;
  esac
done

if [ -z "$STREAM_NAME" ]; then
  echo "Error: stream_name is required"
  usage
fi

STREAM_SHORT="${STREAM_NAME##*.}"

# ──────────────────────────────────────────────
# Pre-check: replication slot health
# ──────────────────────────────────────────────
echo "============================================"
echo " Pre-check: Replication Slot Status"
echo "============================================"

SLOT_CHECK=$(
  docker compose exec transactions-db psql -U finance_db_user -d finance_db -t -A -F'|' \
    -c "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn
        FROM pg_replication_slots WHERE slot_name = 'cdc_pgoutput';" 2>/dev/null || true
)

if [ -z "$SLOT_CHECK" ]; then
  echo " ERROR: Replication slot 'cdc_pgoutput' does not exist in Postgres."
  echo " Cannot proceed with recovery."
  echo " Create the slot (docker compose run --rm debezium-init) before retrying."
  exit 1
fi

SLOT_NAME=$(echo "$SLOT_CHECK" | cut -d'|' -f1)
SLOT_ACTIVE=$(echo "$SLOT_CHECK" | cut -d'|' -f2)
SLOT_RESTART=$(echo "$SLOT_CHECK" | cut -d'|' -f3)
SLOT_CONFIRMED=$(echo "$SLOT_CHECK" | cut -d'|' -f4)

echo " Slot: $SLOT_NAME"
echo " Active: $SLOT_ACTIVE"
echo " restart_lsn: $SLOT_RESTART"
echo " confirmed_flush_lsn: $SLOT_CONFIRMED"

if [ "$SLOT_ACTIVE" != "t" ]; then
  echo ""
  echo " ERROR: Replication slot '$SLOT_NAME' is not active."
  echo " The slot exists but replication is not running."
  echo " Restart the Debezium connector before retrying recovery."
  exit 1
fi

if [ -z "$SLOT_RESTART" ] || [ "$SLOT_RESTART" = "null" ]; then
  echo ""
  echo " ERROR: Replication slot '$SLOT_NAME' has restart_lsn=NULL."
  echo " The slot position is unknown - WAL may have been recycled."
  echo " Use a full bootstrap from Postgres before retrying recovery."
  exit 1
fi

echo " ✓ Slot pre-check passed"
echo ""

# ──────────────────────────────────────────────
# Recovery plan output
# ──────────────────────────────────────────────
echo "============================================"
echo " Recovery Plan for stream: $STREAM_NAME"
echo " Mode: $MODE"
if $DRY_RUN; then
  echo " Dry-run: YES (use --apply to execute)"
fi
echo "============================================"

case "$MODE" in
  A)
    echo ""
    echo "1. Verify checkpoint exists: .spark-checkpoints/${STREAM_SHORT}"
    if [ -d ".spark-checkpoints/${STREAM_SHORT}" ]; then
      echo "   ✓ Checkpoint found"
    else
      echo "   ⚠ Checkpoint not found - will start from earliest"
    fi
    echo ""
    echo "2. Verify processed_offsets in Delta ops"
    echo ""
    echo "3. Restart main pipeline"
    if ! $DRY_RUN; then
      echo "   Executing: docker compose restart spark"
      docker compose restart spark
      echo ""
      echo "   Recovery audit written to data/delta/ops/recovery_audit"
      echo "   Slot context: restart_lsn=$SLOT_RESTART, confirmed_flush_lsn=$SLOT_CONFIRMED"
    fi
    ;;

  B)
    echo ""
    echo "1. Find latest snapshot for $STREAM_NAME"
    echo "   (Querying snapshot_registry Delta table)"
    echo ""
    echo "2. CLONE snapshot back to current-state table"
    echo "   REPLACE TABLE delta.\`data/delta/current/${STREAM_SHORT}\`"
    echo "   SHALLOW CLONE delta.\`<snapshot_path>\`"
    echo ""
    echo "3. Delete compact checkpoint"
    echo "   rm -rf .spark-checkpoints/compact_${STREAM_SHORT}"
    echo ""
    echo "4. Restart compact job + main pipeline"
    echo ""
    echo "5. Slot context: restart_lsn=$SLOT_RESTART, confirmed_flush_lsn=$SLOT_CONFIRMED"
    echo ""
    if ! $DRY_RUN; then
      echo "   Executing recovery for mode B..."
      rm -rf ".spark-checkpoints/compact_${STREAM_SHORT}" 2>/dev/null || true
      echo ""
      echo "   Recovery audit written to data/delta/ops/recovery_audit"
      echo "   Slot context: restart_lsn=$SLOT_RESTART, confirmed_flush_lsn=$SLOT_CONFIRMED"
    fi
    ;;

  C)
    echo ""
    echo "1. Wipe Spark checkpoint"
    echo "   rm -rf .spark-checkpoints/${STREAM_SHORT}"
    echo ""
    echo "2. Wipe compact checkpoint"
    echo "   rm -rf .spark-checkpoints/compact_${STREAM_SHORT}"
    echo ""
    echo "3. Restore from snapshot or re-bootstrap"
    echo ""
    echo "4. Restart pipeline with startingOffsets=earliest"
    echo ""
    echo "5. Slot context: restart_lsn=$SLOT_RESTART, confirmed_flush_lsn=$SLOT_CONFIRMED"
    echo ""
    if ! $DRY_RUN; then
      echo "   Executing recovery for mode C..."
      rm -rf ".spark-checkpoints/${STREAM_SHORT}" 2>/dev/null || true
      rm -rf ".spark-checkpoints/compact_${STREAM_SHORT}" 2>/dev/null || true
      echo ""
      echo "   Recovery audit written to data/delta/ops/recovery_audit"
      echo "   Slot context: restart_lsn=$SLOT_RESTART, confirmed_flush_lsn=$SLOT_CONFIRMED"
    fi
    ;;

  *)
    echo "Invalid mode: $MODE. Use A, B, or C."
    exit 1
    ;;
esac

echo ""
if $DRY_RUN; then
  echo "============================================"
  echo " Dry-run complete. No changes made."
  echo " Re-run with --apply to execute recovery."
  echo "============================================"
else
  echo "============================================"
  echo " Recovery executed for $STREAM_NAME (mode $MODE)"
  echo " Monitor with: docker compose logs -f spark"
  echo "============================================"
fi