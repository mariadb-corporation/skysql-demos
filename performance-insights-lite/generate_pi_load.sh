#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ -f "$SCRIPT_DIR/.env" ]]; then
  set -a
  . "$SCRIPT_DIR/.env"
  set +a
fi

: "${DB_HOST:?Set DB_HOST in .env}"
: "${DB_PORT:?Set DB_PORT in .env}"
: "${DB_USER:?Set DB_USER in .env}"
: "${DB_PASS:?Set DB_PASS in .env}"

PI_SCHEMA="${PI_SCHEMA:-sky}"
TEST_SEED_ROWS="${TEST_SEED_ROWS:-512}"
TEST_WORKER_THREADS="${TEST_WORKER_THREADS:-6}"
TEST_WORKER_SECONDS="${TEST_WORKER_SECONDS:-35}"
TEST_SELECT_EVERY="${TEST_SELECT_EVERY:-3}"
TEST_PAYLOAD_BYTES="${TEST_PAYLOAD_BYTES:-128}"
TEST_LOCK_HOLD_SECONDS="${TEST_LOCK_HOLD_SECONDS:-15}"
TEST_LOCK_WAITER_COUNT="${TEST_LOCK_WAITER_COUNT:-4}"

export MYSQL_PWD="$DB_PASS"

mysql_cmd=(
  mariadb
  --host="$DB_HOST"
  --port="$DB_PORT"
  --user="$DB_USER"
  --ssl
  --ssl-verify-server-cert=0
)

run_sql() {
  local sql_text="$1"
  "${mysql_cmd[@]}" --batch --raw --execute "$sql_text"
}

last_pid=""

start_proc_bg() {
  local sql_text="$1"
  "${mysql_cmd[@]}" --batch --raw --execute "$sql_text" >/dev/null &
  last_pid="$!"
}

echo "==> Seeding PI test workload"
run_sql "CALL ${PI_SCHEMA}.pi_test_seed_workload(${TEST_SEED_ROWS});"

echo
echo "==> Starting write and read workers"
worker_pids=()
for worker_id in $(seq 1 "$TEST_WORKER_THREADS"); do
  start_proc_bg "CALL ${PI_SCHEMA}.pi_test_write_worker_seconds(${TEST_WORKER_SECONDS}, ${worker_id}, ${TEST_SELECT_EVERY}, ${TEST_PAYLOAD_BYTES});"
  worker_pids+=("$last_pid")
done

sleep 3

echo
echo "==> Starting lock holder"
start_proc_bg "CALL ${PI_SCHEMA}.pi_test_lock_holder(${TEST_LOCK_HOLD_SECONDS});"
lock_holder_pid="$last_pid"

sleep 2

echo
echo "==> Starting lock waiters"
lock_waiter_pids=()
for waiter_id in $(seq 1 "$TEST_LOCK_WAITER_COUNT"); do
  start_proc_bg "CALL ${PI_SCHEMA}.pi_test_lock_waiter(${waiter_id});"
  lock_waiter_pids+=("$last_pid")
done

sleep 2

echo
echo "==> Forcing capture while lock waiters are blocked"
run_sql "CALL ${PI_SCHEMA}.pi_capture_sample();"

sleep 8

echo
echo "==> Forcing second capture before the lock holder commits"
run_sql "CALL ${PI_SCHEMA}.pi_capture_sample();"

wait "$lock_holder_pid"

for pid in "${lock_waiter_pids[@]}"; do
  wait "$pid"
done

for pid in "${worker_pids[@]}"; do
  wait "$pid"
done

echo
echo "==> Final capture after workload completion"
run_sql "CALL ${PI_SCHEMA}.pi_capture_sample();"

echo
echo "==> PI-specific load generation complete"
