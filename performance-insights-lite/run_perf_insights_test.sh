#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ ! -f "$SCRIPT_DIR/.env" ]]; then
  echo "Missing $SCRIPT_DIR/.env"
  exit 1
fi

set -a
. "$SCRIPT_DIR/.env"
set +a

: "${DB_HOST:?Set DB_HOST in .env}"
: "${DB_PORT:?Set DB_PORT in .env}"
: "${DB_USER:?Set DB_USER in .env}"
: "${DB_PASS:?Set DB_PASS in .env}"

PI_SCHEMA="${PI_SCHEMA:-sky}"

export MYSQL_PWD="$DB_PASS"

mysql_cmd=(
  mariadb
  --host="$DB_HOST"
  --port="$DB_PORT"
  --user="$DB_USER"
  --ssl
  --ssl-verify-server-cert=0
)

run_sql_file() {
  local sql_file="$1"
  echo
  echo "==> Running $sql_file"
  "${mysql_cmd[@]}" < "$sql_file"
}

run_sql() {
  local sql_text="$1"
  "${mysql_cmd[@]}" --batch --raw --execute "$sql_text"
}

run_scalar() {
  local sql_text="$1"
  "${mysql_cmd[@]}" --batch --raw --skip-column-names --execute "$sql_text"
}

assert_positive() {
  local value="$1"
  local label="$2"
  if [[ "$value" -le 0 ]]; then
    echo
    echo "Assertion failed: expected $label to be > 0, got $value"
    exit 1
  fi
}

echo "==> Verifying server prerequisites"
run_sql "SELECT @@version AS version, @@performance_schema AS performance_schema, @@event_scheduler AS event_scheduler;"

perf_schema_state="$(run_scalar "SELECT LOWER(CAST(@@performance_schema AS CHAR));")"
event_scheduler_state="$(run_scalar "SELECT LOWER(CAST(@@event_scheduler AS CHAR));")"

if [[ "$perf_schema_state" != "1" && "$perf_schema_state" != "on" ]]; then
  echo
  echo "PI-LITE cannot be tested here because performance_schema is OFF."
  exit 2
fi

if [[ "$event_scheduler_state" != "1" && "$event_scheduler_state" != "on" ]]; then
  echo
  echo "PI-LITE cannot be tested here because event_scheduler is OFF."
  exit 2
fi

echo
echo "==> Cleaning previous PI-LITE objects if uninstall helper exists"
if [[ "$(run_scalar "SELECT COUNT(*) FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA='${PI_SCHEMA}' AND ROUTINE_NAME='pi_uninstall' AND ROUTINE_TYPE='PROCEDURE';")" -gt 0 ]]; then
  run_sql "CALL ${PI_SCHEMA}.pi_uninstall();"
fi

run_sql_file "$SCRIPT_DIR/perf_insights.sql"
run_sql_file "$SCRIPT_DIR/pi_test_objects.sql"

echo
echo "==> Resetting PI-LITE data before workload"
run_sql "CALL ${PI_SCHEMA}.pi_pause();"
run_sql "
TRUNCATE TABLE ${PI_SCHEMA}.pi_digest_rollup_10s;
TRUNCATE TABLE ${PI_SCHEMA}.pi_dim_rollup_10s;
TRUNCATE TABLE ${PI_SCHEMA}.pi_sql_wait_rollup_10s;
TRUNCATE TABLE ${PI_SCHEMA}.pi_digest_rollup_1m;
TRUNCATE TABLE ${PI_SCHEMA}.pi_dim_rollup_1m;
TRUNCATE TABLE ${PI_SCHEMA}.pi_sql_wait_rollup_1m;
TRUNCATE TABLE ${PI_SCHEMA}.pi_digest_snapshot;
TRUNCATE TABLE ${PI_SCHEMA}.pi_digest_dict;
UPDATE ${PI_SCHEMA}.pi_runtime_state SET v='0' WHERE k IN ('capture_lock', 'downsample_lock', 'cleanup_lock');
"
run_sql "CALL ${PI_SCHEMA}.pi_resume();"

echo
echo "==> Generating PI-focused load"
bash "$SCRIPT_DIR/generate_pi_load.sh"

echo
echo "==> Waiting for next minute boundary so 1m downsample has data"
current_second="$(date +%S)"
wait_seconds=$((62 - 10#$current_second))
if [[ "$wait_seconds" -lt 5 ]]; then
  wait_seconds=$((wait_seconds + 60))
fi
echo "Sleeping ${wait_seconds}s to move into the next downsample window"
sleep "$wait_seconds"

echo
echo "==> Forcing minute rollup and cleanup"
run_sql "CALL ${PI_SCHEMA}.pi_downsample_1m();"
run_sql "CALL ${PI_SCHEMA}.pi_cleanup_retention();"

echo
echo "==> Inspecting PI-LITE events"
run_sql "
SELECT EVENT_NAME, STATUS, LAST_EXECUTED, INTERVAL_VALUE, INTERVAL_FIELD
  FROM information_schema.EVENTS
 WHERE EVENT_SCHEMA = '${PI_SCHEMA}'
   AND EVENT_NAME LIKE 'pi_ev_%'
 ORDER BY EVENT_NAME;
"

echo
echo "==> Inspecting PI-LITE table population"
run_sql "
SELECT 'pi_digest_rollup_10s' AS object_name, COUNT(*) AS row_count FROM ${PI_SCHEMA}.pi_digest_rollup_10s
UNION ALL
SELECT 'pi_dim_rollup_10s', COUNT(*) FROM ${PI_SCHEMA}.pi_dim_rollup_10s
UNION ALL
SELECT 'pi_sql_wait_rollup_10s', COUNT(*) FROM ${PI_SCHEMA}.pi_sql_wait_rollup_10s
UNION ALL
SELECT 'pi_digest_rollup_1m', COUNT(*) FROM ${PI_SCHEMA}.pi_digest_rollup_1m
UNION ALL
SELECT 'pi_dim_rollup_1m', COUNT(*) FROM ${PI_SCHEMA}.pi_dim_rollup_1m
UNION ALL
SELECT 'pi_sql_wait_rollup_1m', COUNT(*) FROM ${PI_SCHEMA}.pi_sql_wait_rollup_1m;
"

echo
echo "==> Sample rows from key PI views"
run_sql "
SELECT ts_end, digest, ROUND(aas, 4) AS aas
  FROM ${PI_SCHEMA}.pi_v_top_sql_5m
 ORDER BY ts_end DESC, aas DESC
 LIMIT 10;
"

run_sql "
SELECT ts_end, waitclass, ROUND(aas, 4) AS aas
  FROM ${PI_SCHEMA}.pi_v_dbload_by_waitclass_5m
 ORDER BY ts_end DESC, aas DESC
 LIMIT 10;
"

run_sql "
SELECT ts_end, waitclass, digest, ROUND(aas, 4) AS aas
  FROM ${PI_SCHEMA}.pi_v_top_sql_wait_5m
 ORDER BY ts_end DESC, aas DESC
 LIMIT 10;
"

digest_10s_count="$(run_scalar "SELECT COUNT(*) FROM ${PI_SCHEMA}.pi_digest_rollup_10s;")"
dim_10s_count="$(run_scalar "SELECT COUNT(*) FROM ${PI_SCHEMA}.pi_dim_rollup_10s;")"
sql_wait_10s_count="$(run_scalar "SELECT COUNT(*) FROM ${PI_SCHEMA}.pi_sql_wait_rollup_10s;")"
digest_1m_count="$(run_scalar "SELECT COUNT(*) FROM ${PI_SCHEMA}.pi_digest_rollup_1m;")"
dim_1m_count="$(run_scalar "SELECT COUNT(*) FROM ${PI_SCHEMA}.pi_dim_rollup_1m;")"
lock_wait_rows="$(run_scalar "SELECT COUNT(*) FROM ${PI_SCHEMA}.pi_sql_wait_rollup_10s WHERE waitclass='lock';")"

assert_positive "$digest_10s_count" "pi_digest_rollup_10s"
assert_positive "$dim_10s_count" "pi_dim_rollup_10s"
assert_positive "$sql_wait_10s_count" "pi_sql_wait_rollup_10s"
assert_positive "$digest_1m_count" "pi_digest_rollup_1m"
assert_positive "$dim_1m_count" "pi_dim_rollup_1m"
assert_positive "$lock_wait_rows" "lock wait attribution rows"

echo
echo "==> Logical PI checks passed"
echo "Top SQL captured: ${digest_10s_count} rows"
echo "Load dimensions captured: ${dim_10s_count} rows"
echo "SQL wait attribution captured: ${sql_wait_10s_count} rows"
echo "1m digest rollups captured: ${digest_1m_count} rows"
echo "1m dimension rollups captured: ${dim_1m_count} rows"
echo "Lock wait attribution rows: ${lock_wait_rows}"
