# Performance Insights Lite For MariaDB And SkySQL

SQL-native lightweight performance insights for MariaDB, aimed first at SkySQL users.

PI-Lite provides a practical approximation of core database performance-insights workflows without requiring a separate service layer. It captures top SQL by load, load by wait class, SQL-to-wait attribution, and short-term rollups that make recent behavior easier to analyze.

## What It Gives You

- top SQL by load from statement digest deltas
- DB load by wait class such as `lock`, `io`, and `synch`
- DB load by user, host, and schema
- SQL-to-wait attribution for blocking-style investigation
- short-term `10s` and `1m` rollups for recent historical analysis

Everything here runs with SQL plus the `mariadb` CLI. The included test harness is self-contained and intended to validate the feature end to end on a real MariaDB or SkySQL target.

## Who This Is For

- SkySQL users who want lightweight SQL-native observability
- MariaDB operators who need recent top-SQL and wait analysis
- engineers evaluating how far `performance_schema` can go without external tooling

## Quick Start

### 1. Verify prerequisites

PI-Lite requires:

- `performance_schema=ON`
- `event_scheduler=ON`
- an existing target schema, defaulting to `sky`
- the `mariadb` client available locally

Check the server settings:

```sql
SELECT @@performance_schema, @@event_scheduler;
```

### 2. Create a local `.env`

Copy the example file and fill in your target connection details:

```bash
cp .env.example .env
```

Required values:

- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASS`

Optional values used by the test harness:

- `PI_SCHEMA`
- `TEST_SEED_ROWS`
- `TEST_WORKER_THREADS`
- `TEST_WORKER_SECONDS`
- `TEST_SELECT_EVERY`
- `TEST_PAYLOAD_BYTES`
- `TEST_LOCK_HOLD_SECONDS`
- `TEST_LOCK_WAITER_COUNT`

Do not commit the local `.env`.

### 3. Install PI-Lite

From this directory:

```bash
set -a
. ./.env
set +a

mariadb \
  --host="$DB_HOST" \
  --port="$DB_PORT" \
  --user="$DB_USER" \
  --ssl \
  --ssl-verify-server-cert=0 < perf_insights.sql
```

### 4. Query the views

After installation, start with:

```sql
SELECT * FROM sky.pi_v_top_sql_5m LIMIT 10;
SELECT * FROM sky.pi_v_dbload_by_waitclass_5m;
SELECT * FROM sky.pi_v_top_sql_wait_5m LIMIT 10;
SELECT * FROM sky.pi_v_top_sql_blocking_5m LIMIT 10;
SELECT * FROM sky.pi_v_top_sql_1h LIMIT 10;
```

## Important Operational Notes

For replicated or MaxScale-fronted deployments:

- install and validate this per backend node, not only through a load-balanced service
- on `read_only` replicas, collector procedures intentionally no-op before writes
- events may replicate, but the write-heavy procedures skip execution on read-only nodes
- do not assume a round-robin path represents one stable backend during troubleshooting

## Required Privileges

Typical install privileges include:

- `CREATE`
- `ALTER`
- `DROP`
- `EVENT`
- `EXECUTE`
- `CREATE ROUTINE`
- `ALTER ROUTINE`
- `CREATE VIEW`
- `SHOW VIEW`
- `SELECT` and `UPDATE` on required `performance_schema` objects

The tested setup also needed access to:

- `performance_schema.events_statements_summary_by_digest`
- `performance_schema.events_waits_current`
- `performance_schema.threads`
- `performance_schema.setup_consumers`
- `performance_schema.setup_instruments`

## What Gets Installed

All persistent objects live in schema `sky` and use the `pi_` prefix.

Main tables:

- `sky.pi_config`
- `sky.pi_runtime_state`
- `sky.pi_digest_dict`
- `sky.pi_digest_snapshot`
- `sky.pi_digest_rollup_10s`
- `sky.pi_dim_rollup_10s`
- `sky.pi_sql_wait_rollup_10s`
- `sky.pi_digest_rollup_1m`
- `sky.pi_dim_rollup_1m`
- `sky.pi_sql_wait_rollup_1m`

Main procedures:

- `sky.pi_capture_sample`
- `sky.pi_downsample_1m`
- `sky.pi_cleanup_retention`
- `sky.pi_pause`
- `sky.pi_resume`
- `sky.pi_uninstall`

Main events:

- `sky.pi_ev_capture_sample`
- `sky.pi_ev_downsample_1m`
- `sky.pi_ev_cleanup_retention`

## Default Operating Profile

The default profile is intentionally conservative:

- sampling every `10` seconds
- `60` minutes retention for `10s` rollups
- `6` hours retention for `1m` rollups
- top `25` digests per sample window
- wait-class capture enabled
- SQL-to-wait attribution enabled

These values are stored in `sky.pi_config`.

Example tuning:

```sql
UPDATE sky.pi_config SET v='15' WHERE k='sample_window_seconds';
UPDATE sky.pi_config SET v='50' WHERE k='topn_digests_per_window';
UPDATE sky.pi_config SET v='0'  WHERE k='capture_sql_wait_attribution';
```

## Test Harness

This demo includes a self-contained validation harness:

- `pi_test_objects.sql`
- `generate_pi_load.sh`
- `run_perf_insights_test.sh`

What it validates:

- install succeeds cleanly
- events are created and enabled
- top-SQL rollups populate
- wait-class rollups populate
- SQL wait attribution populates
- blocking-style lock attribution appears
- `1m` rollups populate after the next minute boundary

Run it:

```bash
chmod +x generate_pi_load.sh run_perf_insights_test.sh
./run_perf_insights_test.sh
```

## Tested Outcome

The latest self-contained validation run on SkySQL populated:

- `pi_digest_rollup_10s`
- `pi_dim_rollup_10s`
- `pi_sql_wait_rollup_10s`
- `pi_digest_rollup_1m`
- `pi_dim_rollup_1m`
- `pi_sql_wait_rollup_1m`
- lock wait attribution rows

That confirms the core end-to-end PI-style use cases:

- top SQL
- load by wait class
- SQL wait attribution
- blocking attribution
- minute-rollup history

## Overhead And Tuning Notes

The `10s` interval is a reasonable compromise for a SQL-native collector. During testing, the observed overhead was about 1 percent additional CPU utilization on a `2vcpu` MariaDB server.

The most expensive paths are:

- digest-summary scanning and grouping from `performance_schema.events_statements_summary_by_digest`
- SQL-to-wait attribution from current waits plus current statements

If you need to reduce overhead, the first knobs to adjust are:

1. increase `sample_window_seconds`
2. reduce `topn_digests_per_window`
3. disable `capture_sql_wait_attribution`

## SkySQL And MariaDB Notes

During live testing, a few MariaDB-specific adjustments were needed:

- the installer was flattened into a top-level SQL script because MariaDB restricts some event and routine DDL inside stored routines
- `events_statements_summary_by_digest` rows were aggregated by `DIGEST` for correctness on the tested build
- `events_statements_current.DIGEST` can be `NULL` for active routine statements, so SQL wait attribution falls back to a synthetic SQL fingerprint
- some blocking row-lock behavior surfaced as `wait/io/table/sql/handler`, which PI-Lite maps to `lock` when it clearly represents blocked DML

This project was tested against SkySQL and MariaDB `10.11.6-MariaDB-log` and is expected to work on newer compatible releases.

## Operational Commands

Pause collection:

```sql
CALL sky.pi_pause();
```

Resume collection:

```sql
CALL sky.pi_resume();
```

Remove PI-Lite data objects:

```sql
CALL sky.pi_uninstall();
```

On MariaDB, `pi_uninstall()` leaves helper procedures installed for compatibility reasons while removing the data tables, events, and views.
