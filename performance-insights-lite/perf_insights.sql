-- ============================================================================
-- PI-LITE FOR MARIADB / MYSQL-FAMILY
-- SQL-native "Performance Insights-lite"
--
-- Default profile:
--   - low overhead
--   - 10s sampling
--   - 60m retention for 10s data
--   - 6h retention for 1m data
--   - top 25 digests per window
--   - wait classes only by default
--   - sampled SQL->wait attribution enabled
--
-- What it provides:
--   1) Top SQL by DB load (AAS)           via statement digest deltas
--   2) Wait events by type/class          via sampled current waits
--   3) Top SQL by wait load/blocking      via sampled wait attribution to digest
--   4) DB load by user / host / schema    via active thread sampling
--   5) CPU-ish vs wait-ish                via total AAS minus wait AAS
--
-- Production notes:
--   - Run this independently on each node: primary and replicas
--   - performance_schema is a hard prerequisite and must already be ON
--   - This script does NOT attempt to enable performance_schema at runtime
--   - event_scheduler must already be ON; this script will fail cleanly if not
--   - If required privileges are missing, install aborts with a phase-specific
--     error rather than silently continuing with a partial setup
--   - Typical install privileges: CREATE/ALTER/DROP/EVENT/EXECUTE on schema `sky`
--     plus SELECT/UPDATE access to the required performance_schema tables
--   - No schema is created by this script; it installs into existing schema `sky`
--   - All persistent objects use the `pi_` prefix inside schema `sky`
-- ============================================================================

USE sky;

DELIMITER //

DROP PROCEDURE IF EXISTS sky.pi_preflight //
CREATE PROCEDURE sky.pi_preflight()
BEGIN
  DECLARE v_perf_schema VARCHAR(8) DEFAULT 'off';
  DECLARE v_event_scheduler VARCHAR(8) DEFAULT 'off';
  DECLARE v_phase VARCHAR(64) DEFAULT 'startup';
  DECLARE v_sqlstate CHAR(5) DEFAULT '00000';
  DECLARE v_errno INT DEFAULT 0;
  DECLARE v_detail TEXT DEFAULT '';
  DECLARE v_error_text VARCHAR(128) DEFAULT '';
  DECLARE v_required_count INT DEFAULT 0;
  DECLARE v_enabled_count INT DEFAULT 0;

  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    GET DIAGNOSTICS CONDITION 1
      v_sqlstate = RETURNED_SQLSTATE,
      v_errno = MYSQL_ERRNO,
      v_detail = MESSAGE_TEXT;

    SET v_error_text = LEFT(
      CONCAT(
        'PI-LITE install failed during ',
        v_phase,
        '. SQLSTATE=',
        IFNULL(v_sqlstate, '00000'),
        ', errno=',
        IFNULL(CAST(v_errno AS CHAR), '0'),
        ', detail=',
        IFNULL(v_detail, 'unknown error')
      ),
      128
    );

    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = v_error_text;
  END;

  -- --------------------------------------------------------------------------
  -- 1) Hard prerequisites
  -- --------------------------------------------------------------------------
  SET v_phase = 'validate prerequisites';

  SELECT LOWER(CAST(@@performance_schema AS CHAR)) INTO v_perf_schema;
  IF v_perf_schema NOT IN ('1', 'on') THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'PI-LITE install aborted: performance_schema is OFF. Enable it in config, restart, and rerun.';
  END IF;

  SELECT LOWER(CAST(@@event_scheduler AS CHAR)) INTO v_event_scheduler;
  IF v_event_scheduler NOT IN ('1', 'on') THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'PI-LITE install aborted: event_scheduler is OFF. Turn it ON manually, then rerun.';
  END IF;

  SELECT COUNT(*)
    INTO v_required_count
    FROM information_schema.tables
   WHERE table_schema = 'performance_schema'
     AND table_name IN (
       'events_statements_current',
       'events_statements_summary_by_digest',
       'events_waits_current',
       'threads',
       'setup_consumers',
       'setup_instruments'
     );

  IF v_required_count <> 6 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'PI-LITE install aborted: required performance_schema tables are missing.';
  END IF;

  SELECT COUNT(*)
    INTO v_required_count
    FROM performance_schema.setup_consumers
   WHERE NAME IN (
     'events_statements_current',
     'events_waits_current'
   );

  IF v_required_count <> 2 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'PI-LITE install aborted: required performance_schema consumers are unavailable.';
  END IF;

  SELECT COUNT(*)
    INTO v_required_count
    FROM performance_schema.setup_instruments
   WHERE NAME LIKE 'statement/sql/%';

  IF v_required_count = 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'PI-LITE install aborted: required statement instrumentation is unavailable.';
  END IF;

  SELECT COUNT(*)
    INTO v_required_count
    FROM performance_schema.setup_instruments
   WHERE NAME LIKE 'wait/%';

  IF v_required_count = 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'PI-LITE install aborted: required wait instrumentation is unavailable.';
  END IF;

  -- --------------------------------------------------------------------------
  -- 2) Enable minimal required performance_schema consumers/instruments
  -- --------------------------------------------------------------------------
  SET v_phase = 'enable performance_schema consumers';

  UPDATE performance_schema.setup_consumers
     SET ENABLED = 'YES'
   WHERE NAME IN (
     'events_statements_current',
     'events_statements_summary_by_digest',
     'events_waits_current'
   );

  SELECT COUNT(*)
    INTO v_enabled_count
    FROM performance_schema.setup_consumers
   WHERE NAME IN (
     'events_statements_current',
     'events_waits_current'
   )
     AND ENABLED = 'YES';

  IF v_enabled_count <> 2 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'PI-LITE install aborted: unable to enable required performance_schema consumers.';
  END IF;

  SET v_phase = 'enable performance_schema instruments';

  UPDATE performance_schema.setup_instruments
     SET ENABLED = 'YES', TIMED = 'YES'
   WHERE NAME IN (
     'statement/sql/select',
     'statement/sql/insert',
     'statement/sql/update',
     'statement/sql/delete',
     'statement/sql/replace',
     'statement/sql/call'
   );

  UPDATE performance_schema.setup_instruments
     SET ENABLED = 'YES', TIMED = 'YES'
   WHERE NAME LIKE 'wait/io/%';

  UPDATE performance_schema.setup_instruments
     SET ENABLED = 'YES', TIMED = 'YES'
   WHERE NAME LIKE 'wait/lock/%';

  UPDATE performance_schema.setup_instruments
     SET ENABLED = 'YES', TIMED = 'YES'
   WHERE NAME LIKE 'wait/synch/%';

  UPDATE performance_schema.setup_instruments
     SET ENABLED = 'YES', TIMED = 'YES'
   WHERE NAME LIKE 'wait/mutex/%';

END //

CALL sky.pi_preflight() //
DROP PROCEDURE IF EXISTS sky.pi_preflight //

DELIMITER ;

-- --------------------------------------------------------------------------
-- 3) Metadata, config, and runtime tables
-- --------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS sky.pi_config (
    k VARCHAR(64) PRIMARY KEY,
    v VARCHAR(255) NOT NULL
 ) ENGINE=InnoDB;

INSERT INTO sky.pi_config(k, v) VALUES
  ('profile', 'light'),
  ('sample_window_seconds', '10'),
  ('topn_digests_per_window', '25'),
  ('retention_10s_minutes', '60'),
  ('retention_1m_hours', '6'),
  ('capture_wait_event_names', '0'),
  ('capture_sql_wait_attribution', '1'),
  ('capture_host_dimension', '1'),
  ('capture_user_dimension', '1'),
  ('capture_schema_dimension', '1')
ON DUPLICATE KEY UPDATE
  v = VALUES(v);

  CREATE TABLE IF NOT EXISTS sky.pi_runtime_state (
    k VARCHAR(64) PRIMARY KEY,
    v VARCHAR(255) NOT NULL,
    updated_ts DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
      ON UPDATE CURRENT_TIMESTAMP(6)
  ) ENGINE=InnoDB;

  INSERT INTO sky.pi_runtime_state(k, v) VALUES
    ('capture_lock', '0'),
    ('downsample_lock', '0'),
    ('cleanup_lock', '0')
  ON DUPLICATE KEY UPDATE
    v = VALUES(v);

  CREATE TABLE IF NOT EXISTS sky.pi_digest_dict (
    digest VARCHAR(64) PRIMARY KEY,
    digest_text TEXT NOT NULL,
    first_seen_ts DATETIME(6) NOT NULL,
    last_seen_ts DATETIME(6) NOT NULL
  ) ENGINE=InnoDB;

  CREATE TABLE IF NOT EXISTS sky.pi_digest_snapshot (
    digest VARCHAR(64) PRIMARY KEY,
    sum_timer_wait BIGINT UNSIGNED NOT NULL,
    count_star BIGINT UNSIGNED NOT NULL,
    last_seen_ts DATETIME(6) NOT NULL
  ) ENGINE=InnoDB;

  CREATE TABLE IF NOT EXISTS sky.pi_digest_rollup_10s (
    ts_start DATETIME(6) NOT NULL,
    ts_end DATETIME(6) NOT NULL,
    window_sec INT NOT NULL,
    server_id INT NOT NULL,
    host_name VARCHAR(255) NOT NULL,
    node_role ENUM('primary', 'replica', 'unknown') NOT NULL,
    digest VARCHAR(64) NOT NULL,
    delta_exec BIGINT NOT NULL,
    delta_time_sec DOUBLE NOT NULL,
    aas DOUBLE NOT NULL,
    PRIMARY KEY (ts_end, server_id, digest),
    KEY idx_digest_rollup_10s_ts (ts_end),
    KEY idx_digest_rollup_10s_aas (ts_end, aas),
    KEY idx_digest_rollup_10s_digest (digest)
  ) ENGINE=InnoDB;

  CREATE TABLE IF NOT EXISTS sky.pi_dim_rollup_10s (
    ts_end DATETIME(6) NOT NULL,
    window_sec INT NOT NULL,
    server_id INT NOT NULL,
    host_name VARCHAR(255) NOT NULL,
    node_role ENUM('primary', 'replica', 'unknown') NOT NULL,
    dim_type ENUM('waitclass', 'user', 'host', 'schema') NOT NULL,
    dim_value VARCHAR(255) NOT NULL,
    active_count INT NOT NULL,
    aas DOUBLE NOT NULL,
    PRIMARY KEY (ts_end, server_id, dim_type, dim_value),
    KEY idx_dim_rollup_10s_ts (ts_end),
    KEY idx_dim_rollup_10s_dim (dim_type, ts_end)
  ) ENGINE=InnoDB;

  CREATE TABLE IF NOT EXISTS sky.pi_sql_wait_rollup_10s (
    ts_end DATETIME(6) NOT NULL,
    window_sec INT NOT NULL,
    server_id INT NOT NULL,
    host_name VARCHAR(255) NOT NULL,
    node_role ENUM('primary', 'replica', 'unknown') NOT NULL,
    digest VARCHAR(64) NOT NULL,
    waitclass VARCHAR(32) NOT NULL,
    sampled_count INT NOT NULL,
    aas DOUBLE NOT NULL,
    PRIMARY KEY (ts_end, server_id, digest, waitclass),
    KEY idx_sql_wait_rollup_10s_ts (ts_end),
    KEY idx_sql_wait_rollup_10s_digest (digest, ts_end)
  ) ENGINE=InnoDB;

  CREATE TABLE IF NOT EXISTS sky.pi_digest_rollup_1m (
    ts_minute DATETIME NOT NULL,
    server_id INT NOT NULL,
    host_name VARCHAR(255) NOT NULL,
    node_role ENUM('primary', 'replica', 'unknown') NOT NULL,
    digest VARCHAR(64) NOT NULL,
    total_exec BIGINT NOT NULL,
    total_time_sec DOUBLE NOT NULL,
    avg_aas DOUBLE NOT NULL,
    PRIMARY KEY (ts_minute, server_id, digest),
    KEY idx_digest_rollup_1m_ts (ts_minute),
    KEY idx_digest_rollup_1m_aas (ts_minute, avg_aas)
  ) ENGINE=InnoDB;

  CREATE TABLE IF NOT EXISTS sky.pi_dim_rollup_1m (
    ts_minute DATETIME NOT NULL,
    server_id INT NOT NULL,
    host_name VARCHAR(255) NOT NULL,
    node_role ENUM('primary', 'replica', 'unknown') NOT NULL,
    dim_type ENUM('waitclass', 'user', 'host', 'schema') NOT NULL,
    dim_value VARCHAR(255) NOT NULL,
    total_active_count BIGINT NOT NULL,
    avg_aas DOUBLE NOT NULL,
    PRIMARY KEY (ts_minute, server_id, dim_type, dim_value),
    KEY idx_dim_rollup_1m_ts (ts_minute),
    KEY idx_dim_rollup_1m_dim (dim_type, ts_minute)
  ) ENGINE=InnoDB;

  CREATE TABLE IF NOT EXISTS sky.pi_sql_wait_rollup_1m (
    ts_minute DATETIME NOT NULL,
    server_id INT NOT NULL,
    host_name VARCHAR(255) NOT NULL,
    node_role ENUM('primary', 'replica', 'unknown') NOT NULL,
    digest VARCHAR(64) NOT NULL,
    waitclass VARCHAR(32) NOT NULL,
    total_sampled_count BIGINT NOT NULL,
    avg_aas DOUBLE NOT NULL,
    PRIMARY KEY (ts_minute, server_id, digest, waitclass),
    KEY idx_sql_wait_rollup_1m_ts (ts_minute),
    KEY idx_sql_wait_rollup_1m_digest (digest, ts_minute)
  ) ENGINE=InnoDB;

-- --------------------------------------------------------------------------
-- 4) Capture procedure: digest deltas + dimension sampling + SQL wait sampling
-- --------------------------------------------------------------------------

  SET @capture_sql = '
  CREATE OR REPLACE PROCEDURE sky.pi_capture_sample()
  proc: BEGIN
    DECLARE v_window INT DEFAULT 10;
    DECLARE v_topn INT DEFAULT 25;
    DECLARE v_capture_sql_wait INT DEFAULT 1;
    DECLARE v_capture_user INT DEFAULT 1;
    DECLARE v_capture_host INT DEFAULT 1;
    DECLARE v_capture_schema INT DEFAULT 1;

    DECLARE v_server_id INT DEFAULT @@server_id;
    DECLARE v_host_name VARCHAR(255) DEFAULT @@hostname;
    DECLARE v_role VARCHAR(16) DEFAULT IF(@@read_only = 1, ''replica'', ''primary'');
    DECLARE v_is_read_only TINYINT DEFAULT @@read_only;
    DECLARE v_ts_end DATETIME(6) DEFAULT NOW(6);
    DECLARE v_ts_start DATETIME(6);
    DECLARE v_lock_val VARCHAR(16) DEFAULT ''0'';

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
      UPDATE sky.pi_runtime_state
         SET v = ''0''
       WHERE k = ''capture_lock'';
      RESIGNAL;
    END;

    IF v_is_read_only = 1 THEN
      LEAVE proc;
    END IF;

    SELECT v INTO v_window
      FROM sky.pi_config
     WHERE k = ''sample_window_seconds''
     LIMIT 1;

    SELECT v INTO v_topn
      FROM sky.pi_config
     WHERE k = ''topn_digests_per_window''
     LIMIT 1;

    SELECT v INTO v_capture_sql_wait
      FROM sky.pi_config
     WHERE k = ''capture_sql_wait_attribution''
     LIMIT 1;

    SELECT v INTO v_capture_user
      FROM sky.pi_config
     WHERE k = ''capture_user_dimension''
     LIMIT 1;

    SELECT v INTO v_capture_host
      FROM sky.pi_config
     WHERE k = ''capture_host_dimension''
     LIMIT 1;

    SELECT v INTO v_capture_schema
      FROM sky.pi_config
     WHERE k = ''capture_schema_dimension''
     LIMIT 1;

    SET v_ts_start = v_ts_end - INTERVAL v_window SECOND;

    START TRANSACTION;
      SELECT v
        INTO v_lock_val
        FROM sky.pi_runtime_state
       WHERE k = ''capture_lock''
       FOR UPDATE;

      IF v_lock_val = ''1'' THEN
        ROLLBACK;
        LEAVE proc;
      END IF;

      UPDATE sky.pi_runtime_state
         SET v = ''1''
       WHERE k = ''capture_lock'';
    COMMIT;

    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_digest_cur (
      digest VARCHAR(64) PRIMARY KEY,
      digest_text TEXT,
      sum_timer_wait BIGINT UNSIGNED,
      count_star BIGINT UNSIGNED
    ) ENGINE=InnoDB;

    TRUNCATE TABLE tmp_digest_cur;

    INSERT INTO tmp_digest_cur(digest, digest_text, sum_timer_wait, count_star)
    SELECT
      DIGEST,
      MAX(DIGEST_TEXT) AS digest_text,
      SUM(SUM_TIMER_WAIT) AS sum_timer_wait,
      SUM(COUNT_STAR) AS count_star
      FROM performance_schema.events_statements_summary_by_digest
     WHERE DIGEST IS NOT NULL
     GROUP BY DIGEST;

    INSERT INTO sky.pi_digest_dict(digest, digest_text, first_seen_ts, last_seen_ts)
    SELECT c.digest, c.digest_text, v_ts_end, v_ts_end
      FROM tmp_digest_cur c
      LEFT JOIN sky.pi_digest_dict d
        ON d.digest = c.digest
     WHERE d.digest IS NULL
    ON DUPLICATE KEY UPDATE
      digest_text = VALUES(digest_text),
      last_seen_ts = VALUES(last_seen_ts);

    UPDATE sky.pi_digest_dict d
    JOIN (
      SELECT digest, MAX(digest_text) AS digest_text
        FROM tmp_digest_cur
       GROUP BY digest
    ) c
      ON c.digest = d.digest
       SET d.last_seen_ts = v_ts_end
     WHERE d.last_seen_ts < v_ts_end;

    INSERT INTO sky.pi_digest_rollup_10s(
      ts_start, ts_end, window_sec, server_id, host_name, node_role,
      digest, delta_exec, delta_time_sec, aas
    )
    SELECT
      v_ts_start,
      v_ts_end,
      v_window,
      v_server_id,
      v_host_name,
      v_role,
      c.digest,
      GREATEST(0, CAST(c.count_star AS SIGNED) - CAST(IFNULL(s.count_star, 0) AS SIGNED)) AS delta_exec,
      GREATEST(0, (c.sum_timer_wait - IFNULL(s.sum_timer_wait, 0)) / 1e12) AS delta_time_sec,
      GREATEST(0, (c.sum_timer_wait - IFNULL(s.sum_timer_wait, 0)) / 1e12) / v_window AS aas
    FROM tmp_digest_cur c
    LEFT JOIN sky.pi_digest_snapshot s
      ON s.digest = c.digest
    ORDER BY (c.sum_timer_wait - IFNULL(s.sum_timer_wait, 0)) DESC
    LIMIT v_topn;

    INSERT INTO sky.pi_digest_snapshot(digest, sum_timer_wait, count_star, last_seen_ts)
    SELECT digest, sum_timer_wait, count_star, v_ts_end
      FROM tmp_digest_cur
    ON DUPLICATE KEY UPDATE
      sum_timer_wait = VALUES(sum_timer_wait),
      count_star = VALUES(count_star),
      last_seen_ts = VALUES(last_seen_ts);

    CREATE TEMPORARY TABLE IF NOT EXISTS tmp_active_threads (
      thread_id BIGINT PRIMARY KEY,
      processlist_user VARCHAR(128),
      processlist_host VARCHAR(255),
      processlist_db VARCHAR(128)
    ) ENGINE=InnoDB;

    TRUNCATE TABLE tmp_active_threads;

    INSERT INTO tmp_active_threads(thread_id, processlist_user, processlist_host, processlist_db)
    SELECT
      t.THREAD_ID,
      t.PROCESSLIST_USER,
      t.PROCESSLIST_HOST,
      t.PROCESSLIST_DB
    FROM performance_schema.threads t
    WHERE t.PROCESSLIST_ID IS NOT NULL
      AND t.TYPE = ''FOREGROUND''
      AND IFNULL(t.PROCESSLIST_COMMAND, '''') <> ''Sleep'';

    IF v_capture_user = 1 THEN
      INSERT INTO sky.pi_dim_rollup_10s(
        ts_end, window_sec, server_id, host_name, node_role,
        dim_type, dim_value, active_count, aas
      )
      SELECT
        v_ts_end, v_window, v_server_id, v_host_name, v_role,
        ''user'', IFNULL(processlist_user, ''(null)''), COUNT(*), COUNT(*)
      FROM tmp_active_threads
      GROUP BY IFNULL(processlist_user, ''(null)'');
    END IF;

    IF v_capture_host = 1 THEN
      INSERT INTO sky.pi_dim_rollup_10s(
        ts_end, window_sec, server_id, host_name, node_role,
        dim_type, dim_value, active_count, aas
      )
      SELECT
        v_ts_end, v_window, v_server_id, v_host_name, v_role,
        ''host'', IFNULL(processlist_host, ''(null)''), COUNT(*), COUNT(*)
      FROM tmp_active_threads
      GROUP BY IFNULL(processlist_host, ''(null)'');
    END IF;

    IF v_capture_schema = 1 THEN
      INSERT INTO sky.pi_dim_rollup_10s(
        ts_end, window_sec, server_id, host_name, node_role,
        dim_type, dim_value, active_count, aas
      )
      SELECT
        v_ts_end, v_window, v_server_id, v_host_name, v_role,
        ''schema'', IFNULL(processlist_db, ''(none)''), COUNT(*), COUNT(*)
      FROM tmp_active_threads
      GROUP BY IFNULL(processlist_db, ''(none)'');
    END IF;

    INSERT INTO sky.pi_dim_rollup_10s(
      ts_end, window_sec, server_id, host_name, node_role,
      dim_type, dim_value, active_count, aas
    )
    SELECT
      v_ts_end, v_window, v_server_id, v_host_name, v_role,
      ''waitclass'',
      CASE
        WHEN w.EVENT_NAME LIKE ''wait/lock/%'' THEN ''lock''
        WHEN w.EVENT_NAME = ''wait/io/table/sql/handler''
         AND (
           UPPER(IFNULL(t.PROCESSLIST_STATE, '''')) LIKE ''%UPDATE%''
           OR UPPER(IFNULL(t.PROCESSLIST_INFO, '''')) LIKE ''UPDATE %''
           OR UPPER(IFNULL(t.PROCESSLIST_INFO, '''')) LIKE ''DELETE %''
           OR UPPER(IFNULL(t.PROCESSLIST_INFO, '''')) LIKE ''INSERT %''
         ) THEN ''lock''
        WHEN w.EVENT_NAME LIKE ''wait/io/%'' THEN ''io''
        WHEN w.EVENT_NAME LIKE ''wait/synch/%'' THEN ''synch''
        WHEN w.EVENT_NAME LIKE ''wait/mutex/%'' THEN ''mutex''
        WHEN w.EVENT_NAME LIKE ''wait/%'' THEN ''other''
        ELSE ''none''
      END AS waitclass,
      COUNT(*),
      COUNT(*)
    FROM performance_schema.events_waits_current w
    JOIN tmp_active_threads a
      ON a.thread_id = w.THREAD_ID
    JOIN performance_schema.threads t
      ON t.THREAD_ID = a.thread_id
    GROUP BY waitclass;

    IF v_capture_sql_wait = 1 THEN
      CREATE TEMPORARY TABLE IF NOT EXISTS tmp_sql_wait_cur (
        digest VARCHAR(64) NOT NULL,
        digest_text TEXT NOT NULL,
        waitclass VARCHAR(32) NOT NULL,
        sampled_count INT NOT NULL,
        PRIMARY KEY (digest, waitclass)
      ) ENGINE=InnoDB;

      TRUNCATE TABLE tmp_sql_wait_cur;

      INSERT INTO tmp_sql_wait_cur(
        digest, digest_text, waitclass, sampled_count
      )
      SELECT
        x.digest,
        MAX(x.digest_text) AS digest_text,
        x.waitclass,
        SUM(x.sampled_count) AS sampled_count
      FROM (
        SELECT
          COALESCE(
            NULLIF(esc.DIGEST, ''''),
            MD5(LEFT(COALESCE(t.PROCESSLIST_INFO, esc.SQL_TEXT, esc.EVENT_NAME, w.EVENT_NAME), 1024))
          ) AS digest,
          COALESCE(t.PROCESSLIST_INFO, esc.SQL_TEXT, esc.EVENT_NAME, w.EVENT_NAME) AS digest_text,
          CASE
            WHEN w.EVENT_NAME LIKE ''wait/lock/%'' THEN ''lock''
            WHEN w.EVENT_NAME = ''wait/io/table/sql/handler''
             AND (
               UPPER(IFNULL(t.PROCESSLIST_STATE, '''')) LIKE ''%UPDATE%''
               OR UPPER(IFNULL(t.PROCESSLIST_INFO, '''')) LIKE ''UPDATE %''
               OR UPPER(IFNULL(t.PROCESSLIST_INFO, '''')) LIKE ''DELETE %''
               OR UPPER(IFNULL(t.PROCESSLIST_INFO, '''')) LIKE ''INSERT %''
             ) THEN ''lock''
            WHEN w.EVENT_NAME LIKE ''wait/io/%'' THEN ''io''
            WHEN w.EVENT_NAME LIKE ''wait/synch/%'' THEN ''synch''
            WHEN w.EVENT_NAME LIKE ''wait/mutex/%'' THEN ''mutex''
            WHEN w.EVENT_NAME LIKE ''wait/%'' THEN ''other''
            ELSE ''none''
          END AS waitclass,
          COUNT(*) AS sampled_count
        FROM performance_schema.events_waits_current w
        JOIN tmp_active_threads a
          ON a.thread_id = w.THREAD_ID
        JOIN performance_schema.threads t
          ON t.THREAD_ID = a.thread_id
        LEFT JOIN performance_schema.events_statements_current esc
          ON esc.THREAD_ID = a.thread_id
        WHERE COALESCE(esc.DIGEST, t.PROCESSLIST_INFO, esc.SQL_TEXT, esc.EVENT_NAME) IS NOT NULL
        GROUP BY digest, digest_text, waitclass
      ) x
      GROUP BY x.digest, x.waitclass;

      INSERT INTO sky.pi_digest_dict(digest, digest_text, first_seen_ts, last_seen_ts)
      SELECT
        digest,
        MAX(digest_text) AS digest_text,
        v_ts_end,
        v_ts_end
      FROM tmp_sql_wait_cur
      GROUP BY digest
      ON DUPLICATE KEY UPDATE
        digest_text = COALESCE(NULLIF(VALUES(digest_text), ''''), digest_text),
        last_seen_ts = VALUES(last_seen_ts);

      INSERT INTO sky.pi_sql_wait_rollup_10s(
        ts_end, window_sec, server_id, host_name, node_role,
        digest, waitclass, sampled_count, aas
      )
      SELECT
        v_ts_end,
        v_window,
        v_server_id,
        v_host_name,
        v_role,
        digest,
        waitclass,
        sampled_count,
        sampled_count AS aas
      FROM tmp_sql_wait_cur;
    END IF;

    DROP TEMPORARY TABLE IF EXISTS tmp_sql_wait_cur;
    DROP TEMPORARY TABLE IF EXISTS tmp_active_threads;
    DROP TEMPORARY TABLE IF EXISTS tmp_digest_cur;

    UPDATE sky.pi_runtime_state
       SET v = ''0''
     WHERE k = ''capture_lock'';
  END
  ';
  PREPARE stmt FROM @capture_sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

-- --------------------------------------------------------------------------
-- 5) Downsample procedure
-- --------------------------------------------------------------------------

  SET @downsample_sql = '
  CREATE OR REPLACE PROCEDURE sky.pi_downsample_1m()
  proc: BEGIN
    DECLARE v_lock_val VARCHAR(16) DEFAULT ''0'';
    DECLARE v_is_read_only TINYINT DEFAULT @@read_only;
    DECLARE v_ts_minute DATETIME;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
      UPDATE sky.pi_runtime_state
         SET v = ''0''
       WHERE k = ''downsample_lock'';
      RESIGNAL;
    END;

    IF v_is_read_only = 1 THEN
      LEAVE proc;
    END IF;

    SET v_ts_minute = DATE_FORMAT(NOW() - INTERVAL 1 MINUTE, ''%Y-%m-%d %H:%i:00'');

    START TRANSACTION;
      SELECT v
        INTO v_lock_val
        FROM sky.pi_runtime_state
       WHERE k = ''downsample_lock''
       FOR UPDATE;

      IF v_lock_val = ''1'' THEN
        ROLLBACK;
        LEAVE proc;
      END IF;

      UPDATE sky.pi_runtime_state
         SET v = ''1''
       WHERE k = ''downsample_lock'';
    COMMIT;

    INSERT INTO sky.pi_digest_rollup_1m(
      ts_minute, server_id, host_name, node_role, digest,
      total_exec, total_time_sec, avg_aas
    )
    SELECT
      DATE_FORMAT(ts_end, ''%Y-%m-%d %H:%i:00'') AS ts_minute,
      server_id,
      MAX(host_name),
      MAX(node_role),
      digest,
      SUM(delta_exec),
      SUM(delta_time_sec),
      AVG(aas)
    FROM sky.pi_digest_rollup_10s
    WHERE ts_end >= v_ts_minute
      AND ts_end < v_ts_minute + INTERVAL 1 MINUTE
    GROUP BY DATE_FORMAT(ts_end, ''%Y-%m-%d %H:%i:00''), server_id, digest
    ON DUPLICATE KEY UPDATE
      total_exec = VALUES(total_exec),
      total_time_sec = VALUES(total_time_sec),
      avg_aas = VALUES(avg_aas),
      host_name = VALUES(host_name),
      node_role = VALUES(node_role);

    INSERT INTO sky.pi_dim_rollup_1m(
      ts_minute, server_id, host_name, node_role, dim_type, dim_value,
      total_active_count, avg_aas
    )
    SELECT
      DATE_FORMAT(ts_end, ''%Y-%m-%d %H:%i:00'') AS ts_minute,
      server_id,
      MAX(host_name),
      MAX(node_role),
      dim_type,
      dim_value,
      SUM(active_count),
      AVG(aas)
    FROM sky.pi_dim_rollup_10s
    WHERE ts_end >= v_ts_minute
      AND ts_end < v_ts_minute + INTERVAL 1 MINUTE
    GROUP BY DATE_FORMAT(ts_end, ''%Y-%m-%d %H:%i:00''), server_id, dim_type, dim_value
    ON DUPLICATE KEY UPDATE
      total_active_count = VALUES(total_active_count),
      avg_aas = VALUES(avg_aas),
      host_name = VALUES(host_name),
      node_role = VALUES(node_role);

    INSERT INTO sky.pi_sql_wait_rollup_1m(
      ts_minute, server_id, host_name, node_role, digest, waitclass,
      total_sampled_count, avg_aas
    )
    SELECT
      DATE_FORMAT(ts_end, ''%Y-%m-%d %H:%i:00'') AS ts_minute,
      server_id,
      MAX(host_name),
      MAX(node_role),
      digest,
      waitclass,
      SUM(sampled_count),
      AVG(aas)
    FROM sky.pi_sql_wait_rollup_10s
    WHERE ts_end >= v_ts_minute
      AND ts_end < v_ts_minute + INTERVAL 1 MINUTE
    GROUP BY DATE_FORMAT(ts_end, ''%Y-%m-%d %H:%i:00''), server_id, digest, waitclass
    ON DUPLICATE KEY UPDATE
      total_sampled_count = VALUES(total_sampled_count),
      avg_aas = VALUES(avg_aas),
      host_name = VALUES(host_name),
      node_role = VALUES(node_role);

    UPDATE sky.pi_runtime_state
       SET v = ''0''
     WHERE k = ''downsample_lock'';
  END
  ';
  PREPARE stmt FROM @downsample_sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

-- --------------------------------------------------------------------------
-- 6) Cleanup procedure
-- --------------------------------------------------------------------------

  SET @cleanup_sql = '
  CREATE OR REPLACE PROCEDURE sky.pi_cleanup_retention()
  proc: BEGIN
    DECLARE v_ret_10s INT DEFAULT 60;
    DECLARE v_ret_1m INT DEFAULT 6;
    DECLARE v_lock_val VARCHAR(16) DEFAULT ''0'';
    DECLARE v_is_read_only TINYINT DEFAULT @@read_only;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
      UPDATE sky.pi_runtime_state
         SET v = ''0''
       WHERE k = ''cleanup_lock'';
      RESIGNAL;
    END;

    IF v_is_read_only = 1 THEN
      LEAVE proc;
    END IF;

    SELECT v INTO v_ret_10s
      FROM sky.pi_config
     WHERE k = ''retention_10s_minutes''
     LIMIT 1;

    SELECT v INTO v_ret_1m
      FROM sky.pi_config
     WHERE k = ''retention_1m_hours''
     LIMIT 1;

    START TRANSACTION;
      SELECT v
        INTO v_lock_val
        FROM sky.pi_runtime_state
       WHERE k = ''cleanup_lock''
       FOR UPDATE;

      IF v_lock_val = ''1'' THEN
        ROLLBACK;
        LEAVE proc;
      END IF;

      UPDATE sky.pi_runtime_state
         SET v = ''1''
       WHERE k = ''cleanup_lock'';
    COMMIT;

    DELETE FROM sky.pi_digest_rollup_10s
     WHERE ts_end < NOW(6) - INTERVAL v_ret_10s MINUTE;

    DELETE FROM sky.pi_dim_rollup_10s
     WHERE ts_end < NOW(6) - INTERVAL v_ret_10s MINUTE;

    DELETE FROM sky.pi_sql_wait_rollup_10s
     WHERE ts_end < NOW(6) - INTERVAL v_ret_10s MINUTE;

    DELETE FROM sky.pi_digest_rollup_1m
     WHERE ts_minute < NOW() - INTERVAL v_ret_1m HOUR;

    DELETE FROM sky.pi_dim_rollup_1m
     WHERE ts_minute < NOW() - INTERVAL v_ret_1m HOUR;

    DELETE FROM sky.pi_sql_wait_rollup_1m
     WHERE ts_minute < NOW() - INTERVAL v_ret_1m HOUR;

    DELETE FROM sky.pi_digest_snapshot
     WHERE last_seen_ts < NOW(6) - INTERVAL 7 DAY;

    DELETE FROM sky.pi_digest_dict
     WHERE last_seen_ts < NOW(6) - INTERVAL 7 DAY;

    UPDATE sky.pi_runtime_state
       SET v = ''0''
     WHERE k = ''cleanup_lock'';
  END
  ';
  PREPARE stmt FROM @cleanup_sql;
  EXECUTE stmt;
  DEALLOCATE PREPARE stmt;

-- --------------------------------------------------------------------------
-- 7) Install events
-- --------------------------------------------------------------------------

  DROP EVENT IF EXISTS sky.pi_ev_capture_sample;
  DROP EVENT IF EXISTS sky.pi_ev_downsample_1m;
  DROP EVENT IF EXISTS sky.pi_ev_cleanup_retention;

  CREATE EVENT sky.pi_ev_capture_sample
    ON SCHEDULE EVERY 10 SECOND
    ON COMPLETION PRESERVE
    ENABLE
    DO CALL sky.pi_capture_sample();

  CREATE EVENT sky.pi_ev_downsample_1m
    ON SCHEDULE EVERY 1 MINUTE
    ON COMPLETION PRESERVE
    ENABLE
    DO CALL sky.pi_downsample_1m();

  CREATE EVENT sky.pi_ev_cleanup_retention
    ON SCHEDULE EVERY 5 MINUTE
    ON COMPLETION PRESERVE
    ENABLE
    DO CALL sky.pi_cleanup_retention();

-- --------------------------------------------------------------------------
-- 8) Views
-- --------------------------------------------------------------------------

  CREATE OR REPLACE VIEW sky.pi_v_top_sql_5m AS
  SELECT
    r.ts_end,
    r.node_role,
    r.server_id,
    r.host_name,
    r.digest,
    d.digest_text,
    r.delta_exec,
    r.delta_time_sec,
    r.aas
  FROM sky.pi_digest_rollup_10s r
  LEFT JOIN sky.pi_digest_dict d
    ON d.digest = r.digest
  WHERE r.ts_end >= NOW(6) - INTERVAL 5 MINUTE
  ORDER BY r.aas DESC, r.delta_time_sec DESC;

  CREATE OR REPLACE VIEW sky.pi_v_top_sql_1h AS
  SELECT
    DATE_FORMAT(r.ts_minute, '%Y-%m-%d %H:%i:00') AS ts_minute,
    r.node_role,
    r.server_id,
    r.host_name,
    r.digest,
    d.digest_text,
    r.total_exec,
    r.total_time_sec,
    r.avg_aas
  FROM sky.pi_digest_rollup_1m r
  LEFT JOIN sky.pi_digest_dict d
    ON d.digest = r.digest
  WHERE r.ts_minute >= NOW() - INTERVAL 1 HOUR
  ORDER BY r.avg_aas DESC, r.total_time_sec DESC;

  CREATE OR REPLACE VIEW sky.pi_v_dbload_by_waitclass_5m AS
  SELECT
    ts_end,
    node_role,
    server_id,
    host_name,
    dim_value AS waitclass,
    SUM(aas) AS aas
  FROM sky.pi_dim_rollup_10s
  WHERE dim_type = 'waitclass'
    AND ts_end >= NOW(6) - INTERVAL 5 MINUTE
  GROUP BY ts_end, node_role, server_id, host_name, dim_value
  ORDER BY ts_end DESC, aas DESC;

  CREATE OR REPLACE VIEW sky.pi_v_dbload_by_user_5m AS
  SELECT
    ts_end,
    node_role,
    server_id,
    host_name,
    dim_value AS user_name,
    SUM(aas) AS aas
  FROM sky.pi_dim_rollup_10s
  WHERE dim_type = 'user'
    AND ts_end >= NOW(6) - INTERVAL 5 MINUTE
  GROUP BY ts_end, node_role, server_id, host_name, dim_value
  ORDER BY ts_end DESC, aas DESC;

  CREATE OR REPLACE VIEW sky.pi_v_dbload_by_host_5m AS
  SELECT
    ts_end,
    node_role,
    server_id,
    host_name,
    dim_value AS client_host,
    SUM(aas) AS aas
  FROM sky.pi_dim_rollup_10s
  WHERE dim_type = 'host'
    AND ts_end >= NOW(6) - INTERVAL 5 MINUTE
  GROUP BY ts_end, node_role, server_id, host_name, dim_value
  ORDER BY ts_end DESC, aas DESC;

  CREATE OR REPLACE VIEW sky.pi_v_dbload_by_schema_5m AS
  SELECT
    ts_end,
    node_role,
    server_id,
    host_name,
    dim_value AS schema_name,
    SUM(aas) AS aas
  FROM sky.pi_dim_rollup_10s
  WHERE dim_type = 'schema'
    AND ts_end >= NOW(6) - INTERVAL 5 MINUTE
  GROUP BY ts_end, node_role, server_id, host_name, dim_value
  ORDER BY ts_end DESC, aas DESC;

  CREATE OR REPLACE VIEW sky.pi_v_top_sql_wait_5m AS
  SELECT
    s.ts_end,
    s.node_role,
    s.server_id,
    s.host_name,
    s.digest,
    d.digest_text,
    s.waitclass,
    s.sampled_count,
    s.aas
  FROM sky.pi_sql_wait_rollup_10s s
  LEFT JOIN sky.pi_digest_dict d
    ON d.digest = s.digest
  WHERE s.ts_end >= NOW(6) - INTERVAL 5 MINUTE
  ORDER BY s.aas DESC, s.sampled_count DESC;

  CREATE OR REPLACE VIEW sky.pi_v_top_sql_blocking_5m AS
  SELECT
    s.ts_end,
    s.node_role,
    s.server_id,
    s.host_name,
    s.digest,
    d.digest_text,
    s.sampled_count,
    s.aas
  FROM sky.pi_sql_wait_rollup_10s s
  LEFT JOIN sky.pi_digest_dict d
    ON d.digest = s.digest
  WHERE s.ts_end >= NOW(6) - INTERVAL 5 MINUTE
    AND s.waitclass = 'lock'
  ORDER BY s.aas DESC, s.sampled_count DESC;

  CREATE OR REPLACE VIEW sky.pi_v_cpu_vs_wait_5m AS
  WITH total AS (
    SELECT
      ts_end,
      node_role,
      server_id,
      host_name,
      SUM(aas) AS aas_total
    FROM sky.pi_digest_rollup_10s
    WHERE ts_end >= NOW(6) - INTERVAL 5 MINUTE
    GROUP BY ts_end, node_role, server_id, host_name
  ),
  waits AS (
    SELECT
      ts_end,
      node_role,
      server_id,
      host_name,
      SUM(aas) AS aas_wait
    FROM sky.pi_dim_rollup_10s
    WHERE dim_type = 'waitclass'
      AND ts_end >= NOW(6) - INTERVAL 5 MINUTE
    GROUP BY ts_end, node_role, server_id, host_name
  )
  SELECT
    t.ts_end,
    t.node_role,
    t.server_id,
    t.host_name,
    t.aas_total,
    IFNULL(w.aas_wait, 0) AS aas_wait,
    GREATEST(t.aas_total - IFNULL(w.aas_wait, 0), 0) AS aas_cpu_approx
  FROM total t
  LEFT JOIN waits w
    ON w.ts_end = t.ts_end
   AND w.node_role = t.node_role
   AND w.server_id = t.server_id
   AND w.host_name = t.host_name
  ORDER BY t.ts_end DESC;

-- --------------------------------------------------------------------------
-- 9) Helper procedures
-- --------------------------------------------------------------------------
DELIMITER //

CREATE OR REPLACE PROCEDURE sky.pi_pause()
BEGIN
  ALTER EVENT sky.pi_ev_capture_sample DISABLE;
  ALTER EVENT sky.pi_ev_downsample_1m DISABLE;
  ALTER EVENT sky.pi_ev_cleanup_retention DISABLE;
  SELECT 'PI-LITE paused.' AS message;
END //

CREATE OR REPLACE PROCEDURE sky.pi_resume()
BEGIN
  ALTER EVENT sky.pi_ev_capture_sample ENABLE;
  ALTER EVENT sky.pi_ev_downsample_1m ENABLE;
  ALTER EVENT sky.pi_ev_cleanup_retention ENABLE;
  SELECT 'PI-LITE resumed.' AS message;
END //

CREATE OR REPLACE PROCEDURE sky.pi_uninstall()
BEGIN
  DROP EVENT IF EXISTS sky.pi_ev_capture_sample;
  DROP EVENT IF EXISTS sky.pi_ev_downsample_1m;
  DROP EVENT IF EXISTS sky.pi_ev_cleanup_retention;

  DROP VIEW IF EXISTS sky.pi_v_top_sql_5m;
  DROP VIEW IF EXISTS sky.pi_v_top_sql_1h;
  DROP VIEW IF EXISTS sky.pi_v_dbload_by_waitclass_5m;
  DROP VIEW IF EXISTS sky.pi_v_dbload_by_user_5m;
  DROP VIEW IF EXISTS sky.pi_v_dbload_by_host_5m;
  DROP VIEW IF EXISTS sky.pi_v_dbload_by_schema_5m;
  DROP VIEW IF EXISTS sky.pi_v_top_sql_wait_5m;
  DROP VIEW IF EXISTS sky.pi_v_top_sql_blocking_5m;
  DROP VIEW IF EXISTS sky.pi_v_cpu_vs_wait_5m;

  DROP TABLE IF EXISTS sky.pi_sql_wait_rollup_1m;
  DROP TABLE IF EXISTS sky.pi_dim_rollup_1m;
  DROP TABLE IF EXISTS sky.pi_digest_rollup_1m;
  DROP TABLE IF EXISTS sky.pi_sql_wait_rollup_10s;
  DROP TABLE IF EXISTS sky.pi_dim_rollup_10s;
  DROP TABLE IF EXISTS sky.pi_digest_rollup_10s;
  DROP TABLE IF EXISTS sky.pi_digest_snapshot;
  DROP TABLE IF EXISTS sky.pi_digest_dict;
  DROP TABLE IF EXISTS sky.pi_runtime_state;
  DROP TABLE IF EXISTS sky.pi_config;

  SELECT 'PI-LITE data objects removed from schema sky. Helper procedures remain installed for MariaDB compatibility.' AS message;
END //

DELIMITER ;

-- --------------------------------------------------------------------------
-- 10) Success message
-- --------------------------------------------------------------------------
SELECT
  'PI-LITE installed successfully. Sampling is active every 10 seconds.' AS message;

SELECT 'Useful views:' AS note
UNION ALL SELECT '  sky.pi_v_top_sql_5m'
UNION ALL SELECT '  sky.pi_v_top_sql_1h'
UNION ALL SELECT '  sky.pi_v_top_sql_wait_5m'
UNION ALL SELECT '  sky.pi_v_top_sql_blocking_5m'
UNION ALL SELECT '  sky.pi_v_dbload_by_waitclass_5m'
UNION ALL SELECT '  sky.pi_v_dbload_by_user_5m'
UNION ALL SELECT '  sky.pi_v_dbload_by_host_5m'
UNION ALL SELECT '  sky.pi_v_dbload_by_schema_5m'
UNION ALL SELECT '  sky.pi_v_cpu_vs_wait_5m';

-- ============================================================================
-- EXAMPLES
-- ============================================================================
-- SELECT * FROM sky.pi_v_top_sql_5m LIMIT 10;
-- SELECT * FROM sky.pi_v_top_sql_wait_5m LIMIT 10;
-- SELECT * FROM sky.pi_v_top_sql_blocking_5m LIMIT 10;
-- SELECT * FROM sky.pi_v_dbload_by_waitclass_5m;
-- SELECT * FROM sky.pi_v_cpu_vs_wait_5m;
--
-- Tune settings:
-- UPDATE sky.pi_config SET v='25' WHERE k='topn_digests_per_window';
-- UPDATE sky.pi_config SET v='60' WHERE k='retention_10s_minutes';
-- UPDATE sky.pi_config SET v='6'  WHERE k='retention_1m_hours';
--
-- Pause/resume:
-- CALL sky.pi_pause();
-- CALL sky.pi_resume();
--
-- Remove:
-- CALL sky.pi_uninstall();
-- ============================================================================