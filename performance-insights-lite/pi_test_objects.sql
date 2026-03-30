USE sky;

CREATE TABLE IF NOT EXISTS sky.pi_test_workload (
  id INT NOT NULL PRIMARY KEY,
  grp_id INT NOT NULL,
  payload VARBINARY(512) NOT NULL,
  updated_ts DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
    ON UPDATE CURRENT_TIMESTAMP(6),
  KEY idx_pi_test_workload_grp_id (grp_id)
) ENGINE=InnoDB;

DELIMITER //

CREATE OR REPLACE PROCEDURE sky.pi_test_seed_workload(IN p_rows INT)
BEGIN
  DECLARE i INT DEFAULT 1;
  DECLARE v_rows INT DEFAULT p_rows;

  IF v_rows < 32 THEN
    SET v_rows = 32;
  END IF;

  IF v_rows > 5000 THEN
    SET v_rows = 5000;
  END IF;

  WHILE i <= v_rows DO
    INSERT INTO sky.pi_test_workload(id, grp_id, payload)
    VALUES (
      i,
      MOD(i, 32),
      RPAD(CONCAT('seed-', i), 128, 'x')
    )
    ON DUPLICATE KEY UPDATE
      grp_id = VALUES(grp_id),
      payload = VALUES(payload);
    SET i = i + 1;
  END WHILE;

  DELETE FROM sky.pi_test_workload
   WHERE id > v_rows;
END //

CREATE OR REPLACE PROCEDURE sky.pi_test_write_worker_seconds(
  IN p_run_seconds INT,
  IN p_worker_id INT,
  IN p_select_every INT,
  IN p_payload_bytes INT
)
BEGIN
  DECLARE v_end DATETIME(6);
  DECLARE i BIGINT DEFAULT 0;
  DECLARE v_keyspace INT DEFAULT 0;
  DECLARE v_work_keyspace INT DEFAULT 0;
  DECLARE v_id INT DEFAULT 0;
  DECLARE v_grp_lo INT DEFAULT 0;
  DECLARE v_dummy BIGINT DEFAULT 0;
  DECLARE v_payload VARBINARY(512);

  SELECT COUNT(*) INTO v_keyspace
    FROM sky.pi_test_workload;

  IF v_keyspace = 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'pi_test_workload is empty; seed rows first';
  END IF;

  IF p_run_seconds < 1 THEN
    SET p_run_seconds = 1;
  END IF;

  IF p_select_every < 1 THEN
    SET p_select_every = 1;
  END IF;

  IF p_payload_bytes < 32 THEN
    SET p_payload_bytes = 32;
  END IF;

  IF p_payload_bytes > 512 THEN
    SET p_payload_bytes = 512;
  END IF;

  SET v_end = NOW(6) + INTERVAL p_run_seconds SECOND;
  SET v_work_keyspace = GREATEST(v_keyspace - 1, 1);

  WHILE NOW(6) < v_end DO
    SET i = i + 1;

    IF v_keyspace = 1 THEN
      SET v_id = 1;
    ELSE
      SET v_id = 2 + MOD((p_worker_id * 997 + i * 13), v_work_keyspace);
    END IF;

    SET v_payload = RPAD(CONCAT('w', p_worker_id, '-', i), p_payload_bytes, 'x');

    UPDATE sky.pi_test_workload
       SET grp_id = MOD(grp_id + p_worker_id + 1, 64),
           payload = v_payload
     WHERE id = v_id;

    IF MOD(i, p_select_every) = 0 THEN
      SET v_grp_lo = MOD(p_worker_id + i, 32);
      SELECT COUNT(*) INTO v_dummy
        FROM sky.pi_test_workload
       WHERE grp_id BETWEEN v_grp_lo AND LEAST(v_grp_lo + 3, 63);
    END IF;

    IF MOD(i, 5) = 0 THEN
      SELECT grp_id INTO v_dummy
        FROM sky.pi_test_workload
       WHERE id = 1 + MOD(v_id, v_keyspace);
    END IF;
  END WHILE;
END //

CREATE OR REPLACE PROCEDURE sky.pi_test_lock_holder(IN p_hold_seconds INT)
BEGIN
  IF p_hold_seconds < 1 THEN
    SET p_hold_seconds = 1;
  END IF;

  START TRANSACTION;
    UPDATE sky.pi_test_workload
       SET grp_id = grp_id + 1
     WHERE id = 1;
    DO SLEEP(p_hold_seconds);
  COMMIT;
END //

CREATE OR REPLACE PROCEDURE sky.pi_test_lock_waiter(IN p_worker_id INT)
BEGIN
  DECLARE v_dummy INT DEFAULT 0;

  START TRANSACTION;
    UPDATE sky.pi_test_workload
       SET grp_id = grp_id + p_worker_id
     WHERE id = 1;

    SELECT grp_id INTO v_dummy
      FROM sky.pi_test_workload
     WHERE id = 1;
  COMMIT;
END //

DELIMITER ;
