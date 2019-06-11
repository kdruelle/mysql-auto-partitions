-- -----------------------------------------------------------------------------
--
-- Copyright 2016 Kevin Druelle
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
-- -----------------------------------------------------------------------------


-- -----------------------------------------------------------------------------
-- Table structure for table manage_partitions
--
-- This table will hold partitions configuration and states
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS `manage_partitions`;
CREATE TABLE `manage_partitions` (
  `tablename` VARCHAR(64) NOT NULL COMMENT 'Table name',
  `table_date_field`VARCHAR(64) NOT NULL COMMENT 'Column name in database used for ranging',
  `period` ENUM('daily', 'monthly') NOT NULL COMMENT 'Period - either daily or monthly',
  `future` INT(3) UNSIGNED NOT NULL COMMENT 'How many partition should be created for future',
  `keep_history` INT(3) UNSIGNED NOT NULL DEFAULT '1' COMMENT 'For how many days or months to keep the partitions',
  `last_updated` DATETIME DEFAULT NULL COMMENT 'When a partition was added last time',
  `comments` VARCHAR(128) DEFAULT '1' COMMENT 'Comments',
  PRIMARY KEY (`tablename`)
) ENGINE=INNODB;




delimiter $$
-- -----------------------------------------------------------------------------
-- procedure definition for init_partitions
--
-- this procedure is responsible of initializing partitions
-- according configuration in table manage_partitions
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS `init_partitions`$$
CREATE PROCEDURE `init_partitions`()
BEGIN
    DECLARE V_TABLENAME VARCHAR(64);
    DECLARE V_PERIOD VARCHAR(12);
    DECLARE V_KEEP_HISTORY INT;
    DECLARE V_FUTURE INT;
    DECLARE V_TABLE_FIELD VARCHAR(64);
    DECLARE DONE INT DEFAULT 0;

    DECLARE get_prt_tables CURSOR FOR
        SELECT `tablename`, `period`, `keep_history`, `future`, `table_date_field`
            FROM manage_partitions WHERE `last_updated` IS NULL;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET DONE = 1;

    OPEN get_prt_tables;

    loop_create_part: LOOP

        FETCH get_prt_tables INTO V_TABLENAME, V_PERIOD, V_KEEP_HISTORY, V_FUTURE, V_TABLE_FIELD;

        IF DONE THEN
            LEAVE loop_create_part;
        END IF;


        CASE WHEN V_PERIOD = 'daily' THEN
            CALL `init_partition_by_day`(V_TABLENAME, V_KEEP_HISTORY, V_FUTURE, V_TABLE_FIELD);
        WHEN V_PERIOD = 'monthly' THEN
            CALL `init_partition_by_month`(V_TABLENAME, V_KEEP_HISTORY, V_FUTURE, V_TABLE_FIELD);
        ELSE
            BEGIN
                ITERATE loop_create_part;
            END;
        END CASE;

        UPDATE manage_partitions SET last_updated = NOW() WHERE tablename = V_TABLENAME;
    END LOOP loop_create_part;

    CLOSE get_prt_tables;

END$$


-- -----------------------------------------------------------------------------
-- procedure definition for init_partition_by_day
--
-- this procedure is responsible of initialize daily 
-- partitions
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS `init_partition_by_day`$$
CREATE PROCEDURE `init_partition_by_day`(IN_TABLENAME VARCHAR(64), IN_DAYS INT, IN_FUTURE INT , IN_TABLE_FIELD VARCHAR(64))
BEGIN
    DECLARE ROWS_CNT INT UNSIGNED;
    DECLARE BEGINTIME TIMESTAMP;
    DECLARE ENDTIME INT UNSIGNED;
    DECLARE PARTITIONNAME VARCHAR(16);
    DECLARE PART_DEF TEXT;
    DECLARE PART_CNT INT DEFAULT 0;
    SET PART_DEF = CONCAT( 'ALTER TABLE `', IN_TABLENAME, '` PARTITION BY RANGE(TO_DAYS(', IN_TABLE_FIELD , '))(');

    label1: LOOP

        SET BEGINTIME = DATE(NOW()) - INTERVAL (IN_DAYS - PART_CNT) DAY;
        SET PARTITIONNAME = DATE_FORMAT( BEGINTIME, 'p%Y_%m_%d' );

        SET ENDTIME = TO_DAYS(BEGINTIME + INTERVAL 1 DAY);

        SELECT COUNT(*) INTO ROWS_CNT
                FROM information_schema.partitions
                WHERE table_schema = DATABASE() AND table_name = IN_TABLENAME AND partition_name = PARTITIONNAME;

        IF ROWS_CNT = 0 THEN
            SET PART_DEF = CONCAT( PART_DEF, 'PARTITION ', PARTITIONNAME, ' VALUES LESS THAN (', ENDTIME, '),\n' );
        ELSE
            SELECT CONCAT("partition `", PARTITIONNAME, "` for table `",IN_SCHEMANAME, ".", IN_TABLENAME, "` already exists") AS RESULT;
        END IF;

        SET PART_CNT = PART_CNT + 1;
        IF PART_CNT <= IN_DAYS + IN_FUTURE THEN
            ITERATE label1;
        END IF;
        LEAVE label1;
    END LOOP label1;

    SET PART_DEF = CONCAT(PART_DEF, 'PARTITION future VALUES LESS THAN (MAXVALUE))');

    set @SQL=PART_DEF;
    PREPARE STMT FROM @SQL;
    EXECUTE STMT;
    DEALLOCATE PREPARE STMT;
    SELECT PART_DEF;
END$$


-- -----------------------------------------------------------------------------
-- procedure definition for init_partition_by_month
--
-- this procedure is responsible of initialize monthly 
-- partitions
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS `init_partition_by_month`$$
CREATE PROCEDURE `init_partition_by_month`(IN_TABLENAME VARCHAR(64), IN_MONTHS INT, IN_FUTURE INT, IN_TABLE_FIELD VARCHAR(128))
BEGIN
    DECLARE ROWS_CNT INT UNSIGNED;
    DECLARE BEGINTIME TIMESTAMP;
    DECLARE ENDTIME INT UNSIGNED;
    DECLARE PARTITIONNAME VARCHAR(16);
    DECLARE PART_DEF TEXT;
    DECLARE PART_CNT INT DEFAULT 0;
    SET PART_DEF = CONCAT( 'ALTER TABLE `', IN_TABLENAME, '` PARTITION BY RANGE(TO_DAYS(', IN_TABLE_FIELD , '))(');

    label1: LOOP


        SET BEGINTIME = DATE(DATE_FORMAT(NOW() ,'%Y-%m-01')) - INTERVAL (IN_MONTHS - PART_CNT) MONTH;
        SET PARTITIONNAME = DATE_FORMAT( BEGINTIME, 'p%Y_%m' );

        SET ENDTIME = TO_DAYS(BEGINTIME + INTERVAL 1 MONTH);

        SELECT COUNT(*) INTO ROWS_CNT
        FROM information_schema.partitions
        WHERE table_schema = DATABASE() AND table_name = IN_TABLENAME AND partition_name = PARTITIONNAME;

        IF ROWS_CNT = 0 THEN
            SET PART_DEF = CONCAT( PART_DEF, 'PARTITION ', PARTITIONNAME, ' VALUES LESS THAN (', ENDTIME, '),\n' );
        ELSE
            SELECT CONCAT("partition `", PARTITIONNAME, "` for table `", IN_TABLENAME, "` already exists") AS RESULT;
        END IF;

        SET PART_CNT = PART_CNT + 1;
        IF PART_CNT <= IN_MONTHS + IN_FUTURE THEN
            ITERATE label1;
        END IF;
        LEAVE label1;
    END LOOP label1;

    SET PART_DEF = CONCAT(PART_DEF, 'PARTITION future VALUES LESS THAN (MAXVALUE))');

    set @SQL=PART_DEF;
    PREPARE STMT FROM @SQL;
    EXECUTE STMT;
    DEALLOCATE PREPARE STMT;
    SELECT PART_DEF;
END$$


-- -----------------------------------------------------------------------------
-- procedure definition for create_next_partitions
--
-- this procedure is responsible of creating all next partitions
-- according to configuration found in the table manage_partitions
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS `create_next_partitions`$$
CREATE PROCEDURE `create_next_partitions`()
BEGIN
    DECLARE V_TABLENAME VARCHAR(64);
    DECLARE V_PERIOD VARCHAR(12);
    DECLARE V_FUTURE INT;
    DECLARE DONE INT DEFAULT 0;

    DECLARE get_prt_tables CURSOR FOR
        SELECT `tablename`, `period`, `future`
            FROM manage_partitions WHERE `last_updated` IS NOT NULL;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET DONE = 1;

    OPEN get_prt_tables;

    loop_create_part: LOOP

        FETCH get_prt_tables INTO V_TABLENAME, V_PERIOD, V_FUTURE;

        IF DONE THEN
            LEAVE loop_create_part;
        END IF;


        CASE WHEN V_PERIOD = 'daily' THEN
                    CALL `create_partition_by_day`(V_TABLENAME, V_FUTURE);
             WHEN V_PERIOD = 'monthly' THEN
                    CALL `create_partition_by_month`(V_TABLENAME, V_FUTURE);
             ELSE
                BEGIN
                    ITERATE loop_create_part;
                END;
        END CASE;

        UPDATE manage_partitions SET last_updated = NOW() WHERE tablename = V_TABLENAME;
    END LOOP loop_create_part;

    CLOSE get_prt_tables;
END$$

-- -----------------------------------------------------------------------------
-- procedure definition for create_partition_by_day
--
-- this procedure is responsible of creating FUTURE partitions
-- for table IN_TABLENAME on daily basis.
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS `create_partition_by_day`$$
CREATE PROCEDURE `create_partition_by_day`(IN_TABLENAME VARCHAR(64), IN_FUTURE INT)
BEGIN
    DECLARE ROWS_CNT INT UNSIGNED;
    DECLARE BEGINTIME TIMESTAMP;
    DECLARE ENDTIME INT UNSIGNED;
    DECLARE PARTITIONNAME VARCHAR(16);
    DECLARE PART_DEF TEXT;
    DECLARE PART_CNT INT DEFAULT 0;
    DECLARE TODO INT DEFAULT 0;

    SET PART_DEF = CONCAT( 'ALTER TABLE `', IN_TABLENAME, '` REORGANIZE PARTITION future INTO(');

    label1: LOOP
        SET BEGINTIME = DATE(NOW()) + INTERVAL (PART_CNT) DAY;
        SET ENDTIME = TO_DAYS(BEGINTIME + INTERVAL 1 DAY);
        SET PARTITIONNAME = DATE_FORMAT( BEGINTIME, 'p%Y_%m_%d' );


        SELECT COUNT(*) INTO ROWS_CNT
            FROM information_schema.partitions
            WHERE table_schema = DATABASE() AND table_name = IN_TABLENAME AND partition_name = PARTITIONNAME;

        IF ROWS_CNT = 0 THEN
            SET PART_DEF = CONCAT( PART_DEF,  'PARTITION ', PARTITIONNAME, ' VALUES LESS THAN (', ENDTIME, '),\n');
            SET TODO = 1;
        END IF;

        SET PART_CNT = PART_CNT + 1;
        IF PART_CNT <= FUTURE THEN
            ITERATE label1;
        END IF;
        LEAVE label1;

    END LOOP label1;

    IF TODO = 1 THEN
        SET PART_DEF = CONCAT( PART_DEF, 'PARTITION future VALUES LESS THAN (MAXVALUE));');
        SET @SQL = PART_DEF;
        SET SQL_LOG_BIN=1;
        PREPARE STMT FROM @SQL;
        EXECUTE STMT;
        DEALLOCATE PREPARE STMT;
        SET SQL_LOG_BIN=0;
        SELECT PART_DEF;
    ELSE
        SELECT 'nothing to do, all partitions already exists' AS RESULT;
    END IF;
END$$

-- -----------------------------------------------------------------------------
-- procedure definition for create_partition_by_month
--
-- this procedure is responsible of creating FUTURE partitions
-- for table IN_TABLENAME on monthly basis.
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS `create_partition_by_month`$$
CREATE PROCEDURE `create_partition_by_month`(IN_TABLENAME VARCHAR(64), FUTURE INT)
BEGIN
    DECLARE ROWS_CNT INT UNSIGNED;
    DECLARE BEGINTIME TIMESTAMP;
    DECLARE ENDTIME INT UNSIGNED;
    DECLARE PARTITIONNAME VARCHAR(16);
    DECLARE PART_DEF TEXT;
    DECLARE PART_CNT INT DEFAULT 0;
    DECLARE TODO INT DEFAULT 0;

    SET PART_DEF = CONCAT( 'ALTER TABLE `', IN_TABLENAME, '` REORGANIZE PARTITION future INTO(');

    label1: LOOP
        SET BEGINTIME = DATE(DATE_FORMAT(NOW() ,'%Y-%m-01')) + INTERVAL (PART_CNT) MONTH;
        SET PARTITIONNAME = DATE_FORMAT( BEGINTIME, 'p%Y_%m' );
        SET ENDTIME = TO_DAYS(BEGINTIME + INTERVAL 1 MONTH);

        SELECT COUNT(*) INTO ROWS_CNT
            FROM information_schema.partitions
            WHERE table_schema = DATABASE() AND table_name = IN_TABLENAME AND partition_name = PARTITIONNAME;

        IF ROWS_CNT = 0 THEN
            SET PART_DEF = CONCAT( PART_DEF,  'PARTITION ', PARTITIONNAME, ' VALUES LESS THAN (', ENDTIME, '),\n');
            SET TODO = 1;
        END IF;

        SET PART_CNT = PART_CNT + 1;
        IF PART_CNT <= FUTURE THEN
            ITERATE label1;
        END IF;
        LEAVE label1;

    END LOOP label1;

    IF TODO = 1 THEN
        SET PART_DEF = CONCAT( PART_DEF, 'PARTITION future VALUES LESS THAN (MAXVALUE));');
        SET @SQL = PART_DEF;
        SET SQL_LOG_BIN=1;
        PREPARE STMT FROM @SQL;
        EXECUTE STMT;
        DEALLOCATE PREPARE STMT;
        SET SQL_LOG_BIN=0;
        SELECT PART_DEF;
    ELSE
        SELECT 'nothing to do, all partitions already exists' AS RESULT;
    END IF;

END$$



-- -----------------------------------------------------------------------------
-- procedure definition for drop_partitions
--
-- this procedure is responsible of deleting old partitions
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS `drop_partitions`$$

CREATE PROCEDURE `drop_partitions`()
BEGIN
    DECLARE V_TABLENAME VARCHAR(64);
    DECLARE V_PARTITIONNAME VARCHAR(64);
    DECLARE V_VALUES_LESS INT;
    DECLARE V_PERIOD VARCHAR(12);
    DECLARE V_KEEP_HISTORY INT;
    DECLARE V_KEEP_HISTORY_BEFORE INT;
    DECLARE DONE INT DEFAULT 0;

    DECLARE get_partitions CURSOR FOR
        SELECT p.`table_name`, p.`partition_name`, LTRIM(RTRIM(p.`partition_description`)), mp.`period`, mp.`keep_history`
            FROM information_schema.partitions p
            JOIN manage_partitions mp ON mp.tablename = p.table_name
            WHERE p.table_schema = DATABASE() AND mp.last_updated IS NOT NULL AND LTRIM(RTRIM(p.`partition_description`)) <> 'MAXVALUE'
            ORDER BY p.table_name, p.subpartition_ordinal_position;
            DECLARE CONTINUE HANDLER FOR NOT FOUND SET DONE = 1;

    OPEN get_partitions;

    loop_check_prt: LOOP
        IF DONE THEN
            LEAVE loop_check_prt;
        END IF;

        FETCH get_partitions INTO V_TABLENAME, V_PARTITIONNAME, V_VALUES_LESS, V_PERIOD, V_KEEP_HISTORY;
        CASE WHEN V_PERIOD = 'daily' THEN
            SET V_KEEP_HISTORY_BEFORE = TO_DAYS(DATE(NOW() - INTERVAL V_KEEP_HISTORY DAY));
        WHEN V_PERIOD = 'monthly' THEN
            SET V_KEEP_HISTORY_BEFORE = TO_DAYS(DATE(NOW() - INTERVAL V_KEEP_HISTORY MONTH - INTERVAL DAY(NOW())-1 DAY));
        ELSE
             BEGIN
                ITERATE loop_check_prt;
             END;
        END CASE;

        IF V_KEEP_HISTORY_BEFORE >= V_VALUES_LESS THEN
            CALL drop_old_partition(V_TABLENAME, V_PARTITIONNAME);
        END IF;
    END LOOP loop_check_prt;

    CLOSE get_partitions;
END$$

-- -----------------------------------------------------------------------------
-- procedure definition for drop_old_partition
--
-- this procedure is responsible of deleting old partition
-- for table IN_TABLENAME
-- -----------------------------------------------------------------------------
DROP PROCEDURE IF EXISTS `drop_old_partition`$$

CREATE PROCEDURE `drop_old_partition`(IN_TABLENAME VARCHAR(64), IN_PARTITIONNAME VARCHAR(64))
BEGIN
    DECLARE ROWS_CNT INT UNSIGNED;

    SELECT COUNT(*) INTO ROWS_CNT
    FROM information_schema.partitions
    WHERE table_schema = DATABASE() AND table_name = IN_TABLENAME AND partition_name = IN_PARTITIONNAME;

    IF ROWS_CNT = 1 THEN
        SET @SQL = CONCAT( 'ALTER TABLE `', IN_TABLENAME, '`', ' DROP PARTITION ', IN_PARTITIONNAME, ';' );
        SET SQL_LOG_BIN=0;
        PREPARE STMT FROM @SQL;
        EXECUTE STMT;
        DEALLOCATE PREPARE STMT;
        SET SQL_LOG_BIN=1;
    ELSE
        SELECT CONCAT("partition `", IN_PARTITIONNAME, "` for table `", IN_TABLENAME, "` not exists") AS RESULT;
    END IF;
END$$

-- -----------------------------------------------------------------------------
-- EVENT definition for managing partitions
-- -----------------------------------------------------------------------------
CREATE EVENT IF NOT EXISTS `e_part_manage` ON SCHEDULE EVERY 1 HOUR ON COMPLETION PRESERVE ENABLE COMMENT 'Creating and dropping partitions'
DO BEGIN
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
        DO RELEASE_LOCK('part_manage_lock');
    END;
    IF GET_LOCK('part_manage_lock', 0) THEN
        CALL create_next_partitions();
        CALL drop_partitions();
    END IF;
    DO RELEASE_LOCK('part_manage_lock');
END$$

DELIMITER ;

