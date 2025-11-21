-- DQ_PROFILE_FULL Stored Procedure
-- Captures profiling metadata with sampling awareness for Profiling v2.

-- Ensure the run history table is present with sampling metadata columns.
CREATE TABLE IF NOT EXISTS ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_PROFILE_RUN (
    RUN_ID            NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    TARGET_TABLE      STRING,
    STARTED_AT        TIMESTAMP,
    COMPLETED_AT      TIMESTAMP,
    STATUS            STRING,
    DURATION_SECONDS  NUMBER,
    ERROR_MESSAGE     STRING,
    ROW_COUNT         NUMBER,
    SAMPLE_MODE       STRING,
    SAMPLE_PERCENT    NUMBER,
    SAMPLE_EST_ROWS   NUMBER
);

ALTER TABLE IF EXISTS ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_PROFILE_RUN
    ADD COLUMN IF NOT EXISTS ROW_COUNT NUMBER,
    ADD COLUMN IF NOT EXISTS SAMPLE_MODE STRING,
    ADD COLUMN IF NOT EXISTS SAMPLE_PERCENT NUMBER,
    ADD COLUMN IF NOT EXISTS SAMPLE_EST_ROWS NUMBER;

CREATE OR REPLACE PROCEDURE ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_PROFILE_FULL(
    IN_TABLE_FQN STRING,
    DATABASE_NAME STRING DEFAULT NULL,
    SCHEMA_NAME STRING DEFAULT NULL,
    TABLE_NAME STRING DEFAULT NULL,
    MAX_SAMPLE_ROWS NUMBER DEFAULT 100000
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_table_fqn STRING := TRIM(COALESCE(IN_TABLE_FQN, ''));
    v_database_name STRING := IFF(TRIM(COALESCE(DATABASE_NAME, '')) = '', NULL, TRIM(DATABASE_NAME));
    v_schema_name STRING := IFF(TRIM(COALESCE(SCHEMA_NAME, '')) = '', NULL, TRIM(SCHEMA_NAME));
    v_table_name STRING := IFF(TRIM(COALESCE(TABLE_NAME, '')) = '', NULL, TRIM(TABLE_NAME));
    v_max_sample_rows NUMBER := COALESCE(MAX_SAMPLE_ROWS, 100000);
    v_row_count NUMBER := 0;
    v_sample_mode STRING := NULL;
    v_sample_percent NUMBER := NULL;
    v_sample_est_rows NUMBER := NULL;
    v_started_at TIMESTAMP := CURRENT_TIMESTAMP();
    v_completed_at TIMESTAMP;
    v_status STRING := 'SUCCESS';
    v_error STRING := NULL;
    v_from_clause STRING;
    v_profiled_rows NUMBER := 0;
    v_info_schema_table STRING;
BEGIN
    IF (:v_table_fqn = '' AND (v_database_name IS NULL OR v_schema_name IS NULL OR v_table_name IS NULL)) THEN
        RETURN 'ERROR: Table identifier is required';
    END IF;

    IF (:v_table_fqn IS NOT NULL AND :v_table_fqn != '') THEN
        v_database_name := SPLIT_PART(:v_table_fqn, '.', 1);
        v_schema_name := SPLIT_PART(:v_table_fqn, '.', 2);
        v_table_name := SPLIT_PART(:v_table_fqn, '.', 3);
    ELSE
        v_table_fqn := :v_database_name || '.' || :v_schema_name || '.' || :v_table_name;
    END IF;

    v_info_schema_table := :v_database_name || '.INFORMATION_SCHEMA.TABLES';

    EXECUTE IMMEDIATE
        $$SELECT COALESCE(ROW_COUNT, 0)
          FROM IDENTIFIER(?)
         WHERE TABLE_SCHEMA = ?
           AND TABLE_NAME = ?$$
        INTO :v_row_count
        USING (
            v_info_schema_table,
            v_schema_name,
            v_table_name
        );
        
    IF (:v_row_count <= :v_max_sample_rows) THEN
        v_sample_mode := 'FULL';
        v_sample_percent := NULL;
        v_sample_est_rows := :v_row_count;
    ELSE
        v_sample_mode := 'SAMPLE';
        v_sample_percent := CEIL(:v_max_sample_rows * 100.0 / NULLIF(:v_row_count, 0));
        v_sample_est_rows := CEIL(:v_row_count * :v_sample_percent / 100.0);
    END IF;

    IF (:v_sample_mode = 'FULL') THEN
        v_from_clause := :v_table_fqn;
    ELSE
        v_from_clause := :v_table_fqn || ' SAMPLE SYSTEM (' || :v_sample_percent || ')';
    END IF;

    EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || :v_from_clause INTO v_profiled_rows;

    v_completed_at := CURRENT_TIMESTAMP();

    INSERT INTO ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_PROFILE_RUN (
        TARGET_TABLE,
        STARTED_AT,
        COMPLETED_AT,
        STATUS,
        DURATION_SECONDS,
        ERROR_MESSAGE,
        ROW_COUNT,
        SAMPLE_MODE,
        SAMPLE_PERCENT,
        SAMPLE_EST_ROWS
    )
    SELECT
        :v_table_fqn,
        :v_started_at,
        :v_completed_at,
        :v_status,
        DATEDIFF('second', :v_started_at, :v_completed_at),
        :v_error,
        :v_row_count,
        :v_sample_mode,
        :v_sample_percent,
        COALESCE(:v_sample_est_rows, :v_profiled_rows);

    RETURN 'OK';
EXCEPTION
    WHEN OTHER THEN
        v_completed_at := CURRENT_TIMESTAMP();
        v_status := 'FAILED';
        v_error := SQLERRM;

        INSERT INTO ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_PROFILE_RUN (
            TARGET_TABLE,
            STARTED_AT,
            COMPLETED_AT,
            STATUS,
            DURATION_SECONDS,
            ERROR_MESSAGE,
            ROW_COUNT,
            SAMPLE_MODE,
            SAMPLE_PERCENT,
            SAMPLE_EST_ROWS
        )
        SELECT
            :v_table_fqn,
            :v_started_at,
            :v_completed_at,
            :v_status,
            DATEDIFF('second', :v_started_at, :v_completed_at),
            :v_error,
            :v_row_count,
            :v_sample_mode,
            :v_sample_percent,
            :v_sample_est_rows;

        RETURN 'ERROR: ' || COALESCE(:v_error, 'Unknown error');
END;
$$;

