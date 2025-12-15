CREATE OR REPLACE PROCEDURE ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_PROFILE_FULL(
    IN_TABLE_FQN STRING,
    DATABASE_NAME STRING DEFAULT NULL,
    SCHEMA_NAME STRING DEFAULT NULL,
    TABLE_NAME STRING DEFAULT NULL,
    MAX_SAMPLE_ROWS NUMBER DEFAULT 100000
)
RETURNS NUMBER
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
    v_profiled_rows NUMBER := 0;
    v_from_clause STRING;
    v_info_schema_table STRING;
    v_info_schema_columns STRING;
    v_started_at TIMESTAMP := CURRENT_TIMESTAMP();
    v_finished_at TIMESTAMP;
    v_status STRING := 'RUNNING';
    v_details STRING := NULL;
    v_profile_run_id NUMBER := NULL;
    v_feature_sql STRING := '';
    v_feature_count NUMBER := 0;
    v_is_string BOOLEAN;
    v_col_ident STRING;
    v_union_prefix STRING := '';
    v_tmpstr string;
    v_database_literal STRING;
    v_schema_literal STRING;
    v_table_literal STRING;
    v_table_fqn_literal STRING;
    v_col_literal STRING;
    v_data_type_literal STRING;
    v_rs resultset;
    e_table_error exception (-20001,'Table identifier is required');
    e_no_profile_id exception (-20002,'Failed to capture PROFILE_RUN_ID');
    e_no_col exception (-20003,'no col');
BEGIN
    IF (:v_table_fqn = '' AND (v_database_name IS NULL OR v_schema_name IS NULL OR v_table_name IS NULL)) THEN
--        RAISE STATEMENT_ERROR WITH MESSAGE = 'Table identifier is required';
        RAISE e_table_error;
    END IF;

    IF (:v_table_fqn IS NOT NULL AND :v_table_fqn != '') THEN
        v_database_name := SPLIT_PART(:v_table_fqn, '.', 1);
        v_schema_name := SPLIT_PART(:v_table_fqn, '.', 2);
        v_table_name := SPLIT_PART(:v_table_fqn, '.', 3);
    ELSE
        v_table_fqn := :v_database_name || '.' || :v_schema_name || '.' || :v_table_name;
    END IF;

    v_info_schema_table := :v_database_name || '.INFORMATION_SCHEMA.TABLES';
    v_info_schema_columns := :v_database_name || '.INFORMATION_SCHEMA.COLUMNS';

    /*
    EXECUTE IMMEDIATE
        'SELECT COALESCE(ROW_COUNT, 0)
           FROM IDENTIFIER(?)
          WHERE DATABASE_NAME = ? 
            AND TABLE_SCHEMA = ?
            AND TABLE_NAME = ?'
        INTO :v_row_count
        USING (:v_info_schema_table, :v_database_name, :v_schema_name, :v_table_name);
        */
    
    v_rs := (EXECUTE IMMEDIATE
                'SELECT COALESCE(ROW_COUNT, 0) row_count
                FROM IDENTIFIER(?)
                WHERE TABLE_SCHEMA = ?
                AND TABLE_NAME = ?'
            USING (v_info_schema_table, v_schema_name, v_table_name));

    for cols in v_rs
    do
        v_row_count := cols.row_count;
    end for;
    
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

    --EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || :v_from_clause INTO :v_profiled_rows;
    v_rs := (EXECUTE IMMEDIATE 'SELECT COUNT(*) row_count FROM ' || :v_from_clause);
    for col in v_rs
    do
        v_profiled_rows := col.row_count;
    end for;

    INSERT INTO ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_PROFILE_RUN (
        DATABASE_NAME,
        SCHEMA_NAME,
        TABLE_NAME,
        TABLE_FQN,
        STARTED_AT,
        STATUS,
        ROW_COUNT,
        SAMPLE_MODE,
        SAMPLE_PERCENT,
        SAMPLE_EST_ROWS,
        CREATED_AT,
        UPDATED_AT
    )
    SELECT
        :v_database_name,
        :v_schema_name,
        :v_table_name,
        :v_table_fqn,
        :v_started_at,
        :v_status,
        :v_row_count,
        :v_sample_mode,
        :v_sample_percent,
        COALESCE(:v_sample_est_rows, :v_profiled_rows),
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP();

    SELECT MAX(PROFILE_RUN_ID)
      INTO :v_profile_run_id
      FROM ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_PROFILE_RUN
     WHERE TABLE_FQN = :v_table_fqn
       AND STARTED_AT = :v_started_at;

    IF (:v_profile_run_id IS NULL) THEN
        RAISE e_no_profile_id;
    END IF;

    v_database_literal := IFF(v_database_name IS NULL, 'NULL', '\'' || REPLACE(v_database_name, '\'', '\'\'\'') || '\'');
    v_schema_literal := IFF(v_schema_name IS NULL, 'NULL', '\'' || REPLACE(v_schema_name, '\'', '\'\'\'') || '\'');
    v_table_literal := IFF(v_table_name IS NULL, 'NULL', '\'' || REPLACE(v_table_name, '\'', '\'\'\'') || '\'');
    v_table_fqn_literal := IFF(v_table_fqn IS NULL, 'NULL', '\'' || REPLACE(v_table_fqn, '\'', '\'\'\'') || '\'');

   /* FOR rec IN (
        SELECT COLUMN_NAME, DATA_TYPE
        FROM IDENTIFIER(:v_info_schema_columns)
        WHERE TABLE_SCHEMA = :v_schema_name
          AND TABLE_NAME = :v_table_name
        ORDER BY ORDINAL_POSITION
    ) DO*/

    v_rs := (EXECUTE IMMEDIATE
            'SELECT COLUMN_NAME, DATA_TYPE FROM identifier(?) where TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION'
            USING (v_info_schema_columns, v_schema_name, v_table_name));
    
    FOR rec in v_rs DO
        v_col_ident := '"' || REPLACE(rec.COLUMN_NAME, '"', '""') || '"';
        v_is_string := REGEXP_LIKE(UPPER(rec.DATA_TYPE), 'CHAR|TEXT|STRING');
        v_col_literal := IFF(rec.COLUMN_NAME IS NULL, 'NULL', '\'' || REPLACE(rec.COLUMN_NAME, '\'', '\'\'\'') || '\'');
        v_data_type_literal := IFF(rec.DATA_TYPE IS NULL, 'NULL', '\'' || REPLACE(rec.DATA_TYPE, '\'', '\'\'\'') || '\'');

        v_feature_sql := v_feature_sql || v_union_prefix || CHR(10) ||
            'SELECT ' || :v_profile_run_id || ' AS PROFILE_RUN_ID,' || CHR(10) ||
            '       ' || :v_database_literal || ' AS DATABASE_NAME,' || CHR(10) ||
            '       ' || :v_schema_literal || ' AS SCHEMA_NAME,' || CHR(10) ||
            '       ' || :v_table_literal || ' AS TABLE_NAME,' || CHR(10) ||
            '       ' || :v_table_fqn_literal || ' AS TABLE_FQN,' || CHR(10) ||
            '       ' || :v_col_literal || ' AS COLUMN_NAME,' || CHR(10) ||
            '       ' || :v_data_type_literal || ' AS DATA_TYPE,' || CHR(10) ||
            '       ' || :v_profiled_rows || ' AS ROW_COUNT,' || CHR(10) ||
            '       NULL_COUNT,' || CHR(10) ||
            '       NULL_COUNT / NULLIF(' || :v_profiled_rows || ', 0) AS NULL_RATIO,' || CHR(10) ||
            '       DISTINCT_COUNT,' || CHR(10) ||
            '       DISTINCT_COUNT / NULLIF(' || :v_profiled_rows || ', 0) AS DISTINCT_RATIO,' || CHR(10) ||
            '       MIN_VALUE,' || CHR(10) ||
            '       MAX_VALUE,' || CHR(10) ||
            '       ' || IFF(v_is_string, 'MIN_LENGTH_RAW', 'NULL') || ' AS MIN_LENGTH,' || CHR(10) ||
            '       ' || IFF(v_is_string, 'MAX_LENGTH_RAW', 'NULL') || ' AS MAX_LENGTH,' || CHR(10) ||
            '       ' || IFF(v_is_string, 'AVG_LENGTH_RAW', 'NULL') || ' AS AVG_LENGTH,' || CHR(10) ||
            '       CURRENT_TIMESTAMP(),' || CHR(10) ||
            '       CURRENT_TIMESTAMP()' || CHR(10) ||
            '  FROM (SELECT' || CHR(10) ||
            '                SUM(IFF(' || v_col_ident || ' IS NULL, 1, 0)) AS NULL_COUNT,' || CHR(10) ||
            '                COUNT(DISTINCT ' || v_col_ident || ') AS DISTINCT_COUNT,' || CHR(10) ||
            '                TO_VARIANT(MIN(' || v_col_ident || ')) AS MIN_VALUE,' || CHR(10) ||
            '                TO_VARIANT(MAX(' || v_col_ident || ')) AS MAX_VALUE,' || CHR(10) ||
            '                MIN(LENGTH(TO_VARCHAR(' || v_col_ident || '))) AS MIN_LENGTH_RAW,' || CHR(10) ||
            '                MAX(LENGTH(TO_VARCHAR(' || v_col_ident || '))) AS MAX_LENGTH_RAW,' || CHR(10) ||
            '                AVG(LENGTH(TO_VARCHAR(' || v_col_ident || '))) AS AVG_LENGTH_RAW' || CHR(10) ||
            '          FROM ' || :v_from_clause || ')';
        v_union_prefix := CHR(10) || 'UNION ALL';
    END FOR;

    IF (:v_feature_sql = '') THEN
        v_details := 'No columns found to profile for ' || :v_table_fqn;
        --RAISE STATEMENT_ERROR WITH MESSAGE = v_details;
        raise e_no_col;
    END IF;

    EXECUTE IMMEDIATE
        'DELETE FROM ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_COLUMN_FEATURES
         WHERE TABLE_FQN = ?'
        USING (v_table_fqn);
        
    EXECUTE IMMEDIATE 'INSERT INTO ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_COLUMN_FEATURES (
            PROFILE_RUN_ID,
            DATABASE_NAME,
            SCHEMA_NAME,
            TABLE_NAME,
            TABLE_FQN,
            COLUMN_NAME,
            DATA_TYPE,
            ROW_COUNT,
            NULL_COUNT,
            NULL_RATIO,
            DISTINCT_COUNT,
            DISTINCT_RATIO,
            MIN_VALUE,
            MAX_VALUE,
            MIN_LENGTH,
            MAX_LENGTH,
            AVG_LENGTH,
            CREATED_AT,
            UPDATED_AT
        ) ' || v_feature_sql;
        
    v_rs := (EXECUTE IMMEDIATE
            'SELECT COUNT(*) as feature_count
                FROM ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_COLUMN_FEATURES
            WHERE TABLE_FQN = ?'
        --INTO :v_feature_count
        USING (v_table_fqn));
    
    for rec in v_rs do v_feature_count := rec.feature_count; end for;    
    IF (:v_feature_count = 0) THEN
        v_details := 'Profiling completed but no feature rows were persisted for '
                     || :v_table_fqn;
        --RAISE STATEMENT_ERROR WITH MESSAGE = v_details;
        RAISE e_no_col;
    END IF;

    v_finished_at := CURRENT_TIMESTAMP();
    v_status := 'SUCCESS';

    UPDATE ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_PROFILE_RUN
       SET FINISHED_AT = :v_finished_at,
           STATUS = :v_status,
           DETAILS = NULL,
           UPDATED_AT = CURRENT_TIMESTAMP()
     WHERE PROFILE_RUN_ID = :v_profile_run_id;

    RETURN v_profile_run_id;
EXCEPTION
    WHEN STATEMENT_ERROR OR EXPRESSION_ERROR THEN
        v_finished_at := CURRENT_TIMESTAMP();
        v_status := 'FAILED';
        v_details := SQLERRM;

        IF (v_profile_run_id IS NOT NULL) THEN
            UPDATE ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_PROFILE_RUN
               SET FINISHED_AT = :v_finished_at,
                   STATUS = :v_status,
                   DETAILS = :v_details,
                   UPDATED_AT = CURRENT_TIMESTAMP()
             WHERE PROFILE_RUN_ID = :v_profile_run_id;
        END IF;
        RAISE;
END;
$$;
