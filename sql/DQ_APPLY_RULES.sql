CREATE OR REPLACE PROCEDURE ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_APPLY_RULES(
    IN_TABLE_FQN STRING
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER -- SECURITY INVOKER
AS
$$
DECLARE
    v_table_fqn   STRING := TRIM(COALESCE(IN_TABLE_FQN, ''));
    v_deleted     NUMBER := 0;
    v_inserted    NUMBER := 0;
BEGIN
    IF (:v_table_fqn = '') THEN
        RETURN 'ERROR: TABLE_FQN is required';
    END IF;

    DELETE FROM ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_SUGGESTED_CHECKS
    WHERE TABLE_FQN = :v_table_fqn;
    v_deleted := SQLROWCOUNT;

    INSERT INTO ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_SUGGESTED_CHECKS (
        TABLE_FQN,
        COLUMN_NAME,
        RULE_ID,
        CHECK_TYPE,
        SEVERITY,
        RATIONALE,
        SUGGESTED_AT
    )
    WITH features AS (
        SELECT
            TABLE_FQN,
            COLUMN_NAME,
            UPPER(COALESCE(DATA_TYPE, '')) AS DATA_TYPE,
            COALESCE(NULL_RATIO, 1) AS NULL_RATIO,
            COALESCE(DISTINCT_RATIO, 1) AS DISTINCT_RATIO,
            COALESCE(DISTINCT_COUNT, 0) AS DISTINCT_COUNT,
            MIN_VALUE,
            MAX_VALUE
        FROM ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_COLUMN_FEATURES
        WHERE TABLE_FQN = :v_table_fqn
    ),
    classification AS (
        SELECT TABLE_FQN, COLUMN_NAME, LOWER(COALESCE(CONTENT_TYPE, '')) AS CONTENT_TYPE
        FROM (
            SELECT
                TABLE_FQN,
                COLUMN_NAME,
                CONTENT_TYPE,
                CLASSIFIED_AT,
                ROW_NUMBER() OVER (
                    PARTITION BY TABLE_FQN, COLUMN_NAME
                    ORDER BY CLASSIFIED_AT DESC
                ) AS RN
            FROM ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_COLUMN_CLASSIFICATION
            WHERE TABLE_FQN = :v_table_fqn
        )
        WHERE RN = 1
    ),
    column_context AS (
        SELECT
            f.TABLE_FQN,
            f.COLUMN_NAME,
            f.DATA_TYPE,
            f.NULL_RATIO,
            f.DISTINCT_RATIO,
            f.DISTINCT_COUNT,
            f.MIN_VALUE,
            f.MAX_VALUE,
            COALESCE(c.CONTENT_TYPE, '') AS CONTENT_TYPE,
            CASE
                WHEN REGEXP_LIKE(f.DATA_TYPE, 'NUMBER|DECIMAL|INT|FLOAT|DOUBLE|REAL') THEN 'NUMERIC'
                WHEN REGEXP_LIKE(f.DATA_TYPE, 'DATE|TIME|TIMESTAMP') THEN 'DATE'
                WHEN REGEXP_LIKE(f.DATA_TYPE, 'CHAR|TEXT|STRING|VARCHAR') THEN 'STRING'
                ELSE 'ANY'
            END AS DATA_TYPE_FAMILY,
            IFF(
                NULLIF(c.CONTENT_TYPE, '') IS NULL,
                ARRAY_CONSTRUCT(),
                ARRAY_CONSTRUCT(UPPER(c.CONTENT_TYPE))
            ) AS CLASS_LABELS
        FROM features f
        LEFT JOIN classification c
            ON f.TABLE_FQN = c.TABLE_FQN
           AND f.COLUMN_NAME = c.COLUMN_NAME
    ),
    active_rules AS (
        SELECT RULE_ID, CHECK_TYPE, DEFAULT_SEVERITY, DATA_TYPE_FAMILY, APPLICABILITY_TAGS
        FROM ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_RULE_LIBRARY
        WHERE COALESCE(ENABLED, TRUE) = TRUE
          AND COALESCE(DEFAULT_SUGGEST, TRUE) = TRUE
          AND COALESCE(SCOPE, 'COLUMN') = 'COLUMN'
    )
    SELECT
        ctx.TABLE_FQN,
        ctx.COLUMN_NAME,
        'NOT_NULL_BASIC' AS RULE_ID,
        rule_not_null.CHECK_TYPE,
        rule_not_null.DEFAULT_SEVERITY,
        'Observed null ratio ' || TO_CHAR(ctx.NULL_RATIO * 100, 'FM999990.00') || '% is below the 10% tolerance.' AS RATIONALE,
        CURRENT_TIMESTAMP()
    FROM column_context ctx
    JOIN active_rules rule_not_null
        ON rule_not_null.RULE_ID = 'NOT_NULL_BASIC'
       AND (
            rule_not_null.DATA_TYPE_FAMILY IN ('ANY', ctx.DATA_TYPE_FAMILY)
            OR rule_not_null.DATA_TYPE_FAMILY IS NULL
        )
       AND (
            rule_not_null.APPLICABILITY_TAGS IS NULL
            OR ARRAY_SIZE(rule_not_null.APPLICABILITY_TAGS) = 0
            OR ARRAY_SIZE(ARRAY_INTERSECTION(rule_not_null.APPLICABILITY_TAGS, ctx.CLASS_LABELS)) > 0
        )
    WHERE ctx.NULL_RATIO < 0.1

    UNION ALL

    SELECT
        ctx.TABLE_FQN,
        ctx.COLUMN_NAME,
        'RANGE_NUMERIC' AS RULE_ID,
        rule_range.CHECK_TYPE,
        rule_range.DEFAULT_SEVERITY,
        'Numeric column with min=' || COALESCE(TO_VARCHAR(ctx.MIN_VALUE), 'NULL') || ' and max=' || COALESCE(TO_VARCHAR(ctx.MAX_VALUE), 'NULL') || ' warrants a range check.' AS RATIONALE,
        CURRENT_TIMESTAMP()
    FROM column_context ctx
    JOIN active_rules rule_range
        ON rule_range.RULE_ID = 'RANGE_NUMERIC'
       AND (
            rule_range.DATA_TYPE_FAMILY IN ('ANY', ctx.DATA_TYPE_FAMILY)
            OR rule_range.DATA_TYPE_FAMILY IS NULL
        )
       AND (
            rule_range.APPLICABILITY_TAGS IS NULL
            OR ARRAY_SIZE(rule_range.APPLICABILITY_TAGS) = 0
            OR ARRAY_SIZE(ARRAY_INTERSECTION(rule_range.APPLICABILITY_TAGS, ctx.CLASS_LABELS)) > 0
        )
    WHERE REGEXP_LIKE(ctx.DATA_TYPE, '^NUMBER')

    UNION ALL

    SELECT
        ctx.TABLE_FQN,
        ctx.COLUMN_NAME,
        'ENUM_SMALL_CARDINALITY' AS RULE_ID,
        rule_enum.CHECK_TYPE,
        rule_enum.DEFAULT_SEVERITY,
        'Only ' || COALESCE(ctx.DISTINCT_COUNT, 0) || ' distinct values detected, ideal for ENUM enforcement.' AS RATIONALE,
        CURRENT_TIMESTAMP()
    FROM column_context ctx
    JOIN active_rules rule_enum
        ON rule_enum.RULE_ID = 'ENUM_SMALL_CARDINALITY'
       AND (
            rule_enum.DATA_TYPE_FAMILY IN ('ANY', ctx.DATA_TYPE_FAMILY)
            OR rule_enum.DATA_TYPE_FAMILY IS NULL
        )
       AND (
            rule_enum.APPLICABILITY_TAGS IS NULL
            OR ARRAY_SIZE(rule_enum.APPLICABILITY_TAGS) = 0
            OR ARRAY_SIZE(ARRAY_INTERSECTION(rule_enum.APPLICABILITY_TAGS, ctx.CLASS_LABELS)) > 0
        )
    WHERE ctx.DISTINCT_COUNT <= 20

    UNION ALL

    SELECT
        ctx.TABLE_FQN,
        ctx.COLUMN_NAME,
        'NOT_FUTURE_DATE' AS RULE_ID,
        rule_date.CHECK_TYPE,
        rule_date.DEFAULT_SEVERITY,
        'Column classified as date content so values should not be in the future.' AS RATIONALE,
        CURRENT_TIMESTAMP()
    FROM column_context ctx
    JOIN active_rules rule_date
        ON rule_date.RULE_ID = 'NOT_FUTURE_DATE'
       AND (
            rule_date.DATA_TYPE_FAMILY IN ('ANY', ctx.DATA_TYPE_FAMILY)
            OR rule_date.DATA_TYPE_FAMILY IS NULL
        )
       AND (
            rule_date.APPLICABILITY_TAGS IS NULL
            OR ARRAY_SIZE(rule_date.APPLICABILITY_TAGS) = 0
            OR ARRAY_SIZE(ARRAY_INTERSECTION(rule_date.APPLICABILITY_TAGS, ctx.CLASS_LABELS)) > 0
        )
    WHERE ctx.CONTENT_TYPE = 'date'

    UNION ALL

    SELECT
        ctx.TABLE_FQN,
        ctx.COLUMN_NAME,
        'PATTERN_BASIC' AS RULE_ID,
        rule_pattern.CHECK_TYPE,
        rule_pattern.DEFAULT_SEVERITY,
        'Column tagged as code/identifier; enforce structural pattern.' AS RATIONALE,
        CURRENT_TIMESTAMP()
    FROM column_context ctx
    JOIN active_rules rule_pattern
        ON rule_pattern.RULE_ID = 'PATTERN_BASIC'
       AND (
            rule_pattern.DATA_TYPE_FAMILY IN ('ANY', ctx.DATA_TYPE_FAMILY)
            OR rule_pattern.DATA_TYPE_FAMILY IS NULL
        )
       AND (
            rule_pattern.APPLICABILITY_TAGS IS NULL
            OR ARRAY_SIZE(rule_pattern.APPLICABILITY_TAGS) = 0
            OR ARRAY_SIZE(ARRAY_INTERSECTION(rule_pattern.APPLICABILITY_TAGS, ctx.CLASS_LABELS)) > 0
        )
    WHERE ctx.CONTENT_TYPE ILIKE '%code%'
       OR ctx.CONTENT_TYPE ILIKE '%identifier%';
    v_inserted := SQLROWCOUNT;

    RETURN 'OK: suggestions=' || COALESCE(:v_inserted, 0) || ', cleared=' || COALESCE(:v_deleted, 0);
END;
$$;
