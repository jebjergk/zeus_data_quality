-- Seeds core DSL-based rules into DQ_RULE_LIBRARY without impacting existing entries
-- Assumptions:
--   - DQ_RULE_LIBRARY schema matches ZEUS_ANALYTICS_SIMU.DISCOVERY with columns used below.
--   - PARAM_SCHEMA, DEFAULT_PARAMS, ALLOWED_DATA_TYPES, and ALLOWED_CLASSIFICATIONS accept VARIANT JSON.
MERGE INTO ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_RULE_LIBRARY AS target
USING (
    SELECT
        RULE_CODE,
        SCOPE,
        ENGINE_TYPE,
        EXPRESSION,
        PARSE_JSON(PARAM_SCHEMA_JSON) AS PARAM_SCHEMA,
        PARSE_JSON(DEFAULT_PARAMS_JSON) AS DEFAULT_PARAMS,
        PARSE_JSON(ALLOWED_DATA_TYPES_JSON) AS ALLOWED_DATA_TYPES,
        PARSE_JSON(ALLOWED_CLASSIFICATIONS_JSON) AS ALLOWED_CLASSIFICATIONS,
        CATEGORY,
        SEVERITY,
        ENABLED,
        VERSION,
        CHECK_TYPE
    FROM (
        SELECT * FROM VALUES
            (
                'NOT_NULL',
                'COLUMN',
                'DSL',
                'ASSERT NOT is_null(value)',
                '[]',
                '{}',
                '[]',
                '[]',
                'COMPLETENESS',
                'HIGH',
                TRUE,
                1,
                'COMPLETENESS'
            ),
            (
                'RANGE_CHECK',
                'COLUMN',
                'DSL',
                'ASSERT value BETWEEN param("min_value") AND param("max_value")',
                '[{"name":"min_value","type":"NUMBER","required":true},{"name":"max_value","type":"NUMBER","required":true}]',
                '{}',
                '[]',
                '[]',
                'VALIDITY',
                'MEDIUM',
                TRUE,
                1,
                'VALIDITY'
            ),
            (
                'REGEX_MATCH',
                'COLUMN',
                'DSL',
                'ASSERT matches(value, param("pattern"))',
                '[{"name":"pattern","type":"STRING","required":true}]',
                '{}',
                '[]',
                '[]',
                'VALIDITY',
                'MEDIUM',
                TRUE,
                1,
                'VALIDITY'
            ),
            (
                'IN_REFERENCE_TABLE',
                'COLUMN',
                'DSL',
                'ASSERT (is_null(value) AND param("allow_nulls")) OR lookup_exists(param("ref_table"), param("ref_key_column"), value)',
                '[{"name":"ref_table","type":"FQN_TABLE","required":true},{"name":"ref_key_column","type":"COLUMN_NAME","required":true},{"name":"allow_nulls","type":"BOOLEAN","required":false}]',
                '{"allow_nulls": true}',
                '[]',
                '[]',
                'REFERENTIAL_INTEGRITY',
                'HIGH',
                TRUE,
                1,
                'REFERENTIAL_INTEGRITY'
            ),
            (
                'TABLE_FRESHNESS_CHECK',
                'TABLE',
                'SQL',
                NULL,
                '[{"name":"timestamp_column","type":"STRING","required":true},{"name":"max_age_minutes","type":"NUMBER","required":true}]',
                '{}',
                '[]',
                '[]',
                'TIMELINESS',
                'HIGH',
                TRUE,
                1,
                'FRESHNESS'
            ),
            (
                'TABLE_ROWCOUNT_ANOMALY',
                'TABLE',
                'SQL',
                NULL,
                '[{"name":"timestamp_column","type":"STRING","required":true},{"name":"lookback_days","type":"NUMBER","required":true},{"name":"sensitivity","type":"NUMBER","required":true},{"name":"min_history_days","type":"NUMBER","required":true}]',
                '{}',
                '[]',
                '[]',
                'VALIDITY',
                'MEDIUM',
                TRUE,
                1,
                'ROW_COUNT_ANOMALY'
            )
            AS v(RULE_CODE, SCOPE, ENGINE_TYPE, EXPRESSION, PARAM_SCHEMA_JSON, DEFAULT_PARAMS_JSON, ALLOWED_DATA_TYPES_JSON, ALLOWED_CLASSIFICATIONS_JSON, CATEGORY, SEVERITY, ENABLED, VERSION, CHECK_TYPE)
    )
) AS source
ON target.RULE_CODE = source.RULE_CODE
WHEN MATCHED THEN UPDATE SET
    SCOPE = source.SCOPE,
    ENGINE_TYPE = source.ENGINE_TYPE,
    EXPRESSION = source.EXPRESSION,
    PARAM_SCHEMA = source.PARAM_SCHEMA,
    DEFAULT_PARAMS = source.DEFAULT_PARAMS,
    ALLOWED_DATA_TYPES = source.ALLOWED_DATA_TYPES,
    ALLOWED_CLASSIFICATIONS = source.ALLOWED_CLASSIFICATIONS,
    CATEGORY = source.CATEGORY,
    SEVERITY = source.SEVERITY,
    ENABLED = source.ENABLED,
    VERSION = source.VERSION,
    RULE_ID = COALESCE(target.RULE_ID, source.RULE_CODE),
    CHECK_TYPE = COALESCE(target.CHECK_TYPE, source.CHECK_TYPE),
    EXPRESSION_TEMPLATE = COALESCE(target.EXPRESSION_TEMPLATE, source.EXPRESSION),
    DEFAULT_SEVERITY = COALESCE(target.DEFAULT_SEVERITY, source.SEVERITY),
    ACTIVE = COALESCE(target.ACTIVE, source.ENABLED),
    UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    RULE_UID, RULE_ID, RULE_CODE, SCOPE, ENGINE_TYPE, EXPRESSION, PARAM_SCHEMA, DEFAULT_PARAMS,
    ALLOWED_DATA_TYPES, ALLOWED_CLASSIFICATIONS, CATEGORY, SEVERITY, ENABLED, VERSION,
    CHECK_TYPE, EXPRESSION_TEMPLATE, DEFAULT_SEVERITY, ACTIVE, CREATED_AT, UPDATED_AT
) VALUES (
    UUID_STRING(),
    source.RULE_CODE,
    source.RULE_CODE,
    source.SCOPE,
    source.ENGINE_TYPE,
    source.EXPRESSION,
    source.PARAM_SCHEMA,
    source.DEFAULT_PARAMS,
    source.ALLOWED_DATA_TYPES,
    source.ALLOWED_CLASSIFICATIONS,
    source.CATEGORY,
    source.SEVERITY,
    source.ENABLED,
    source.VERSION,
    source.CHECK_TYPE,
    source.EXPRESSION,
    source.SEVERITY,
    source.ENABLED,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);
