-- Seeds core DSL-based rules into DQ_RULE_LIBRARY without impacting existing entries
MERGE INTO ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_RULE_LIBRARY AS target
USING (
    SELECT * FROM VALUES
        ('NOT_NULL', 'COLUMN', 'DSL', 'ASSERT NOT is_null(value)', PARSE_JSON('[]'), PARSE_JSON('{}'), PARSE_JSON('[]'), PARSE_JSON('[]'), 'Completeness', 'HIGH', TRUE, 1),
        ('RANGE_CHECK', 'COLUMN', 'DSL', 'ASSERT value BETWEEN param("min_value") AND param("max_value")', PARSE_JSON('["min_value","max_value"]'), PARSE_JSON('{}'), PARSE_JSON('[]'), PARSE_JSON('[]'), 'Validity', 'MEDIUM', TRUE, 1),
        ('REGEX_MATCH', 'COLUMN', 'DSL', 'ASSERT matches(value, param("pattern"))', PARSE_JSON('["pattern"]'), PARSE_JSON('{}'), PARSE_JSON('[]'), PARSE_JSON('[]'), 'Validity', 'MEDIUM', TRUE, 1),
        ('IN_REFERENCE_TABLE', 'COLUMN', 'DSL', 'ASSERT (is_null(value) AND param("allow_nulls")) OR lookup_exists(param("ref_table"), param("ref_key_column"), value)', PARSE_JSON('["allow_nulls","ref_table","ref_key_column"]'), PARSE_JSON('{"allow_nulls": false}'), PARSE_JSON('[]'), PARSE_JSON('[]'), 'Consistency', 'HIGH', TRUE, 1)
        AS v(RULE_CODE, SCOPE, ENGINE_TYPE, EXPRESSION, PARAM_SCHEMA, DEFAULT_PARAMS, ALLOWED_DATA_TYPES, ALLOWED_CLASSIFICATIONS, CATEGORY, SEVERITY, ENABLED, VERSION)
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
    CHECK_TYPE = COALESCE(target.CHECK_TYPE, source.CATEGORY),
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
    source.CATEGORY,
    source.EXPRESSION,
    source.SEVERITY,
    source.ENABLED,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);
