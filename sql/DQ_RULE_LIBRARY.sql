-- DQ_RULE_LIBRARY DDL and seed data
-- Ensures ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_RULE_LIBRARY supports dynamic rule templates

CREATE OR REPLACE TABLE ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_RULE_LIBRARY (
    RULE_ID VARCHAR PRIMARY KEY,
    CHECK_TYPE VARCHAR,
    EXPRESSION_TEMPLATE VARCHAR,
    PARAM_SCHEMA VARIANT,
    DEFAULT_SEVERITY VARCHAR,
    DESCRIPTION VARCHAR,
    ACTIVE BOOLEAN DEFAULT TRUE,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

MERGE INTO ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_RULE_LIBRARY AS target
USING (
    SELECT
        RULE_ID,
        CHECK_TYPE,
        EXPRESSION_TEMPLATE,
        PARSE_JSON(PARAM_SCHEMA_JSON) AS PARAM_SCHEMA,
        DEFAULT_SEVERITY,
        DESCRIPTION,
        ACTIVE
    FROM (
        SELECT
            'NOT_NULL_BASIC' AS RULE_ID,
            'NOT_NULL' AS CHECK_TYPE,
            '({column_expr} IS NOT NULL)' AS EXPRESSION_TEMPLATE,
            '[]' AS PARAM_SCHEMA_JSON,
            'HIGH' AS DEFAULT_SEVERITY,
            'Basic non-null check' AS DESCRIPTION,
            TRUE AS ACTIVE
        UNION ALL SELECT
            'RANGE_NUMERIC',
            'RANGE',
            '({column_expr} BETWEEN {min_value} AND {max_value})',
            '["min_value","max_value"]',
            'MEDIUM',
            'Numeric allowed range',
            TRUE
        UNION ALL SELECT
            'ENUM_SMALL_CARDINALITY',
            'ENUM',
            '({column_expr} IN ({allowed_values}))',
            '["allowed_values"]',
            'MEDIUM',
            'Enumeration check for low-cardinality columns',
            TRUE
        UNION ALL SELECT
            'NOT_FUTURE_DATE',
            'DATE_CHECK',
            '({column_expr} <= CURRENT_DATE())',
            '[]',
            'HIGH',
            'Values cannot be in the future',
            TRUE
        UNION ALL SELECT
            'PATTERN_BASIC',
            'PATTERN',
            '({column_expr} REGEXP {pattern})',
            '["pattern"]',
            'LOW',
            'Regex pattern match',
            TRUE
    )
) AS source
ON target.RULE_ID = source.RULE_ID
WHEN MATCHED THEN UPDATE SET
    CHECK_TYPE = source.CHECK_TYPE,
    EXPRESSION_TEMPLATE = source.EXPRESSION_TEMPLATE,
    PARAM_SCHEMA = source.PARAM_SCHEMA,
    DEFAULT_SEVERITY = source.DEFAULT_SEVERITY,
    DESCRIPTION = source.DESCRIPTION,
    ACTIVE = source.ACTIVE,
    UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    RULE_ID,
    CHECK_TYPE,
    EXPRESSION_TEMPLATE,
    PARAM_SCHEMA,
    DEFAULT_SEVERITY,
    DESCRIPTION,
    ACTIVE
) VALUES (
    source.RULE_ID,
    source.CHECK_TYPE,
    source.EXPRESSION_TEMPLATE,
    source.PARAM_SCHEMA,
    source.DEFAULT_SEVERITY,
    source.DESCRIPTION,
    source.ACTIVE
);
