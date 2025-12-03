-- Minimal compiler for DSL-based rules. Profiling and DQ Config will call this API later; it is intentionally standalone for now.
CREATE OR REPLACE PROCEDURE ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_COMPILE_RULE_SQL(
    RULE_CODE STRING,
    TARGET_TABLE_FQN STRING,
    TARGET_COLUMNS ARRAY,
    PARAM_VALUES VARIANT
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
// Helper to safely quote SQL identifiers
function quoteIdent(name) {
    if (!name) {
        throw new Error('Column or identifier name is required');
    }
    return '"' + name.replace(/"/g, '""') + '"';
}

// Helper to render SQL literals from parameters
function toSqlLiteral(value) {
    if (value === null || value === undefined) {
        return 'NULL';
    }
    if (typeof value === 'number') {
        return value.toString();
    }
    if (typeof value === 'boolean') {
        return value ? 'TRUE' : 'FALSE';
    }
    return '\'' + value.toString().replace(/'/g, "''") + '\'';
}

function fetchRule(ruleCode) {
    const stmt = snowflake.createStatement({
        sqlText: `SELECT RULE_CODE, ENGINE_TYPE, SCOPE, EXPRESSION, DEFAULT_PARAMS FROM ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_RULE_LIBRARY WHERE RULE_CODE = ?`,
        binds: [ruleCode]
    });
    const rs = stmt.execute();
    if (!rs.next()) {
        throw new Error(`Rule not found for RULE_CODE=${ruleCode}`);
    }
    return {
        rule_code: rs.getColumnValue('RULE_CODE'),
        engine_type: rs.getColumnValue('ENGINE_TYPE'),
        scope: rs.getColumnValue('SCOPE'),
        expression: rs.getColumnValue('EXPRESSION'),
        default_params: rs.getColumnValue('DEFAULT_PARAMS') || {}
    };
}

function getParamValue(paramValues, defaults, name, fallback) {
    if (paramValues && Object.prototype.hasOwnProperty.call(paramValues, name)) {
        return paramValues[name];
    }
    if (defaults && Object.prototype.hasOwnProperty.call(defaults, name)) {
        return defaults[name];
    }
    return fallback;
}

function compile(ruleCode, targetTable, targetColumns, paramValues) {
    const rule = fetchRule(ruleCode);
    if (rule.engine_type !== 'DSL') {
        throw new Error(`Rule ${ruleCode} is not supported by DSL engine`);
    }

    const columns = Array.isArray(targetColumns) ? targetColumns : [];
    if (columns.length === 0) {
        throw new Error('At least one target column is required');
    }
    const columnIdent = quoteIdent(columns[0]);
    const tableIdent = targetTable;
    const params = paramValues || {};

    let violationCondition;
    switch (rule.rule_code) {
        case 'NOT_NULL':
            violationCondition = `${columnIdent} IS NULL`;
            break;
        case 'RANGE_CHECK': {
            const minVal = getParamValue(params, rule.default_params, 'min_value', null);
            const maxVal = getParamValue(params, rule.default_params, 'max_value', null);
            if (minVal === null || maxVal === null) {
                throw new Error('RANGE_CHECK requires min_value and max_value parameters');
            }
            violationCondition = `NOT (${columnIdent} BETWEEN ${toSqlLiteral(minVal)} AND ${toSqlLiteral(maxVal)})`;
            break;
        }
        case 'REGEX_MATCH': {
            const pattern = getParamValue(params, rule.default_params, 'pattern', null);
            if (!pattern) {
                throw new Error('REGEX_MATCH requires a pattern parameter');
            }
            violationCondition = `NOT REGEXP_LIKE(${columnIdent}, ${toSqlLiteral(pattern)})`;
            break;
        }
        case 'IN_REFERENCE_TABLE': {
            const allowNulls = !!getParamValue(params, rule.default_params, 'allow_nulls', false);
            const refTable = getParamValue(params, rule.default_params, 'ref_table', null);
            const refKeyColumn = getParamValue(params, rule.default_params, 'ref_key_column', null);
            if (!refTable || !refKeyColumn) {
                throw new Error('IN_REFERENCE_TABLE requires ref_table and ref_key_column parameters');
            }
            const refKeyIdent = quoteIdent(refKeyColumn);
            const nullClause = allowNulls ? `(${columnIdent} IS NULL)` : 'FALSE';
            const lookupClause = `EXISTS (SELECT 1 FROM ${refTable} WHERE ${refKeyIdent} = ${columnIdent})`;
            violationCondition = `NOT ( ${nullClause} OR ${lookupClause} )`;
            break;
        }
        default:
            throw new Error(`Rule ${ruleCode} is not supported by the compiler`);
    }

    const violationQuery = `SELECT t.* FROM ${tableIdent} AS t WHERE ${violationCondition}`;
    return {
        rule_code: rule.rule_code,
        engine_type: rule.engine_type,
        scope: rule.scope,
        expression: rule.expression,
        violation_query: violationQuery
    };
}

// The compiler is intentionally minimal; profiling and DQ Config layers will invoke it later.
return compile(RULE_CODE, TARGET_TABLE_FQN, TARGET_COLUMNS, PARAM_VALUES);
$$;
