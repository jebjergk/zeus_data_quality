-- Stored procedure to validate rule expression templates
CREATE OR REPLACE PROCEDURE ZEUS_ANALYTICS_SIMU.DISCOVERY.DQ_VALIDATE_RULE_TEMPLATE(
    IN_TEMPLATE STRING
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_expr STRING;
    v_sql  STRING;
BEGIN
    -- Start with incoming template
    v_expr := IN_TEMPLATE;

    -- Replace placeholders with safe dummy literals
    v_expr := REPLACE(v_expr, '{column_expr}', '1');
    v_expr := REPLACE(v_expr, '{min_value}', '0');
    v_expr := REPLACE(v_expr, '{max_value}', '100');

    -- Regex pattern placeholder → safe string literal (double single quotes!)
    v_expr := REPLACE(v_expr, '{pattern}', '''^.*$''');

    -- Allowed values placeholder → safe stub (comma-separated literals)
    v_expr := REPLACE(v_expr, '{allowed_values}', '''A'',''B''');

    -- Detect any unresolved placeholders
    IF (POSITION('{', v_expr) > 0) THEN
        RETURN 'ERROR: Unresolved placeholder in expression: ' || v_expr;
    END IF;

    -- Build simple SELECT to validate
    v_sql := 'SELECT CASE WHEN ' || v_expr || ' THEN 1 ELSE 0 END AS ok';

    BEGIN
        EXECUTE IMMEDIATE v_sql;
    EXCEPTION
        WHEN OTHER THEN
            RETURN 'ERROR: ' || SQLERRM || ' (SQLCODE=' || SQLCODE || ')';
    END;

    RETURN 'OK';
END;
$$;
