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
    -- 1) Start from input template
    v_expr := IN_TEMPLATE;

    -- 2) Replace known placeholders with safe dummy literals
    -- column_expr -> numeric literal
    v_expr := REPLACE(v_expr, '{column_expr}', '1');

    -- numeric params
    v_expr := REPLACE(v_expr, '{min_value}', '0');
    v_expr := REPLACE(v_expr, '{max_value}', '100');

    -- pattern / allowed_values -> basic strings
    v_expr := REPLACE(v_expr, '{pattern}', '\''^.*$\'');
    v_expr := REPLACE(v_expr, '{allowed_values}', '\''A\'',\''B\'');

    -- 3) If any '{' remains, return an error about unresolved placeholders
    IF POSITION('{', v_expr) > 0 THEN
        RETURN 'ERROR: Unresolved placeholder(s) remain in expression: ' || v_expr;
    END IF;

    -- 4) Build a minimal SELECT that uses the expression as a boolean
    v_sql := 'SELECT CASE WHEN ' || v_expr || ' THEN 1 ELSE 0 END AS ok';

    -- 5) Try to execute it; catch any compilation/runtime error
    BEGIN
        EXECUTE IMMEDIATE :v_sql;
    EXCEPTION
        WHEN OTHER THEN
            RETURN 'ERROR: ' || SQLCODE || ' - ' || SQLERRM;
    END;

    RETURN 'OK';
END;
$$;
