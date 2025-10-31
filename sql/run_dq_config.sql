-- DQ_RUN_CONFIG Stored Procedure
-- To execute in Snowsight: open a worksheet, set your context, and run:
--   CALL DQ_RUN_CONFIG('<CONFIG_ID>');
CREATE OR REPLACE PROCEDURE DQ_RUN_CONFIG(CONFIG_ID STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    RETURN 'OK: ' || :CONFIG_ID;
END;
$$;
