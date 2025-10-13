-- DQ_RUN_CONFIG Stored Procedure
-- To execute in Snowsight: open a worksheet, set your context, and run:
--   CALL DQ_RUN_CONFIG('<CONFIG_ID>');
create or replace procedure DQ_RUN_CONFIG(CONFIG_ID STRING)
returns STRING
language SQL
as
$$
begin
  return 'OK: ' || :CONFIG_ID;
end;
$$;
