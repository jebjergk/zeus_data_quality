-- RUN_DQ_CONFIG Stored Procedure
-- To execute in Snowsight: open a worksheet, set your context, and run:
--   CALL RUN_DQ_CONFIG('<CONFIG_ID>');
create or replace procedure RUN_DQ_CONFIG(CONFIG_ID STRING)
returns STRING
language SQL
as
$$
begin
  return 'OK: ' || :CONFIG_ID;
end;
$$;
