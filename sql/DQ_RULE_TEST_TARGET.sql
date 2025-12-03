-- Helper table for DQ rule test compilation
-- Intended to provide a minimal target for DQ_COMPILE_RULE_SQL
CREATE TABLE IF NOT EXISTS "{METADATA_DB}"."{METADATA_SCHEMA}"."DQ_RULE_TEST_TARGET" (
    DUMMY_COL VARCHAR
);
