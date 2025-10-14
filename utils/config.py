import os

# Defaults (adjust to your environment if different)
DEFAULT_METADATA_DB = "ZEUS_ANALYTICS_SIMU"
DEFAULT_METADATA_SCHEMA = "DISCOVERY"
DEFAULT_PROC_NAME = "SP_RUN_DQ_CONFIG"  # keep consistent with your deployment

def get_metadata_namespace():
    """Return the database and schema used for metadata objects."""

    db = os.getenv("DQ_METADATA_DB", DEFAULT_METADATA_DB)
    schema = os.getenv("DQ_METADATA_SCHEMA", DEFAULT_METADATA_SCHEMA)
    return db, schema

def get_proc_name():
    """Return the stored procedure name used to execute DQ configurations."""

    return os.getenv("DQ_PROC_NAME", DEFAULT_PROC_NAME)
