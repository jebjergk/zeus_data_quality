import os

# Defaults (adjust to your environment if different)
DEFAULT_METADATA_DB = "ZEUS_ANALYTICS_SIMU"
DEFAULT_METADATA_SCHEMA = "DISCOVERY"
DEFAULT_PROC_NAME = "SP_RUN_DQ_CONFIG"  # or "DQ_RUN_CONFIG" if that's what you deploy


def get_metadata_namespace():
    """
    Returns (db, schema) for metadata objects: DQ_CONFIG, DQ_CHECK, DQ_RUN_RESULTS,
    and the stored procedure that runs checks.
    Override via env vars if present.
    """
    db = os.getenv("DQ_METADATA_DB", DEFAULT_METADATA_DB)
    schema = os.getenv("DQ_METADATA_SCHEMA", DEFAULT_METADATA_SCHEMA)
    return db, schema


def get_proc_name():
    """
    Returns the stored procedure name used to run one config.
    Override via env var DQ_PROC_NAME.
    """
    return os.getenv("DQ_PROC_NAME", DEFAULT_PROC_NAME)


__all__ = [
    "DEFAULT_METADATA_DB",
    "DEFAULT_METADATA_SCHEMA",
    "DEFAULT_PROC_NAME",
    "get_metadata_namespace",
    "get_proc_name",
]
