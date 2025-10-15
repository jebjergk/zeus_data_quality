"""Shared configuration defaults for metadata objects and procedures.

The utilities here provide the canonical database and schema locations
for metadata tables along with the stored procedure name used when
running DQ configs. Service modules (for example
``services.configs``) import these helpers so the defaults stay in one
place and can be overridden with environment variables.
"""

import os

# Defaults (adjust to your environment if different)
DEFAULT_METADATA_DB = "ZEUS_ANALYTICS_SIMU"
DEFAULT_METADATA_SCHEMA = "DISCOVERY"
DEFAULT_PROC_NAME = "DQ_RUN_CONFIG"


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
