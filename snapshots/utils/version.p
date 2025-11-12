import os
import pathlib


def build_sha() -> str:
    p = pathlib.Path("snapshots/BUILD_SHA.txt")
    return p.read_text().strip() if p.exists() else os.getenv("GIT_SHA", "unknown")


def build_time() -> str:
    p = pathlib.Path("snapshots/BUILD_TIME_UTC.txt")
    return p.read_text().strip() if p.exists() else "unknown"
