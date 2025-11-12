import os, pathlib


def build_sha():
    p = pathlib.Path("snapshots/BUILD_SHA.txt")
    return p.read_text().strip() if p.exists() else os.getenv("GIT_SHA", "unknown")


def build_time():
    p = pathlib.Path("snapshots/BUILD_TIME_UTC.txt")
    return p.read_text().strip() if p.exists() else "unknown"
