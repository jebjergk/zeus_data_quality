# tools/mirror_snapshots.py
# Mirror Python source files into /snapshots with a .p extension.
import shutil, pathlib

ROOT = pathlib.Path(__file__).resolve().parents[1]
EXCLUDE_DIRS = {'.git', '.github', 'venv', '.venv', '__pycache__', 'snapshots'}
OUT = ROOT / 'snapshots'

def mirror_file(src: pathlib.Path):
    rel = src.relative_to(ROOT)
    dest = OUT / rel
    dest = dest.with_suffix('.p')  # .py -> .p
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dest)

def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for p in ROOT.rglob('*.py'):
        if any(part in EXCLUDE_DIRS for part in p.parts):
            continue
        mirror_file(p)

if __name__ == "__main__":
    main()
