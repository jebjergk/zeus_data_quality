# zeus_data_quality
Inrerim Data Quiality Solution for Zeus, till we have a solution via governance tool

## Source snapshots for copy/paste
To view code in environments where `.py` is blocked, this repo generates read-only mirrors under `/snapshots` with `.p` extension.

Run:
  python -m tools snapshot

Outputs:
  - snapshots/<same structure>.p
  - snapshots/SOURCE_SNAPSHOT.md

Do not edit files in `/snapshots`; they are generated from the real sources.
