# Scripts

This folder is reserved for local automation, developer bootstrap helpers, and repeatable operational scripts.

Prefer scripts that:

- are idempotent,
- log clearly,
- do not hide failures,
- match the agent-run delivery model.

Current operational helpers:

- `seed_reference_data.py`: loads the controlled reference seed bundle into a
  prepared database
- `build_uat_manifest.py`: builds the Phase 7 UAT manifest from seeds and the
  scenario pack
