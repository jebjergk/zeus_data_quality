# Zeus DQ App — Branching & PR Workflow

This workflow ensures predictable, non-destructive iteration.

---

## Permanent Branches

| Branch | Purpose |
|---|---|
| `V1.3-profiling` | **The canonical baseline**. Always stable. |
| Feature branches | Short-lived branches for individual tasks. |

---

## Rules

1. **Never commit directly to `V1.3-profiling`.**
2. Each task = **one** feature branch:

git checkout -b feat/profile-avg-len-fix

yaml
Copy code

3. Codex / GPT makes changes → submit PR → human reviews → merge back.

4. After merge:
   - Copy updated files into Snowflake Streamlit.
   - Smoke test UI.
   - You **may delete** the feature branch.

---

## Multi-Agent Workflow (Safe Parallel Work)

Agent 1 → UI changes only (views/*.py)
Agent 2 → Profiling engine only (services/profiling.py)
Agent 3 → DQ editor only (views/config_editor.py + services/configs.py)

yaml
Copy code

**Agents do not overlap file scopes.**

---

## Example Commit Message Standard

feat(profile): fix avg_len handling for numeric-as-text, restore confidence colors

Copy code
