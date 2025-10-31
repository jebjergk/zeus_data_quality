# Contributing to Zeus Data Quality

Welcome! This document captures the guardrails that keep the Zeus Data Quality
Streamlit application stable. Please read it in full before proposing changes.

## Development workflow
- Create feature branches from `dev_bs` and rely on the **Auto PR & Squash
  Merge** workflow to integrate your work. Do not push directly to `main` or
  `dev_bs`.
- Keep existing CI workflows intact. If your change adds a new automated check,
  document how it should run locally.
- Write focused commits with descriptive messages. Prefer the imperative mood
  (e.g., `feat: add config preview card`).
- Pull requests must include:
  - A clear summary of the change and the affected areas.
  - A checklist of verification steps (tests, manual validation, screenshots).
  - Links to any related tickets or discussions.

## Coding style
- Python code follows PEP 8 with type hints. Favor explicit imports, small
  functions, and pure helpers in `utils/`.
- Streamlit UI components belong in `views/` and should reuse utilities from
  `utils/ui.py` whenever possible.
- Services in `services/` should remain idempotent and free of Streamlit calls;
  they are responsible for Snowflake interactions and business rules.
- SQL scripts in `sql/` must be formatted for readability and kept in sync with
  Snowflake stored procedures, tasks, and tables.
- Prefer adding tests or reproducible scripts when altering complex business
  logic.

## UI Contract and "Do-Not-Change" surfaces
The Streamlit UI constitutes a **UI Contract** with downstream teams. Key
layouts (navigation tabs, config editors, run history tables, and status badges)
are **Do-Not-Change** surfaces unless a product owner explicitly signs off.
Respect existing component names, query parameters, and the documented flows in
`views/`.

### No unsolicited UX refactors
Avoid broad UX or visual refactors without a product requirement. Incremental UI
improvements are welcomed only when they do not alter navigation, layout
hierarchy, or existing affordances.

### When in doubt, ask before changing UI/flows
Unsure if a change might affect the contract? Start a conversation in the
team's channel or reference the original specification ticket. Getting alignment
before coding prevents rework.

## Commit and PR expectations
- Reference tickets in commit messages when available (`ref #123`).
- Keep commits scoped so that reverting a single commit is safe.
- For pull requests, update all relevant documentation (architecture notes,
  changelog entries, onboarding docs) alongside the code change.
- If you touch any Snowflake object, mention required grants and deployment
  sequencing in the PR description.

## UI review checklist
Before requesting review on UI changes:
1. Verify the navigation still respects the `ALLOWED_PAGES` contract in
   `streamlit_app.py`.
2. Test the change across light/dark themes and narrow layouts.
3. Update screenshots or walkthrough docs if the visual flow changes.

Thank you for keeping the Zeus Data Quality experience consistent!
