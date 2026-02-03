# BRICK-OPS 🧱

Databricks Operations CLI

BRICK-OPS is an opinionated, interactive command-line tool for Databricks operations, focused on Jobs and Unity Catalog.
It is designed for data engineers and platform teams who want safe, fast, and consistent operational workflows — with previews, confirmations, and dry-runs by default.

---

## Features

### Databricks Jobs

- Search jobs using regular expressions
- Interactive selection (checkbox / radio prompts)
- Start jobs in parallel
- Watch runs with live status updates
- Full `--dry-run` support
- Rich tables for jobs, runs, and statuses

### Unity Catalog

- List catalogs, schemas, and tables
- Delete tables safely (owner → current user → delete)
- Delete schemas safely (owner → current user → tables → schema)
- Interactive previews before destructive actions
- Consistent confirmations and error handling
- Full `--dry-run` support
- Result tables with success / failure per object

---

## Installation

### Requirements

- Python 3.11+
- Databricks CLI
- Access to a Databricks workspace

### Using uv (from source)

```bash
git clone https://github.com/datarow1/db-ops.git
cd db-ops
uv venv
source .venv/bin/activate
uv pip install .
```

### Using homebrew (recommended)

```bash
brew tap datarow1/brick-ops
brew install brick-ops
```

Verify (after authentication - see below):

```bash
dbops --help
```

---

## Authentication

BRICK-OPS uses Databricks CLI authentication.

```bash
databricks auth login
```

Using a specific profile:

```bash
dbops jobs list --profile prod
```

---

## Command Overview

```text
dbops
├── jobs
│   ├── find
│   └── run
└── uc
    ├── catalogs-list
    ├── schemas-list
    ├── tables-list
    ├── tables-delete
    ├── tables-owner-set
    └── schema-delete
```

---

## Jobs – Examples

### Find jobs

```bash
dbops jobs find --name "python.*"
```

### Run jobs (interactive)

```bash
dbops jobs run --name "python.*"
```

### Dry-run

```bash
dbops jobs run --name "python.*" --dry-run
```

### Watch runs

```bash
dbops jobs run --name "python.*" --watch
```

---

## Unity Catalog – Examples

### List catalogs

```bash
dbops uc catalogs-list
```

### List schemas

```bash
dbops uc schemas-list --catalog main
```

### List tables

```bash
dbops uc tables-list --schema main.sales
```

With filters:

```bash
dbops uc tables-list --schema main.sales --name ".*tmp.*"
dbops uc tables-list --schema main.sales --owner you@company.com
dbops uc tables-list --schema main.sales --type VIEW
```

---

### Delete tables (safe by default)

```bash
dbops uc tables-delete --schema main.sales
```

What happens:

1. Preview of tables
2. Owner is set to the current user
3. Confirmation prompt
4. Tables are deleted

Dry-run:

```bash
dbops uc tables-delete --schema main.sales --dry-run
```

---

### Delete schema

```bash
dbops uc schema-delete main.sales
```

What happens:

1. Schema owner is set to the current user
2. All tables are deleted
3. Schema is deleted
4. Results are displayed in a summary table

---

## Interactive Prompts

### Checkbox prompts

- ↑ / ↓ — navigate
- space — toggle selection
- a — select all
- i — invert selection
- enter — confirm

### Confirm prompts

- y / n
- enter — accept default

All prompts are styled consistently with the BRICK-OPS theme.

---

## Project Structure

```text
db-ops/
├── src/
│   └── dbops/               # Single package
│       ├── core/            # Core logic
│       │   ├── adapters/
│       │   ├── selectors.py
│       │   ├── jobs.py
│       │   └── uc_models.py
│       └── cli/             # CLI
│           ├── commands/
│           ├── common/
│           │   ├── output.py
│           │   ├── progress.py
│           │   └── selector_builder.py
│           └── cli.py
├── pyproject.toml
└── README.md
```

---

## Design Principles

- Safety first  
  Always preview and confirm destructive actions.
- Dry-run everywhere  
  See what would happen before it happens.
- Single source of truth  
  Core contains logic, CLI handles orchestration.
- Consistent UX  
  All output goes through unified helpers.

---

## Exit Codes

| Code | Meaning                            |
| ---- | ---------------------------------- |
| 0    | Success                            |
| 1    | Runtime or API error               |
| 2    | Invalid input or missing arguments |

---

## Development

```bash
uv venv
source .venv/bin/activate
uv pip install -e .
uv pip install ruff pytest pre-commit
```

```bash
ruff check .
ruff format .
```

Run tests and a quick smoke check:

```bash
pytest
python -m dbops --help
```

Install commit message linting (Conventional Commits):

```bash
pre-commit install --hook-type commit-msg
```

Before merging checklist:

- `ruff check .`
- `pytest`
- `python -m dbops --help`
- Conventional Commit message on your commits (or use the pre-commit hook)

Make shortcuts:

- `make setup`
- `make check`
- `make precommit`

---

## Roadmap Ideas

- Permissions inspection
- Grants diff / apply
- Unity Catalog lineage inspection
- Homebrew distribution
- Read-only mode (`--readonly`)

---

## Disclaimer

BRICK-OPS performs destructive operations.
Always use `--dry-run` when in doubt.

---
