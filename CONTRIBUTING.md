# Contributing

## Code of Conduct

Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md).

## Reporting bugs

Include the following when opening a bug report:

- Python version (`python --version`)
- OS and version
- Minimal reproduction case
- Full traceback if applicable

## Opening an issue first

For non-trivial changes — new features, API changes, significant refactors — open an
issue to discuss the approach before starting work. This avoids effort on a direction
that may not be accepted.

## Development setup

```bash
git clone https://github.com/adetokunbo/triggr
cd triggr
nix-shell   # or: pip install -e ".[dev]"
```

## Pre-commit hooks

Install the hooks once after cloning:

```bash
pre-commit install
```

They run automatically on `git commit` and check:

- **ruff** — linting and formatting
- **basedpyright** — type checking

To run them manually across all files:

```bash
pre-commit run --all-files
```

## Running tests

```bash
pytest
```

## Type checking

```bash
basedpyright
```

## Before submitting a PR

- All tests pass
- `pre-commit run --all-files` is clean
- New behaviour is covered by tests
- `CHANGELOG.md` is updated under the `Unreleased` section

## Branch naming and PR scope

Branch from `main`. Keep PRs focused — one concern per PR. Name branches
descriptively, e.g. `fix/polling-loop-timeout` or `feat/error-classifier-protocol`.

## Submitting changes

Open a pull request against `main`. Reference the related issue if one exists.
