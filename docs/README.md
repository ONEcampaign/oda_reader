# ODA Reader Documentation

This directory contains the MkDocs documentation for ODA Reader.

## Building Locally

Install dependencies:

```bash
cd ..  # back to project root
uv sync --group docs
```

Serve locally:

```bash
cd docs
uv run mkdocs serve
```

Visit http://127.0.0.1:8000

## Building for Production

```bash
cd docs
uv run mkdocs build
```

Output is in `site/` directory.

## Testing Documentation Examples

Run example test scripts:

```bash
cd ..  # back to project root
uv run python docs/examples/getting_started_examples.py
uv run python docs/examples/filtering_examples.py
```

All examples should pass before committing documentation updates.

## Documentation Structure

- `mkdocs.yml` - MkDocs configuration
- `docs/` - All markdown content
- `examples/` - Test scripts for documentation examples
- `plans/` - Design documents and implementation plans

## Deployment

(Add deployment instructions for your hosting platform here)
