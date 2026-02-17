# Contributing to financial-scraper

Thanks for your interest in contributing! This guide will help you get started.

## Development Setup

```bash
git clone https://github.com/HydrAI/financial-scraper.git
cd financial-scraper/financial_scraper
pip install -e ".[dev]"
```

## Running Tests

```bash
cd financial_scraper
python -m pytest tests/ -v --cov
```

All tests must pass before submitting a PR. Current coverage target: **85%+**.

## Code Style

- Follow existing patterns in the codebase
- Use type hints for function signatures
- Keep modules focused, one responsibility per file
- Async functions should use `aiohttp`, not `requests`
- All network-facing code must respect `robots.txt` by default

## Pull Request Workflow

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes and add tests
4. Run the test suite to confirm nothing breaks
5. Commit with a clear message describing the change
6. Push to your fork and open a PR against `main`

## Reporting Issues

Use [GitHub Issues](https://github.com/HydrAI/financial-scraper/issues) to report bugs or request features. Include:

- Python version and OS
- Steps to reproduce
- Expected vs actual behavior
- Relevant log output

## Architecture

See [docs/architecture.md](docs/architecture.md) for an overview of the pipeline design and module responsibilities.
