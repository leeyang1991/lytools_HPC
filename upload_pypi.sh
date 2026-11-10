#!/bin/sh
# Run with: sh upload_pypi.sh
# Requires: python -m pip install setuptools wheel twine
set -eu

# Resolve project files relative to this script, even when invoked elsewhere.
PROJECT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$PROJECT_DIR"

# Build into a fresh directory so stale artifacts cannot be uploaded.
RELEASE_DIR=$(mktemp -d "${TMPDIR:-/tmp}/lytools-release.XXXXXX")
trap 'rm -rf "$RELEASE_DIR"' EXIT HUP INT TERM

python setup.py sdist --dist-dir "$RELEASE_DIR" bdist_wheel --dist-dir "$RELEASE_DIR"
python -m twine check "$RELEASE_DIR"/*
python -m twine upload --repository pypi "$RELEASE_DIR"/*
