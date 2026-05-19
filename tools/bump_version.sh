#!/usr/bin/env bash
# bump_version.sh — Update version across all FluxTrade manifests.
# Usage: ./tools/bump_version.sh <new-version>
# Example: ./tools/bump_version.sh 0.2.0
#
# Updates:
#   - VERSION (single source of truth)
#   - python-strategy/pyproject.toml
#   - rust-data-service/Cargo.toml
#
# Does NOT commit — you decide when to commit.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <new-version>"
    echo "Example: $0 0.2.0"
    exit 1
fi

NEW_VERSION="$1"

# Validate semver format (major.minor.patch, optional pre-release)
if ! echo "$NEW_VERSION" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$'; then
    echo "Error: '$NEW_VERSION' is not valid semver (expected: X.Y.Z or X.Y.Z-pre)"
    exit 1
fi

OLD_VERSION="$(cat "$REPO_ROOT/VERSION" | tr -d '[:space:]')"

if [[ "$OLD_VERSION" == "$NEW_VERSION" ]]; then
    echo "Version is already $NEW_VERSION — nothing to do."
    exit 0
fi

echo "Bumping version: $OLD_VERSION -> $NEW_VERSION"
echo ""

# 1. VERSION file
printf '%s\n' "$NEW_VERSION" > "$REPO_ROOT/VERSION"
echo "  Updated VERSION"

# 2. pyproject.toml
PYPROJECT="$REPO_ROOT/python-strategy/pyproject.toml"
if [[ -f "$PYPROJECT" ]]; then
    sed -i '' "s/^version = \"$OLD_VERSION\"/version = \"$NEW_VERSION\"/" "$PYPROJECT"
    echo "  Updated python-strategy/pyproject.toml"
else
    echo "  WARNING: $PYPROJECT not found, skipped"
fi

# 3. Cargo.toml
CARGO="$REPO_ROOT/rust-data-service/Cargo.toml"
if [[ -f "$CARGO" ]]; then
    sed -i '' "s/^version = \"$OLD_VERSION\"/version = \"$NEW_VERSION\"/" "$CARGO"
    echo "  Updated rust-data-service/Cargo.toml"
else
    echo "  WARNING: $CARGO not found, skipped"
fi

echo ""
echo "Done. Review changes with: git diff"
