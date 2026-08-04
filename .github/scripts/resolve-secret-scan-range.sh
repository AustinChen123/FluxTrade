#!/usr/bin/env bash
set -euo pipefail

before_sha=${1-}
head_sha=${2-}
baseline_ref=${3-}
zero_sha=0000000000000000000000000000000000000000

is_sha() {
  [[ $1 =~ ^[0-9a-fA-F]{40}$ ]]
}

if ! is_sha "$before_sha" || ! is_sha "$head_sha"; then
  echo "Invalid commit SHA" >&2
  exit 1
fi

git cat-file -e "${head_sha}^{commit}"

if [[ $before_sha != "$zero_sha" ]]; then
  git cat-file -e "${before_sha}^{commit}"
  printf '%s..%s\n' "$before_sha" "$head_sha"
  exit 0
fi

if [[ $baseline_ref != refs/remotes/origin/develop ]]; then
  echo "Invalid first-push baseline ref" >&2
  exit 1
fi
git cat-file -e "${baseline_ref}^{commit}"
merge_base=$(git merge-base "$baseline_ref" "$head_sha")
printf '%s..%s\n' "$merge_base" "$head_sha"
