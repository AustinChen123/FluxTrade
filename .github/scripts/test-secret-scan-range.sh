#!/usr/bin/env bash
set -euo pipefail

resolver=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/resolve-secret-scan-range.sh
test_dir=$(mktemp -d)
trap 'rm -rf -- "$test_dir"' EXIT

run_resolver() {
  (cd "$test_dir" && bash "$resolver" "$@")
}

git -C "$test_dir" init --quiet
git -C "$test_dir" config user.name "CI Test"
git -C "$test_dir" config user.email "ci-test@example.invalid"

printf 'baseline\n' >"$test_dir/history.txt"
git -C "$test_dir" add history.txt
git -C "$test_dir" commit --quiet -m baseline
baseline=$(git -C "$test_dir" rev-parse HEAD)
git -C "$test_dir" update-ref refs/remotes/origin/develop "$baseline"

printf 'range-marker\n' >"$test_dir/marker.txt"
git -C "$test_dir" add marker.txt
git -C "$test_dir" commit --quiet -m slice-one
slice_one=$(git -C "$test_dir" rev-parse HEAD)

printf 'later\n' >"$test_dir/later.txt"
git -C "$test_dir" add later.txt
git -C "$test_dir" commit --quiet -m slice-two
head_sha=$(git -C "$test_dir" rev-parse HEAD)

zero_sha=0000000000000000000000000000000000000000
first_range=$(run_resolver "$zero_sha" "$head_sha" refs/remotes/origin/develop)
[[ $first_range == "$baseline..$head_sha" ]]
[[ $(git -C "$test_dir" rev-list --count "$first_range") == 2 ]]
git -C "$test_dir" rev-list "$first_range" | grep -Fxq "$slice_one"
git -C "$test_dir" diff "$first_range" -- marker.txt | grep -Fq range-marker

later_range=$(run_resolver "$slice_one" "$head_sha" refs/remotes/origin/develop)
[[ $later_range == "$slice_one..$head_sha" ]]
[[ $(git -C "$test_dir" rev-list --count "$later_range") == 1 ]]
[[ $(git -C "$test_dir" rev-list "$later_range") == "$head_sha" ]]

git -C "$test_dir" update-ref -d refs/remotes/origin/develop
if run_resolver "$zero_sha" "$head_sha" refs/remotes/origin/develop >/dev/null 2>&1; then
  echo "Missing baseline unexpectedly succeeded" >&2
  exit 1
fi

empty_tree=$(git -C "$test_dir" mktree </dev/null)
orphan=$(printf 'orphan\n' | git -C "$test_dir" commit-tree "$empty_tree")
git -C "$test_dir" update-ref refs/remotes/origin/develop "$orphan"
if run_resolver "$zero_sha" "$head_sha" refs/remotes/origin/develop >/dev/null 2>&1; then
  echo "Unrelated baseline unexpectedly succeeded" >&2
  exit 1
fi

if run_resolver malformed "$head_sha" refs/remotes/origin/develop >/dev/null 2>&1; then
  echo "Malformed input unexpectedly succeeded" >&2
  exit 1
fi

echo "Secret scan range tests passed"
