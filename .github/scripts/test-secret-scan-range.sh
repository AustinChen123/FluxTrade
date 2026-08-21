#!/usr/bin/env bash
set -euo pipefail

resolver=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/resolve-secret-scan-range.sh
test_dir=$(mktemp -d)
trap 'rm -rf -- "$test_dir"' EXIT

run_resolver() {
  (cd "$test_dir" && bash "$resolver" "$@")
}

expect_failure() {
  expected_error=$1
  shift
  if run_resolver "$@" >"$test_dir/rejected.stdout" 2>"$test_dir/rejected.stderr"; then
    echo "Invalid resolver input unexpectedly succeeded: $*" >&2
    exit 1
  fi
  [[ $(wc -c <"$test_dir/rejected.stdout") -eq 0 ]]
  [[ $(wc -l <"$test_dir/rejected.stdout") -eq 0 ]]
  [[ $(wc -l <"$test_dir/rejected.stderr") -eq 1 ]]
  printf '%s\n' "$expected_error" | cmp -s - "$test_dir/rejected.stderr"
}

expect_success() {
  expected_range=$1
  expected_audit=$2
  shift 2
  run_resolver "$@" >"$test_dir/accepted.stdout" 2>"$test_dir/accepted.stderr"
  [[ $(wc -l <"$test_dir/accepted.stdout") -eq 1 ]]
  [[ $(wc -l <"$test_dir/accepted.stderr") -eq 1 ]]
  printf '%s\n' "$expected_range" | cmp -s - "$test_dir/accepted.stdout"
  printf '%s\n' "$expected_audit" | cmp -s - "$test_dir/accepted.stderr"
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
first_range=$baseline..$head_sha
expect_success "$first_range" "secret_scan_range event=push created=true ref=refs/heads/feature raw_before=$zero_sha resolved_base=$baseline head=$head_sha range=$first_range commit_count=2" \
  push true refs/heads/feature "$zero_sha" "$head_sha" refs/remotes/origin/develop
[[ $(git -C "$test_dir" rev-list --count "$first_range") == 2 ]]
git -C "$test_dir" rev-list "$first_range" | grep -Fxq "$slice_one"
git -C "$test_dir" diff "$first_range" -- marker.txt | grep -Fq range-marker

later_range=$slice_one..$head_sha
expect_success "$later_range" "secret_scan_range event=push created=false ref=refs/heads/feature raw_before=$slice_one resolved_base=$slice_one head=$head_sha range=$later_range commit_count=1" \
  push false refs/heads/feature "$slice_one" "$head_sha" refs/remotes/origin/develop
[[ $(git -C "$test_dir" rev-list --count "$later_range") == 1 ]]
[[ $(git -C "$test_dir" rev-list "$later_range") == "$head_sha" ]]

pr_range=$slice_one..$head_sha
expect_success "$pr_range" "secret_scan_range event=pull_request created=empty ref=refs/pull/1/merge raw_before=$slice_one resolved_base=$slice_one head=$head_sha range=$pr_range commit_count=1" \
  pull_request '' refs/pull/1/merge "$slice_one" "$head_sha" refs/remotes/origin/develop

expect_failure "secret_scan_range_error code=INVALID_EVENT_IDENTITY field=before_sha event=push created=false ref=refs/heads/feature before=$zero_sha head=$head_sha" push false refs/heads/feature "$zero_sha" "$head_sha" refs/remotes/origin/develop
expect_failure "secret_scan_range_error code=INVALID_EVENT_IDENTITY field=before_sha event=push created=true ref=refs/heads/feature before=$slice_one head=$head_sha" push true refs/heads/feature "$slice_one" "$head_sha" refs/remotes/origin/develop
expect_failure "secret_scan_range_error code=INVALID_EVENT_CREATED field=event_created event=push" push $'false\ninjected=true' refs/heads/feature "$slice_one" "$head_sha" refs/remotes/origin/develop
expect_failure "secret_scan_range_error code=INVALID_EVENT_CREATED field=event_created event=pull_request" pull_request true refs/pull/1/merge "$slice_one" "$head_sha" refs/remotes/origin/develop
expect_failure "secret_scan_range_error code=INVALID_EVENT_IDENTITY field=before_sha event=pull_request created=empty ref=refs/pull/1/merge before=$zero_sha head=$head_sha" pull_request '' refs/pull/1/merge "$zero_sha" "$head_sha" refs/remotes/origin/develop
expect_failure "secret_scan_range_error code=INVALID_EVENT_NAME field=event_name" $'workflow_dispatch\ninjected=true' '' refs/heads/feature "$slice_one" "$head_sha" refs/remotes/origin/develop
expect_failure "secret_scan_range_error code=INVALID_EVENT_REF field=event_ref event=push created=false" push false $'refs/heads/bad\ninjected=true' "$slice_one" "$head_sha" refs/remotes/origin/develop
expect_failure "secret_scan_range_error code=INVALID_EVENT_REF_NAMESPACE field=event_ref event=push created=false ref=refs/pull/1/merge" push false refs/pull/1/merge "$slice_one" "$head_sha" refs/remotes/origin/develop
expect_failure "secret_scan_range_error code=INVALID_EVENT_REF_NAMESPACE field=event_ref event=pull_request created=empty ref=refs/heads/feature" pull_request '' refs/heads/feature "$slice_one" "$head_sha" refs/remotes/origin/develop
expect_failure "secret_scan_range_error code=INVALID_EVENT_REF_NAMESPACE field=event_ref event=pull_request created=empty ref=refs/pull/0/merge" pull_request '' refs/pull/0/merge "$slice_one" "$head_sha" refs/remotes/origin/develop
expect_failure "secret_scan_range_error code=INVALID_COMMIT_SHA field=before_sha event=push created=false ref=refs/heads/feature head=$head_sha" push false refs/heads/feature $'malformed\ninjected=true' "$head_sha" refs/remotes/origin/develop
expect_failure "secret_scan_range_error code=INVALID_COMMIT_SHA field=head_sha event=push created=false ref=refs/heads/feature" push false refs/heads/feature "$slice_one" malformed refs/remotes/origin/develop
expect_failure "secret_scan_range_error code=INVALID_BASELINE_REF field=baseline_ref event=push created=true ref=refs/heads/feature head=$head_sha" push true refs/heads/feature "$zero_sha" "$head_sha" refs/remotes/origin/main

missing=1111111111111111111111111111111111111111
expect_failure "secret_scan_range_error code=MISSING_BASE_OBJECT field=before_sha event=push created=false ref=refs/heads/feature before=$missing head=$head_sha" push false refs/heads/feature "$missing" "$head_sha" refs/remotes/origin/develop
expect_failure "secret_scan_range_error code=MISSING_BASE_OBJECT field=before_sha event=pull_request created=empty ref=refs/pull/1/merge before=$missing head=$head_sha" pull_request '' refs/pull/1/merge "$missing" "$head_sha" refs/remotes/origin/develop
expect_failure "secret_scan_range_error code=MISSING_HEAD_OBJECT field=head_sha event=push created=false ref=refs/heads/feature head=$missing" push false refs/heads/feature "$slice_one" "$missing" refs/remotes/origin/develop

git -C "$test_dir" update-ref refs/remotes/origin/develop "$head_sha"
expect_failure "secret_scan_range_error code=SELF_BASELINE_REF field=event_ref event=push created=true ref=refs/heads/develop head=$head_sha" push true refs/heads/develop "$zero_sha" "$head_sha" refs/remotes/origin/develop
expect_failure "secret_scan_range_error code=SELF_BASELINE_COMMIT field=baseline_ref event=push created=true ref=refs/heads/feature baseline=$head_sha head=$head_sha" push true refs/heads/feature "$zero_sha" "$head_sha" refs/remotes/origin/develop

printf 'descendant\n' >"$test_dir/descendant.txt"
git -C "$test_dir" add descendant.txt
git -C "$test_dir" commit --quiet -m descendant-baseline
descendant=$(git -C "$test_dir" rev-parse HEAD)
git -C "$test_dir" update-ref refs/remotes/origin/develop "$descendant"
expect_failure "secret_scan_range_error code=EMPTY_CREATED_RANGE field=resolved_base event=push created=true ref=refs/heads/feature baseline=$descendant resolved_base=$head_sha head=$head_sha" push true refs/heads/feature "$zero_sha" "$head_sha" refs/remotes/origin/develop

git -C "$test_dir" update-ref -d refs/remotes/origin/develop
expect_failure "secret_scan_range_error code=MISSING_BASELINE_OBJECT field=baseline_ref event=push created=true ref=refs/heads/feature baseline=refs/remotes/origin/develop head=$head_sha" push true refs/heads/feature "$zero_sha" "$head_sha" refs/remotes/origin/develop

empty_tree=$(git -C "$test_dir" mktree </dev/null)
orphan=$(printf 'orphan\n' | git -C "$test_dir" commit-tree "$empty_tree")
git -C "$test_dir" update-ref refs/remotes/origin/develop "$orphan"
expect_failure "secret_scan_range_error code=NO_COMMON_ANCESTOR field=history event=push created=true ref=refs/heads/feature baseline=$orphan head=$head_sha" push true refs/heads/feature "$zero_sha" "$head_sha" refs/remotes/origin/develop
expect_failure "secret_scan_range_error code=NO_COMMON_ANCESTOR field=history event=push created=false ref=refs/heads/feature base=$orphan head=$head_sha" push false refs/heads/feature "$orphan" "$head_sha" refs/remotes/origin/develop
expect_failure "secret_scan_range_error code=NO_COMMON_ANCESTOR field=history event=pull_request created=empty ref=refs/pull/1/merge base=$orphan head=$head_sha" pull_request '' refs/pull/1/merge "$orphan" "$head_sha" refs/remotes/origin/develop

echo "Secret scan range tests passed"
