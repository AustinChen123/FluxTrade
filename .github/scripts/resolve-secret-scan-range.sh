#!/usr/bin/env bash
set -euo pipefail

event_name=${1-}
event_created=${2-}
event_ref=${3-}
before_sha=${4-}
head_sha=${5-}
baseline_ref=${6-}
zero_sha=0000000000000000000000000000000000000000

is_sha() {
  [[ $1 =~ ^[0-9a-fA-F]{40}$ ]]
}

reject() {
  code=$1
  field=$2
  shift 2
  printf 'secret_scan_range_error code=%s field=%s' "$code" "$field" >&2
  (($# == 0)) || printf ' %s' "$@" >&2
  printf '\n' >&2
  exit 1
}

case "$event_name" in
  push)
    [[ $event_created == true || $event_created == false ]] || reject INVALID_EVENT_CREATED event_created "event=push"
    ;;
  pull_request)
    [[ -z $event_created ]] || reject INVALID_EVENT_CREATED event_created "event=pull_request"
    ;;
  *) reject INVALID_EVENT_NAME event_name ;;
esac

created_context=${event_created:-empty}
git check-ref-format "$event_ref" >/dev/null 2>&1 || reject INVALID_EVENT_REF event_ref "event=$event_name" "created=$created_context"
safe_event=("event=$event_name" "created=$created_context" "ref=$event_ref")
if [[ $event_name == push ]]; then
  [[ $event_ref == refs/heads/* ]] || reject INVALID_EVENT_REF_NAMESPACE event_ref "${safe_event[@]}"
else
  [[ $event_ref =~ ^refs/pull/[1-9][0-9]*/merge$ ]] || reject INVALID_EVENT_REF_NAMESPACE event_ref "${safe_event[@]}"
fi

is_sha "$head_sha" || reject INVALID_COMMIT_SHA head_sha "${safe_event[@]}"
is_sha "$before_sha" || reject INVALID_COMMIT_SHA before_sha "${safe_event[@]}" "head=$head_sha"
git cat-file -e "${head_sha}^{commit}" 2>/dev/null || reject MISSING_HEAD_OBJECT head_sha "${safe_event[@]}" "head=$head_sha"

if [[ $event_name == push && $event_created == true ]]; then
  [[ $before_sha == "$zero_sha" ]] || reject INVALID_EVENT_IDENTITY before_sha "${safe_event[@]}" "before=$before_sha" "head=$head_sha"
  [[ $event_ref != refs/heads/develop ]] || reject SELF_BASELINE_REF event_ref "${safe_event[@]}" "head=$head_sha"
  [[ $baseline_ref == refs/remotes/origin/develop ]] || reject INVALID_BASELINE_REF baseline_ref "${safe_event[@]}" "head=$head_sha"
  baseline_sha=$(git rev-parse --verify "${baseline_ref}^{commit}" 2>/dev/null) || reject MISSING_BASELINE_OBJECT baseline_ref "${safe_event[@]}" "baseline=refs/remotes/origin/develop" "head=$head_sha"
  [[ $baseline_sha != "$head_sha" ]] || reject SELF_BASELINE_COMMIT baseline_ref "${safe_event[@]}" "baseline=$baseline_sha" "head=$head_sha"
  resolved_base=$(git merge-base "$baseline_sha" "$head_sha" 2>/dev/null) || reject NO_COMMON_ANCESTOR history "${safe_event[@]}" "baseline=$baseline_sha" "head=$head_sha"
  [[ $resolved_base != "$head_sha" ]] || reject EMPTY_CREATED_RANGE resolved_base "${safe_event[@]}" "baseline=$baseline_sha" "resolved_base=$resolved_base" "head=$head_sha"
else
  [[ $before_sha != "$zero_sha" ]] || reject INVALID_EVENT_IDENTITY before_sha "${safe_event[@]}" "before=$before_sha" "head=$head_sha"
  git cat-file -e "${before_sha}^{commit}" 2>/dev/null || reject MISSING_BASE_OBJECT before_sha "${safe_event[@]}" "before=$before_sha" "head=$head_sha"
  git merge-base "$before_sha" "$head_sha" >/dev/null 2>&1 || reject NO_COMMON_ANCESTOR history "${safe_event[@]}" "base=$before_sha" "head=$head_sha"
  resolved_base=$before_sha
fi

scan_range=$resolved_base..$head_sha
commit_count=$(git rev-list --count "$scan_range" 2>/dev/null) || reject RANGE_COUNT_FAILED range "${safe_event[@]}" "resolved_base=$resolved_base" "head=$head_sha"
printf 'secret_scan_range event=%s created=%s ref=%s raw_before=%s resolved_base=%s head=%s range=%s commit_count=%s\n' \
  "$event_name" "$created_context" "$event_ref" "$before_sha" "$resolved_base" "$head_sha" "$scan_range" "$commit_count" >&2
printf '%s\n' "$scan_range"
