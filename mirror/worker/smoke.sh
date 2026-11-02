#!/usr/bin/env bash
# Smoke test for the bulk mirror Worker. Usage: smoke.sh [BASE_URL]
# Makes about ten requests, far below the per-IP limits (30 files / 120 other per minute).
set -u

BASE_URL="${1:-https://bulk.developmentfinance.org}"
BASE_URL="${BASE_URL%/}"
failures=0

check() {
  local name="$1" expected="$2" actual="$3"
  if [[ "$expected" == "$actual" ]]; then
    echo "ok    $name"
  else
    echo "FAIL  $name: expected '$expected', got '$actual'"
    failures=$((failures + 1))
  fi
}

status() { curl -s -o /dev/null -w '%{http_code}' "$@"; }

manifest="$(mktemp)"
trap 'rm -f "$manifest"' EXIT

check "GET manifest" 200 "$(curl -s -o "$manifest" -w '%{http_code}' "$BASE_URL/oecd/manifest.json")"

if jq -e '.schema == 1 and (.files | type == "object")' "$manifest" >/dev/null 2>&1; then
  echo "ok    manifest is valid JSON with schema 1"
  read -r object size < <(jq -r '.files | to_entries | first | "\(.value.object) \(.value.size)"' "$manifest")
  if [[ -n "${object:-}" && "$object" != "null" ]]; then
    head_out="$(curl -sI "$BASE_URL/$object")"
    check "HEAD first object" 200 "$(printf '%s' "$head_out" | head -n1 | awk '{print $2}')"
    length="$(printf '%s' "$head_out" | tr -d '\r' | awk 'tolower($1) == "content-length:" {print $2}')"
    check "content-length equals manifest size" "$size" "$length"
  else
    echo "FAIL  manifest lists no files"
    failures=$((failures + 1))
  fi
else
  echo "FAIL  manifest is not valid JSON with schema 1"
  failures=$((failures + 1))
fi

check "GET unknown object" 404 "$(status "$BASE_URL/oecd/does-not-exist")"
check "GET outside /oecd/" 404 "$(status "$BASE_URL/other/manifest.json")"
check "PUT" 405 "$(status -X PUT --data x "$BASE_URL/oecd/manifest.json")"

if ((failures > 0)); then
  echo "$failures check(s) failed"
  exit 1
fi
echo "all checks passed"
