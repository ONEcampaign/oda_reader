#!/bin/bash
# Refresh the R2 mirror of OECD bulk files, as launchd runs it each day.
# Reads the R2 credentials from the macOS Keychain, runs the refresher from the repository
# root, appends timestamped output to ~/Library/Logs/oda-mirror-refresh.log, and posts a
# notification when the refresher exits non-zero. Arguments go to the refresher
# (for example --dry-run or --dataset dac1). Exits with the refresher's exit code.
# Refuses to run unless the checkout is on main, since the refresher imports this
# checkout's oda_reader; set ODA_MIRROR_ALLOW_BRANCH=1 to run from another branch.
set -u -o pipefail

R2_ACCOUNT_ID="8b994c58b2ba94f032bbe3bc3fd97d52"
KEYCHAIN_SERVICE="oda-mirror-r2"
LOG="$HOME/Library/Logs/oda-mirror-refresh.log"

# launchd starts jobs with a minimal PATH that has neither Homebrew nor ~/.local/bin.
export PATH="$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

keychain_secret() {
  security find-generic-password -s "$KEYCHAIN_SERVICE" -a "$1" -w
}

refresh() {
  cd "$repo_root" || return 1
  branch="$(git symbolic-ref --quiet --short HEAD)"
  if [ "$branch" != "main" ] && [ "${ODA_MIRROR_ALLOW_BRANCH:-}" != "1" ]; then
    echo "The checkout at $repo_root is on '${branch:-a detached HEAD}', not main." \
      "Run from a clone of main, or set ODA_MIRROR_ALLOW_BRANCH=1."
    return 1
  fi
  R2_ACCESS_KEY_ID="$(keychain_secret access-key-id)" || {
    echo "Keychain item $KEYCHAIN_SERVICE / access-key-id could not be read."
    return 1
  }
  R2_SECRET_ACCESS_KEY="$(keychain_secret secret-access-key)" || {
    echo "Keychain item $KEYCHAIN_SERVICE / secret-access-key could not be read."
    return 1
  }
  export R2_ACCOUNT_ID R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY

  command -v uv >/dev/null || {
    echo "uv is not on PATH ($PATH)."
    return 1
  }
  uv run --group mirror python -m scripts.mirror.refresh_oecd_mirror "$@"
}

stamp_lines() {
  while IFS= read -r line; do
    printf '%s %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$line"
  done
}

mkdir -p "$(dirname "$LOG")"
printf '%s run-refresh.sh started\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" >> "$LOG"
refresh "$@" 2>&1 | stamp_lines >> "$LOG"
status="${PIPESTATUS[0]}"
printf '%s run-refresh.sh finished with exit code %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" "$status" >> "$LOG"

if [ "$status" -ne 0 ]; then
  osascript -e "display notification \"Exit code $status. See ~/Library/Logs/oda-mirror-refresh.log\" with title \"ODA mirror refresh failed\"" || true
fi
exit "$status"
