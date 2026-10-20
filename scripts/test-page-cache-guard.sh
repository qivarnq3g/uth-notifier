#!/bin/bash
set -euo pipefail

repository_root=$(cd "$(dirname "$0")/.." && pwd)
guard="$repository_root/deploy/page-cache-guard.sh"
work=$(mktemp -d "${TMPDIR:-/tmp}/page-cache-guard-test.XXXXXX")
trap 'rm -rf "$work"' EXIT

real_dd=$(command -v dd)
mkdir -p "$work/shim" "$work/disk" "$work/files/browser" "$work/state"
cat > "$work/shim/dd" <<EOF
#!/bin/bash
input=""
direct=0
nocache=0
for argument in "\$@"; do
  case "\$argument" in
    if=*) input="\${argument#if=}" ;;
    iflag=direct) direct=1 ;;
    iflag=nocache) nocache=1 ;;
  esac
done
truth="$work/disk/\$(basename "\$input")"
if [ "\$direct" = 1 ] && [ -f "\$truth" ]; then
  cat "\$truth"
  exit 0
fi
if [ "\$nocache" = 1 ]; then
  printf '%s\n' "\$input" >> "$work/evictions"
  if [ "\${SHIM_HEAL:-0}" = 1 ] && [ -f "\$truth" ]; then
    modified=\$(stat -c %Y "\$input")
    cat "\$truth" > "\$input"
    touch -d "@\$modified" "\$input"
  fi
  exit 0
fi
exec "$real_dd" "\$@"
EOF
chmod +x "$work/shim/dd"

failures=0
check() {
  if [ "$2" = "$3" ]; then
    printf 'ok   %s\n' "$1"
  else
    printf 'FAIL %s: expected [%s] got [%s]\n' "$1" "$2" "$3"
    failures=$((failures + 1))
  fi
}

run_guard() {
  set +e
  output=$(PATH="$work/shim:$PATH" PAGE_CACHE_GUARD_STATE_DIRECTORY="$work/state" bash "$guard" "$work/files/browser" "$work/files/node" 2>&1)
  status=$?
  set -e
  summary=$(printf '%s\n' "$output" | grep '"event":"summary"' || true)
}

field() {
  printf '%s' "$summary" | grep -o "\"$1\":[0-9]*" | cut -d: -f2
}

printf 'chrome-good' > "$work/files/browser/chrome"
printf 'node-good' > "$work/files/node"
printf 'chrome-good' > "$work/disk/chrome"
printf 'node-good' > "$work/disk/node"

run_guard
check "first run exits zero" 0 "$status"
check "first run checks every file" 2 "$(field checked)"
check "first run pins every file" 2 "$(field pinned)"
check "first run evicts nothing" 0 "$(field evicted)"

run_guard
check "second run reuses pinned digests" 0 "$(field pinned)"
check "second run still checks every file" 2 "$(field checked)"
check "second run evicts nothing" 0 "$(field evicted)"

mtime=$(stat -c %Y "$work/files/browser/chrome")
printf 'chrome-flip' > "$work/files/browser/chrome"
touch -d "@$mtime" "$work/files/browser/chrome"
SHIM_HEAL=1 run_guard
check "healed corruption exits zero" 0 "$status"
check "healed corruption is evicted" 1 "$(field evicted)"
check "healed corruption is not unrecovered" 0 "$(field unrecovered)"
check "eviction targets the corrupted file" "$work/files/browser/chrome" "$(cat "$work/evictions")"
check "corruption event is logged" 1 "$(printf '%s\n' "$output" | grep -c '"event":"page_cache_mismatch_evicted"' || true)"
check "file content is restored" chrome-good "$(cat "$work/files/browser/chrome")"

rm -f "$work/evictions"
printf 'chrome-flip' > "$work/files/browser/chrome"
touch -d "@$mtime" "$work/files/browser/chrome"
SHIM_HEAL=0 run_guard
check "unrecovered corruption exits nonzero" 1 "$status"
check "unrecovered corruption is counted" 1 "$(field unrecovered)"
check "unrecovered event is logged" 1 "$(printf '%s\n' "$output" | grep -c '"event":"page_cache_mismatch_unrecovered"' || true)"

printf 'chrome-good' > "$work/files/browser/chrome"
touch -d "@$mtime" "$work/files/browser/chrome"
rm -f "$work/evictions"
sleep 1
printf 'chrome-new-release' > "$work/files/browser/chrome"
printf 'chrome-new-release' > "$work/disk/chrome"
run_guard
check "replaced file exits zero" 0 "$status"
check "replaced file is pinned again" 1 "$(field pinned)"
check "replaced file is not evicted" 0 "$(field evicted)"
check "state keeps only current files" 2 "$(wc -l < "$work/state/digests" | tr -d ' ')"

set +e
PAGE_CACHE_GUARD_STATE_DIRECTORY="$work/state" bash "$guard" "$work/missing" > /dev/null 2>&1
missing_status=$?
set -e
check "missing target exits nonzero" 1 "$missing_status"

real_output=$(PAGE_CACHE_GUARD_STATE_DIRECTORY="$work/real-state" bash "$guard" "$work/files/browser" 2>&1) && real_status=0 || real_status=$?
check "real direct read path exits zero" 0 "$real_status"
check "real direct read path pins the file" 1 "$(printf '%s' "$real_output" | grep -o '"pinned":[0-9]*' | cut -d: -f2)"

if [ "$failures" -ne 0 ]; then
  printf '%s failure(s)\n' "$failures"
  exit 1
fi
printf 'all page-cache guard checks passed\n'
