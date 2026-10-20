#!/bin/bash
set -euo pipefail

state_directory=${PAGE_CACHE_GUARD_STATE_DIRECTORY:?PAGE_CACHE_GUARD_STATE_DIRECTORY is required}
if [ "$#" -eq 0 ]; then
  printf 'usage: page-cache-guard.sh PATH...\n' >&2
  exit 2
fi

cd /
state_file="$state_directory/digests"
next_state_file="$state_directory/digests.next"
mkdir -p "$state_directory"
touch "$state_file"
: > "$next_state_file"

checked=0
pinned=0
evicted=0
unrecovered=0
errors=0

json_string() {
  local value=$1
  value=${value//\\/\\\\}
  value=${value//\"/\\\"}
  printf '"%s"' "$value"
}

emit() {
  printf '{"schema_version":"page-cache-guard.v1","event":"%s","path":%s,"expected":"%s","observed":"%s"}\n' \
    "$1" "$(json_string "$2")" "$3" "$4"
}

buffered_digest() {
  sha256sum < "$1" | cut -d' ' -f1
}

direct_digest() {
  dd if="$1" iflag=direct bs=1M status=none | sha256sum | cut -d' ' -f1
}

evict() {
  dd if="$1" iflag=nocache count=0 status=none
}

pinned_digest() {
  awk -v key="$1" '$1 == key { print $2; exit }' "$state_file"
}

guard_file() {
  local path=$1 key expected observed after disk
  key=$(stat -L -c '%d:%i:%s:%Y' "$path")
  checked=$((checked + 1))
  expected=$(pinned_digest "$key")
  if [ -z "$expected" ]; then
    if ! expected=$(direct_digest "$path"); then
      emit direct_read_failed "$path" "" ""
      errors=$((errors + 1))
      return
    fi
    pinned=$((pinned + 1))
  fi
  printf '%s %s %s\n' "$key" "$expected" "$path" >> "$next_state_file"
  observed=$(buffered_digest "$path")
  if [ "$observed" = "$expected" ]; then
    return
  fi
  evict "$path"
  after=$(buffered_digest "$path")
  if [ "$after" = "$expected" ]; then
    emit page_cache_mismatch_evicted "$path" "$expected" "$observed"
    evicted=$((evicted + 1))
    return
  fi
  disk=$(direct_digest "$path" || printf 'unreadable')
  emit page_cache_mismatch_unrecovered "$path" "$expected" "$after/disk:$disk"
  unrecovered=$((unrecovered + 1))
}

for target in "$@"; do
  if [ -d "$target" ]; then
    while IFS= read -r -d '' path; do
      guard_file "$path"
    done < <(find "$target" -type f -print0 | sort -z)
  elif [ -f "$target" ]; then
    guard_file "$target"
  else
    emit target_missing "$target" "" ""
    errors=$((errors + 1))
  fi
done

mv "$next_state_file" "$state_file"
printf '{"schema_version":"page-cache-guard.v1","event":"summary","checked":%d,"pinned":%d,"evicted":%d,"unrecovered":%d,"errors":%d}\n' \
  "$checked" "$pinned" "$evicted" "$unrecovered" "$errors"
if [ "$unrecovered" -ne 0 ] || [ "$errors" -ne 0 ]; then
  exit 1
fi
