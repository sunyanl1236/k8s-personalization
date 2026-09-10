#!/usr/bin/env bash
#
# The gap check instrument for Phase 5's Drills A, B, and C.
#
# Usage:
#   ./scripts/recommendation-snapshot.sh snapshot <out-file> [since-epoch-ms]
#   ./scripts/recommendation-snapshot.sh compare  <before-file> <after-file>
#
# Why snapshot takes a time window. Without one, kcat -C -e reads the topic from
# the beginning, and this topic now spans Phases 3 to 6 plus at least one full
# source replay. Measured on 2026-09-10: 140598 records carrying only 134754
# identities, so 5844 duplicates were already present before any Phase 7 Drill
# ran. A duplicate count over that range is dominated by history and says nothing
# about the Drill. `since-epoch-ms` maps to kcat's `-o s@<ms>`, which starts at
# the first offset whose record timestamp is at or after that time.
#
# The trap inside that. The filter is on the record TIMESTAMP, and this topic's
# record timestamp IS generatedAt, an event-time value (see below). So a job
# replaying old Clicks emits records carrying OLD timestamps, and a window keyed
# on wall-clock start time will not contain them. That is correct for a
# promotion, where event time keeps advancing. It is wrong for a stateless
# restart with --start-from-earliest=true, which replays the whole clickstream
# topic: those Recommendations land at their original event times, behind the
# window. Take the baseline AFTER catch-up, not before it.
#
# Why the topic is compared against itself, and never against the input.
# The pipeline suppresses output twice on purpose: Phase 4 Task 1 drops
# out-of-stock candidates, measured at 9.6% of generator volume, and Phase 4
# Task 4 sends a candidate with no trigger to UNMATCHED without publishing it.
# So the topic legitimately holds fewer Recommendations than there are closed
# Browsing Sessions. Compare those two numbers and correct suppression reads as
# a Drill failure.
#
# What identifies one Recommendation. Recommendation has no id field, so the
# identity is (shopperId, generatedAt). generatedAt is the Browsing Session's
# window end, an event-time value, and Phase 4's global constraints keep
# wall-clock time out of the output. Replay the same input and the same pair is
# produced again. RecommendationSerializationSchema puts shopperId in the record
# key and generatedAt in the record timestamp, so kcat -f '%k %T\n' yields the
# pair with no JSON parsing.
#
# Why isolation.level=read_committed is not optional. The sink is transactional.
# Without the flag, the BEFORE snapshot picks up records that are later aborted,
# and every Drill then reports a gap that never happened. Measured on this topic
# on 2026-09-07: 16 records uncommitted against 15 committed.
#
# Why the snapshot is sorted but NOT deduplicated. Two different failures are
# reported, and they need different views of the same data. A gap needs set
# difference, which needs unique input. A duplicate is the same identity
# published twice, which only exists in the raw stream. Deduplicating on write
# would make the duplicate check permanently empty and silently useless.
#
# The window is an OFFSET, not a timestamp filter. kcat -o s@<ms> resolves the
# timestamp to one starting offset per partition and then reads everything from
# there. Because this topic's record timestamp is generatedAt, an event-time
# value, timestamps within a partition are not monotonic, so the result includes
# some records older than the requested time. Measured on 2026-09-10: asking for
# the topic's midpoint timestamp returned 127728 records where a strict
# timestamp filter over the same data would return 117191. That is harmless for a
# Drill, because BEFORE and AFTER use the same since value and therefore share
# the same starting offsets. Do not use this as an exact time filter.
#
# Why the window is recorded in a sidecar .meta file and not in the snapshot.
# The snapshot is sorted, and comm and uniq read every line as an identity. A
# header line would sort into the data and be compared as though it were a
# Recommendation. The sidecar keeps the data format untouched and lets compare
# refuse a mismatched pair: a full-topic BEFORE against a windowed AFTER reports
# a gap of tens of thousands of identities that never went missing.
#
# Why the two listings use a herestring and not `printf | head`. head exits after
# 20 lines and closes the pipe, printf takes SIGPIPE, and under `set -o pipefail`
# the pipeline's status becomes 141. The script then exited 141 while reporting a
# result it had computed correctly. A herestring is not a pipe, so nothing is
# signalled.
#
# LC_ALL=C on both sort and comm. comm compares byte for byte and rejects input
# sorted under a different collation with "file N is not in sorted order".
set -euo pipefail

BROKER="localhost:30016"
TOPIC="recommendation"

export LC_ALL=C

info() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m ok\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m  !\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m  x\033[0m %s\n' "$*" >&2; exit 1; }

command -v kcat >/dev/null || die "kcat not found"

cmd_snapshot() {
  local out="${1:-}" since="${2:-}"
  [[ -n "${out}" ]] || die "usage: $0 snapshot <out-file> [since-epoch-ms]"

  local dir
  dir="$(dirname "${out}")"
  [[ -d "${dir}" ]] || die "directory does not exist: ${dir}"

  local offset_args=()
  if [[ -n "${since}" ]]; then
    [[ "${since}" =~ ^[0-9]{13}$ ]] \
      || die "since-epoch-ms must be 13 digits (milliseconds), got: ${since}"
    offset_args=(-o "s@${since}")
    info "Reading ${TOPIC} from ${BROKER}, records at or after $(date -d "@$((since / 1000))" '+%F %T %Z')"
  else
    info "Reading ${TOPIC} from ${BROKER}, whole topic"
  fi

  kcat -b "${BROKER}" -t "${TOPIC}" -C -e \
    "${offset_args[@]}" \
    -X isolation.level=read_committed \
    -f '%k %T\n' \
    | sort > "${out}"

  local total unique
  total="$(wc -l < "${out}")"
  unique="$(uniq "${out}" | wc -l)"

  printf 'since=%s\nrecords=%s\nidentities=%s\ntaken=%s\n' \
    "${since:-0}" "${total}" "${unique}" "$(date -u '+%FT%TZ')" > "${out}.meta"

  if [[ "${total}" -eq 0 ]]; then
    warn "the snapshot is empty; the job may not be producing Recommendations"
    [[ -n "${since}" ]] && warn "the window may start after the last record, or behind a replay's event times"
  fi
  if [[ "${total}" -ne "${unique}" ]]; then
    warn "${total} records but only ${unique} identities: duplicates already present before any Drill"
  fi

  ok "${out}: ${total} records, ${unique} identities, window ${since:-whole topic}"
}

cmd_compare() {
  local before="${1:-}" after="${2:-}"
  [[ -n "${before}" && -n "${after}" ]] || die "usage: $0 compare <before-file> <after-file>"
  [[ -f "${before}" ]] || die "no such file: ${before}"
  [[ -f "${after}"  ]] || die "no such file: ${after}"

  # A windowed snapshot compared against a whole-topic one reports a gap of
  # every identity outside the window. Refuse rather than print a false failure.
  local before_since after_since
  before_since="$( [[ -f "${before}.meta" ]] && sed -n 's/^since=//p' "${before}.meta" || echo unknown )"
  after_since="$(  [[ -f "${after}.meta"  ]] && sed -n 's/^since=//p' "${after}.meta"  || echo unknown )"
  if [[ "${before_since}" != "${after_since}" ]]; then
    die "window mismatch: ${before} starts at ${before_since}, ${after} at ${after_since}. Take both with the same since-epoch-ms."
  fi

  local gap dupes gap_n dupes_n
  gap="$(comm -23 <(uniq "${before}") <(uniq "${after}") || true)"
  dupes="$(uniq -d "${after}" || true)"

  gap_n="$([[ -z "${gap}" ]] && echo 0 || printf '%s\n' "${gap}" | wc -l)"
  dupes_n="$([[ -z "${dupes}" ]] && echo 0 || printf '%s\n' "${dupes}" | wc -l)"

  info "before: $(uniq "${before}" | wc -l) identities"
  info "after:  $(uniq "${after}" | wc -l) identities"

  if [[ "${gap_n}" -gt 0 ]]; then
    warn "GAP: ${gap_n} identities in BEFORE are missing from AFTER"
    head -20 <<< "${gap}" >&2
    [[ "${gap_n}" -gt 20 ]] && warn "... ${gap_n} total"
  else
    ok "no gap"
  fi

  if [[ "${dupes_n}" -gt 0 ]]; then
    warn "DUPLICATES: ${dupes_n} identities appear more than once in AFTER"
    head -20 <<< "${dupes}" >&2
    [[ "${dupes_n}" -gt 20 ]] && warn "... ${dupes_n} total"
  else
    ok "no duplicates"
  fi

  if [[ "${gap_n}" -gt 0 || "${dupes_n}" -gt 0 ]]; then
    die "the Drill did not recover cleanly"
  fi

  ok "recovery is clean"
}

case "${1:-}" in
  snapshot) shift; cmd_snapshot "$@" ;;
  compare)  shift; cmd_compare  "$@" ;;
  *) die "usage: $0 snapshot <out-file> | compare <before-file> <after-file>" ;;
esac
