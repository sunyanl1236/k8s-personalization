#!/usr/bin/env bash
#
# The gap check instrument for Phase 5's Drills A, B, and C.
#
# Usage:
#   ./scripts/recommendation-snapshot.sh snapshot <out-file>
#   ./scripts/recommendation-snapshot.sh compare  <before-file> <after-file>
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
  local out="${1:-}"
  [[ -n "${out}" ]] || die "usage: $0 snapshot <out-file>"

  local dir
  dir="$(dirname "${out}")"
  [[ -d "${dir}" ]] || die "directory does not exist: ${dir}"

  info "Reading ${TOPIC} from ${BROKER}"
  kcat -b "${BROKER}" -t "${TOPIC}" -C -e \
    -X isolation.level=read_committed \
    -f '%k %T\n' \
    | sort > "${out}"

  local total unique
  total="$(wc -l < "${out}")"
  unique="$(uniq "${out}" | wc -l)"

  if [[ "${total}" -eq 0 ]]; then
    warn "the snapshot is empty; the job may not be producing Recommendations"
  fi
  if [[ "${total}" -ne "${unique}" ]]; then
    warn "${total} records but only ${unique} identities: duplicates already present before any Drill"
  fi

  ok "${out}: ${total} records, ${unique} identities"
}

cmd_compare() {
  local before="${1:-}" after="${2:-}"
  [[ -n "${before}" && -n "${after}" ]] || die "usage: $0 compare <before-file> <after-file>"
  [[ -f "${before}" ]] || die "no such file: ${before}"
  [[ -f "${after}"  ]] || die "no such file: ${after}"

  local gap dupes gap_n dupes_n
  gap="$(comm -23 <(uniq "${before}") <(uniq "${after}") || true)"
  dupes="$(uniq -d "${after}" || true)"

  gap_n="$([[ -z "${gap}" ]] && echo 0 || printf '%s\n' "${gap}" | wc -l)"
  dupes_n="$([[ -z "${dupes}" ]] && echo 0 || printf '%s\n' "${dupes}" | wc -l)"

  info "before: $(uniq "${before}" | wc -l) identities"
  info "after:  $(uniq "${after}" | wc -l) identities"

  if [[ "${gap_n}" -gt 0 ]]; then
    warn "GAP: ${gap_n} identities in BEFORE are missing from AFTER"
    printf '%s\n' "${gap}" | head -20 >&2
    [[ "${gap_n}" -gt 20 ]] && warn "... ${gap_n} total"
  else
    ok "no gap"
  fi

  if [[ "${dupes_n}" -gt 0 ]]; then
    warn "DUPLICATES: ${dupes_n} identities appear more than once in AFTER"
    printf '%s\n' "${dupes}" | head -20 >&2
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
