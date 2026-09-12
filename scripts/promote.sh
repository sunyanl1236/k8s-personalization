#!/usr/bin/env bash
#
# Moves the Active Side from one namespace to the other, in either direction.
#
# Usage:
#   ./scripts/promote.sh [--dry-run]
#
# Per CONTEXT.md a Promotion is directionless: `blue` and `green` are namespace
# names, not roles. This script therefore takes no --from and no --to. It reads
# both sides and works out which is which. A direction supplied by a human can
# contradict reality after a rollback, and the check that caught it would be
# accidental.
#
# ---------------------------------------------------------------------------
# Why this writes to Git and never patches a live object
# ---------------------------------------------------------------------------
# `kubectl patch flinkdeployment ... state: suspended` changes the cluster while
# Git still says `running`. ArgoCD then reports the Application OutOfSync for the
# whole time the other side is Active, and any manual Sync in that window
# restarts the side that was just suspended. Both sides then consume every
# partition, because Flink's KafkaSource does not use consumer groups (ADR 0006).
#
# So every state change here is: edit the file, commit, push, `argocd app sync`,
# poll. The cluster and the repository never disagree, not even for the minutes a
# promotion takes. The cost is two commits and two syncs per promotion.
#
# ---------------------------------------------------------------------------
# Why the files are edited with sed and not with yq
# ---------------------------------------------------------------------------
# `yq -i` rewrites the whole document and reflows it. The manifests under
# manifests/flink/ carry load-bearing comments explaining why each field is what
# it is, and losing them costs more than the convenience is worth. Only two
# fields ever change, both simple scalars on their own line, so anchored line
# edits are enough.
#
# `^    state: ` is anchored to four spaces on purpose. `state.backend.type` sits
# at the same indent under flinkConfiguration, and an unanchored match would hit
# it.
#
# ---------------------------------------------------------------------------
# Which status fields actually carry the suspend signal
# ---------------------------------------------------------------------------
# Measured on a real suspend, 2026-09-12:
#
#   status.lifecycleState                             = SUSPENDED   <- the signal
#   status.jobStatus.state                            = FINISHED
#   status.jobStatus.savepointInfo.lastSavepoint.location = (absent)
#   status.jobStatus.upgradeSavepointPath             = s3://.../savepoint-6fc866-...
#
# `stop-with-savepoint` leaves Flink's own job status at FINISHED, not SUSPENDED.
# SUSPENDED is the OPERATOR's lifecycle state, a different field. And the
# operator records the savepoint it took in upgradeSavepointPath;
# savepointInfo.lastSavepoint is for savepoints triggered on their own, not for
# an upgrade, and stays empty here.
#
# An earlier version of this script polled jobStatus.state == "SUSPENDED" and
# read savepointInfo.lastSavepoint.location. Both are wrong, so every promotion
# would have polled for the full timeout and then rolled itself back. A dry run
# cannot catch it, because a dry run never polls.
#
# The poll still checks TWO conditions. Writing an empty initialSavepointPath
# into the other side is not an error: it is a legal way to say "fresh start", so
# the Standby Side would come up having silently discarded every Shopper's state.
#
# Discovery is different and still reads jobStatus.state, because "is it RUNNING"
# is exactly what that field answers.
#
# ---------------------------------------------------------------------------
# What a never-run deployment reports
# ---------------------------------------------------------------------------
# Measured on 2026-09-10: a FlinkDeployment created with state: suspended that
# has never run has NO status at all. `.status` is `{}`, so
# `.status.jobStatus.state` yields an empty string, not "SUSPENDED". Every test
# below is therefore "is it RUNNING", never a comparison against SUSPENDED.
set -euo pipefail

info() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m ok\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m  !\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m  x\033[0m %s\n' "$*" >&2; exit 1; }

require() { command -v "$1" >/dev/null || die "$1 not found${2:+, $2}"; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

SIDES=(blue green)
POLL_INTERVAL=1          # seconds. 1 and not 5: it is part of the pause a promotion costs
SUSPEND_TIMEOUT=300      # seconds to reach SUSPENDED with a savepoint path
RUNNING_TIMEOUT=600      # seconds for the incoming side to reach RUNNING
DRY_RUN=false

manifest() { printf 'manifests/flink/%s/flinkdeployment.yaml' "$1"; }
crname()   { printf 'personalization-%s' "$1"; }
namespace(){ printf 'personalization-%s' "$1"; }
appname()  { printf 'flink-job-%s' "$1"; }

run() {
  if [[ "${DRY_RUN}" == true ]]; then
    printf '    would run: %s\n' "$*"
  else
    "$@"
  fi
}

# ---------------------------------------------------------------------------
# Reading the two sources of truth
# ---------------------------------------------------------------------------

git_state() {  # what we WANT, from the file. lowercase: running | suspended
  awk '/^    state: /{print $2; exit}' "$(manifest "$1")"
}

live_state() { # Flink's own job status. RUNNING | FINISHED | FAILED | ...
  kubectl get flinkdeployment "$(crname "$1")" -n "$(namespace "$1")" \
    -o jsonpath='{.status.jobStatus.state}' 2>/dev/null || true
}

lifecycle_state() { # the OPERATOR's state. STABLE | SUSPENDED | DEPLOYED | ...
  kubectl get flinkdeployment "$(crname "$1")" -n "$(namespace "$1")" \
    -o jsonpath='{.status.lifecycleState}' 2>/dev/null || true
}

savepoint_path() { # the savepoint the operator took for THIS upgrade
  kubectl get flinkdeployment "$(crname "$1")" -n "$(namespace "$1")" \
    -o jsonpath='{.status.jobStatus.upgradeSavepointPath}' 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

set_job_state() { # side, running|suspended
  local file; file="$(manifest "$1")"
  grep -q '^    state: ' "${file}" || die "no '    state:' line in ${file}"
  run sed -i "0,/^    state: .*/s//    state: $2/" "${file}"
}

set_savepoint_path() { # side, path. Removes any existing line first, so this is
  local file; file="$(manifest "$1")"   # idempotent and never leaves two.
  run sed -i '/^    initialSavepointPath: /d' "${file}"
  run sed -i "/^    upgradeMode: /a\\    initialSavepointPath: $2" "${file}"
}

clear_savepoint_path() {
  run sed -i '/^    initialSavepointPath: /d' "$(manifest "$1")"
}

commit_and_sync() { # side, message
  local side="$1" message="$2"
  run git add "$(manifest "${side}")"
  run git commit -m "${message}"
  run git push
  # ArgoCD polls Git every 3 minutes on its own. Asking directly turns that into
  # seconds, and the wait is part of the pause a promotion costs.
  run argocd app sync "$(appname "${side}")"
}

# ---------------------------------------------------------------------------
# Polling
# ---------------------------------------------------------------------------

wait_suspended_with_savepoint() { # side -> prints the path on success
  local side="$1" waited=0 state path
  while (( waited < SUSPEND_TIMEOUT )); do
    state="$(lifecycle_state "${side}")"
    path="$(savepoint_path "${side}")"
    if [[ "${state}" == "SUSPENDED" && -n "${path}" ]]; then
      printf '%s' "${path}"
      return 0
    fi
    sleep "${POLL_INTERVAL}"
    waited=$(( waited + POLL_INTERVAL ))
  done
  return 1
}

wait_running() {
  local side="$1" waited=0
  while (( waited < RUNNING_TIMEOUT )); do
    [[ "$(live_state "${side}")" == "RUNNING" ]] && return 0
    sleep "${POLL_INTERVAL}"
    waited=$(( waited + POLL_INTERVAL ))
  done
  return 1
}

# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

print_table() {
  printf '\n    %-8s %-12s %-12s %-12s\n' side 'git wants' 'job status' 'lifecycle'
  printf '    %-8s %-12s %-12s %-12s\n' -------- ------------ ------------ ------------
  local side
  for side in "${SIDES[@]}"; do
    printf '    %-8s %-12s %-12s %-12s\n' \
      "${side}" "$(git_state "${side}")" \
      "$(live_state "${side}" || true)" "$(lifecycle_state "${side}" || true)"
  done
  printf '\n'
}

# Sets MODE, FROM, TO.
discover() {
  local side active=() standby=()
  for side in "${SIDES[@]}"; do
    local want have
    want="$(git_state "${side}")"
    have="$(live_state "${side}")"
    if [[ "${want}" == "running" && "${have}" == "RUNNING" ]]; then
      active+=("${side}")
    elif [[ "${want}" == "suspended" && "${have}" != "RUNNING" ]]; then
      standby+=("${side}")
    fi
  done

  if (( ${#active[@]} == 0 && ${#standby[@]} == 2 )); then
    MODE=fresh; FROM=""
    # Not just SIDES[0]. A side suspended under upgradeMode: savepoint carries
    # status.jobStatus.upgradeSavepointPath, and resuming it RESTORES from that
    # savepoint. That is correct for a resume and wrong for a fresh deploy: the
    # side would come back holding state from before whatever made you want a
    # fresh start. Prefer a side that has never run.
    TO=""
    local side
    for side in "${SIDES[@]}"; do
      if [[ -z "$(savepoint_path "${side}")" ]]; then TO="${side}"; break; fi
    done
    if [[ -z "${TO}" ]]; then
      TO="${SIDES[0]}"
      warn "every side carries a recorded savepoint, so this is a RESUME, not a fresh start."
      warn "${TO} will restore from: $(savepoint_path "${TO}")"
      warn "for a genuinely clean start, set upgradeMode: stateless for that one transition."
    fi
    return 0
  fi
  if (( ${#active[@]} == 1 && ${#standby[@]} == 1 )); then
    MODE=promotion; FROM="${active[0]}"; TO="${standby[0]}"
    return 0
  fi

  print_table
  if (( ${#active[@]} == 2 )); then
    die "BOTH sides are Active. Every Click is being processed twice. Do not promote; suspend one side through Git first."
  fi
  die "neither a clean fresh deploy nor a clean promotion. Git and the cluster disagree on at least one side; reconcile them before promoting."
}

# ---------------------------------------------------------------------------
# The two paths
# ---------------------------------------------------------------------------

do_fresh_deploy() {
  local recorded; recorded="$(savepoint_path "${TO}")"
  if [[ -z "${recorded}" ]]; then
    info "FRESH DEPLOY: starting ${TO}, which has never run, with no savepoint"
  else
    info "RESUME: starting ${TO}, which will restore from ${recorded}"
  fi
  warn "--start-from-earliest defaults to true, so this reads the input topics from their earliest"
  warn "retained offset. On a topic that still holds history this is a full replay; take any"
  warn "baseline snapshot AFTER catch-up rather than at start."
  clear_savepoint_path "${TO}"
  set_job_state "${TO}" running
  commit_and_sync "${TO}" "Fresh deploy: start ${TO}"
  if [[ "${DRY_RUN}" == true ]]; then ok "dry run complete"; return 0; fi
  wait_running "${TO}" || die "${TO} did not reach RUNNING within ${RUNNING_TIMEOUT}s"
  ok "${TO} is RUNNING"
}

do_promotion() {
  info "PROMOTION: ${FROM} -> ${TO}"

  set_job_state "${FROM}" suspended
  commit_and_sync "${FROM}" "Suspend ${FROM} for promotion to ${TO}"

  if [[ "${DRY_RUN}" == true ]]; then
    printf '    would poll %s for lifecycleState=SUSPENDED with a non-empty upgradeSavepointPath\n' "${FROM}"
    printf '    would write that path into %s and start it\n' "${TO}"
    ok "dry run complete"
    return 0
  fi

  local path
  if ! path="$(wait_suspended_with_savepoint "${FROM}")"; then
    warn "${FROM} did not reach lifecycleState=SUSPENDED with an upgradeSavepointPath within ${SUSPEND_TIMEOUT}s"
    warn "rolling the suspend back. ${TO} was never touched, so nothing is lost."
    set_job_state "${FROM}" running
    commit_and_sync "${FROM}" "Abort promotion, resume ${FROM}"
    die "promotion aborted cleanly"
  fi
  ok "${FROM} SUSPENDED, savepoint at ${path}"

  set_savepoint_path "${TO}" "${path}"
  set_job_state "${TO}" running
  # The outgoing side's spec.image is deliberately NOT touched. Its manifest,
  # left alone, records what was running before, and that record is what makes a
  # rollback a `git revert` of this commit.
  commit_and_sync "${TO}" "Promote ${TO} from ${path}"

  if ! wait_running "${TO}"; then
    warn "${TO} did not reach RUNNING within ${RUNNING_TIMEOUT}s"
    warn "This is the one failure with no automatic safe path: ${FROM} is already"
    warn "suspended and ${TO} is not up. Nothing will be attempted automatically."
    warn ""
    warn "To mitigate now, resume ${FROM} from its own pre-promotion savepoint:"
    warn "  git revert --no-edit HEAD          # undo the promote commit"
    warn "  sed -i '0,/^    state: .*/s//    state: running/' $(manifest "${FROM}")"
    warn "  git commit -am 'Resume ${FROM} after failed promotion' && git push"
    warn "  argocd app sync $(appname "${FROM}")"
    warn ""
    warn "Then debug ${TO} separately. Its savepoint path is still in Git."
    die "promotion failed with ${FROM} suspended"
  fi
  ok "${TO} is RUNNING, promoted from ${path}"
}

# ---------------------------------------------------------------------------

main() {
  while (( $# )); do
    case "$1" in
      --dry-run) DRY_RUN=true; shift ;;
      *) die "usage: $0 [--dry-run]" ;;
    esac
  done

  require kubectl
  require git
  require argocd "install it from https://argo-cd.readthedocs.io/en/stable/cli_installation/ and log in to localhost:30010"

  # A kwok node crashes kindnet on the real nodes (Phase 6 Drill H), and a
  # promotion depends on pod networking across two namespaces.
  if kubectl get nodes -o wide 2>/dev/null | grep -qi kwok; then
    die "kwok nodes are present. Scale the Decoy Workload to zero and let Karpenter consolidate before promoting."
  fi

  # This script commits on your behalf. Running it over a dirty tree would sweep
  # unrelated edits into a promotion commit.
  if [[ -n "$(git status --porcelain -- manifests/flink/)" ]]; then
    die "manifests/flink/ has uncommitted changes; commit or stash them first"
  fi

  discover
  print_table
  case "${MODE}" in
    fresh)     do_fresh_deploy ;;
    promotion) do_promotion ;;
  esac
}

main "$@"
