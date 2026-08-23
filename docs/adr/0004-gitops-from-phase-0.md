# ADR 0004: ArgoCD is installed in Phase 0, and every later component is an Application

Date: 2026-08-10
Status: accepted

## Context

The design spec installs cert-manager, the Flink Kubernetes Operator, Karpenter,
and KEDA with Helm, then introduces ArgoCD at the end of its Phase 3 and brings
"all of the above under GitOps sync."

That ordering means every install is done twice. The first pass is imperative
`helm install`. The second pass rewrites each one as an ArgoCD `Application`
with the same values, then reconciles the drift between what Helm's release
state thinks it owns and what ArgoCD thinks it owns. That migration is
throwaway work, and adopting a Helm release into ArgoCD is fiddlier than
installing it that way to begin with.

## Decision

ArgoCD is installed in Phase 0, immediately after the cluster and cert-manager.
It is bootstrapped once with Helm and then manages itself plus everything else
via an app-of-apps root Application.

Every component from Phase 1 onward lands as an ArgoCD `Application` on first
install. There is no imperative install path in the repo.

## Consequences

**CRD ordering needs sync waves.** cert-manager, Strimzi, the Flink operator,
Karpenter, and KEDA each ship CRDs that later Applications' resources depend
on. Without `argocd.argoproj.io/sync-wave` annotations, ArgoCD attempts to apply
a `FlinkDeployment` before the CRD exists and the sync fails. Assign waves
explicitly: CRD-providing controllers in early waves, custom resources in later
ones.

**`selfHeal` is off.** The design spec's ArgoCD Lua actions
(`suspend`/`resume`/`restart`) exist to demonstrate drift correction: you patch
a live `FlinkDeployment`, and the next sync reverts it. With `automated.selfHeal:
true` that revert happens within seconds and the patched state is never
observable. Set `selfHeal: false` and trigger the revert with a manual sync so
the drill is legible.

**You debug ArgoCD before you understand what it is syncing.** This is the real
cost. A failed sync in Phase 1 could be a Strimzi problem or an ArgoCD problem
and you will not yet have the intuition to tell. Accepted, because the
alternative pays a larger cost later.

**The Lua sandbox constraint still holds.** ArgoCD's Lua has no `string` or
`math` library. The custom actions do whole-value patches such as flipping
`spec.job.state`. No string parsing. This is why the blue/green promotion is a
shell script and not a Lua action. See
[0006](0006-blue-green-native-mode.md).

## Alternatives rejected

- **ArgoCD last, as the spec has it.** Rejected for the double-install cost.
- **ArgoCD at Phase 5 with the operator.** Splits the difference but leaves two
  install idioms coexisting in the repo permanently.
