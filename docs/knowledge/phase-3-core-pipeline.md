# Phase 3 knowledge: Core pipeline

Written during Phase 3, not before it. Companion to
[phase-1-data-platform.md](phase-1-data-platform.md), which covers standing
Strimzi and MinIO up in the first place. This file covers what actually came up
while building the Shopper-keyed branch against `MiniCluster`.

Design decisions and their rejected alternatives live in
[the core pipeline design](../superpowers/specs/2026-08-23-core-pipeline-design.md).
The ordered steps live in
[the implementation plan](../superpowers/plans/2026-08-23-phase-3-core-pipeline.md).
This file is only for how things actually work.

## How a NodePort Service reaches the pod, and why it never visits the ClusterIP

Came up in Task 2, while exposing MinIO's S3 API so `MiniCluster` on the host
could write checkpoints to it.

The confusion is worth recording because the obvious mental model is wrong. A
`NodePort` Service looks like it should chain: host port, then ClusterIP, then
pod. It does not. There is no process listening on the ClusterIP at all. A
Service is a set of packet-rewriting rules that `kube-proxy` programs into the
node's iptables or nftables, so nothing ever "arrives" at the ClusterIP to be
forwarded onward.

`nodePort` and `port` are two separate keys in the same rewrite table, not two
hops in one path. Both keys name the same right-hand side, which is
`targetPort`.

MinIO's ClusterIP is 10.96.241.160 and targetPort is 9000. Say the pod's IP is
10.244.1.7 (check yours with `kubectl get pod -n minio-tenant -o wide`).

Your path, from the host:

```
localhost:30014
  └─▶ kind node container, port 30014
        └─▶ rule: "dst *:30014" → rewrite dst to 10.244.1.7:9000
              └─▶ pod, listening on 9000
```

A pod inside the cluster calling the Service:

```
curl http://minio.minio-tenant.svc:80
  └─▶ DNS resolves to 10.96.241.160
        └─▶ rule: "dst 10.96.241.160:80" → rewrite dst to 10.244.1.7:9000
              └─▶ pod, listening on 9000
```

Two entry points. Both rules end at targetPort. Neither one visits the other.

Corrected diagram

                    ┌─ nodePort 30014 ─┐
host / any node ────┤                  ├──▶ pod 10.244.1.7:9000   (targetPort)
                    │                  │
in-cluster client ──┴─ port 80 ────────┘
                       (ClusterIP 10.96.241.160)

### Three consequences worth keeping

- **`targetPort` is the only one of the three that must be correct.** It sits on
  the right-hand side of both rules. `nodePort` decides how the host gets in.
  `port` is only used by in-cluster callers addressing the ClusterIP, and
  nothing in this project does that, so its value is free.
- **The rules are generated from the `selector`, not from the Service name.** A
  wrong selector produces a Service that is created without error and routes
  nowhere. Listing the endpoints prints the real `podIP:targetPort` list, so an
  empty result identifies a selector problem before `curl` is ever run.

  On Kubernetes 1.33 and later, `kubectl get endpoints` warns that the `v1
  Endpoints` API is deprecated in favour of `discovery.k8s.io/v1
  EndpointSlice`. This cluster is on 1.34.8, so the current form is:

  ```bash
  kubectl get endpointslice -n minio-tenant -l kubernetes.io/service-name=minio-s3-api
  ```

  The old command still works and still answers the question. It just will not
  forever.
- **In `kind`, a NodePort alone is not enough.** A node is a container, so the
  port must also be published to the host by `extraPortMappings` at cluster
  creation time. Phase 0 reserved 30014 and 30015 for MinIO in
  `clusters/kind/kind-cluster.yaml` before anything needed them, which is why
  Phase 3 did not require recreating the cluster.

### Observed on this cluster, 2026-08-23

The `minio-s3-api` Service, `nodePort: 30014`, `targetPort: 9000`:

```
NAME           ENDPOINTS          AGE
minio-s3-api   10.244.3.14:9000   4m58s

$ curl -i http://localhost:30014/minio/health/live
HTTP/1.1 200 OK
```

The endpoint line proves the selector matched a real pod and that `targetPort`
is right. The `200 OK` proves the whole chain from outside the cluster: host
port 30014, the `kind` `extraPortMapping`, the `zone-a` worker, the nodePort
rewrite rule, the pod on 9000.

**Neither is implied by ArgoCD reporting Synced and Healthy.** For a Service
that means only that the YAML applied and Kubernetes accepted it. A Service
whose selector matches zero pods is Synced and Healthy too.

### Which namespace the Service goes in is not a choice

A Service's selector only matches pods in its own namespace. Selectors do not
cross namespace boundaries. Put this Service in `default` with the same
`v1.min.io/tenant: personalization` label and it matches zero pods, produces an
empty endpoint list, and routes nowhere.

The MinIO pods are in `minio-tenant` because `manifests/minio/tenant.yaml` sets
that namespace on the `Tenant`. So the Service has to be there too. This is
mechanical, not a preference.

