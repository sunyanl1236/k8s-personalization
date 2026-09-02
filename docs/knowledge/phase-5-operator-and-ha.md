# Phase 5 knowledge: Operator and HA

Written during Phase 5, not before it. Companion to
[phase-0-cluster-floor.md](phase-0-cluster-floor.md), which covers ArgoCD's
install, and to [phase-1-data-platform.md](phase-1-data-platform.md), which
covers Kafka and MinIO.

Design decisions and their rejected alternatives live in
[the Operator and HA design](../superpowers/specs/2026-08-30-operator-and-ha-design.md).
The ordered steps live in
[the implementation plan](../superpowers/plans/2026-08-30-phase-5-operator-and-ha.md).
This file is only for how things actually work.

It grows as tasks land. Sections appear in the order the confusion did, not in
the order the plan runs.

## Why a pod could not reach Kafka, and why a second listener was the fix

Task 0. The `Kafka` resource had one listener, `external`, type `nodeport`, port
9094, from [ADR 0002](../adr/0002-strimzi-external-listener.md). That was correct
for Phases 2 to 4, where every client ran on the host. Phase 5 is the first time
a client runs **inside** the cluster.

### The symptom

```bash
kubectl get svc personalization-kafka-bootstrap -n kafka \
  -o jsonpath='{range .spec.ports[*]}{.name}{" "}{.port}{"\n"}{end}'
```

Before: one line, `tcp-replication 9091`. That is Strimzi's broker-to-broker
replication listener. It is not for clients. So the in-cluster Service carried no
client port at all.

### Why the external listener could not simply be reused from inside

Not because a pod cannot reach a NodePort. It can. The reason is the **two-step
bootstrap**, which is how every Kafka client connects:

1. The client asks the bootstrap address: "who has which partitions?"
2. The brokers reply with a list of addresses.
3. The client **disconnects** and reconnects to those addresses.

Step 3 is the trap. The addresses in step 2 are whatever the listener is
configured to advertise, not the address the client originally dialled.

The `external` listener advertises `localhost:30017`, `30018`, `30019`, set
deliberately in Phase 1 so a host-side client could reach each broker through its
own NodePort. Inside a pod, `localhost` is that pod. A Flink TaskManager
following that redirect would connect to itself.

### The proof, from the probe

After adding `- name: plain / port: 9092 / type: internal / tls: false`:

```
$ kubectl run kcat-probe --rm -it --restart=Never -n kafka \
    --image=edenhill/kcat:1.7.1 -- \
    -b personalization-kafka-bootstrap.kafka.svc.cluster.local:9092 -L

3 brokers:
  broker 4 at personalization-brokers-4.personalization-kafka-brokers.kafka.svc:9092 (controller)
  broker 5 at personalization-brokers-5.personalization-kafka-brokers.kafka.svc:9092
  broker 3 at personalization-brokers-3.personalization-kafka-brokers.kafka.svc:9092
```

The advertised addresses are now **pod DNS names**, resolvable from any pod and
from nowhere on the host. That contrast, `localhost:3001x` versus
`personalization-brokers-N...svc:9092`, is the whole reason both listeners exist.

The two never interfere, because a listener's advertised addresses are a property
of the listener and not of the cluster.

### The Service port name is not your listener name

Worth knowing, because it looks like a rename and is not. The listener is named
`plain`, but the bootstrap Service port is `tcp-clients`:

```
$ kubectl get kafka personalization -n kafka \
    -o jsonpath='{range .spec.kafka.listeners[*]}{.name}{" "}{.port}{" "}{.type}{"\n"}{end}'
external 9094 nodeport
plain 9092 internal

$ kubectl get svc personalization-kafka-bootstrap -n kafka \
    -o jsonpath='{range .spec.ports[*]}{.name}{" "}{.port}{"\n"}{end}'
tcp-replication 9091
tcp-clients 9092
```

Strimzi uses a fixed port name per listener role, `tcp-clients` for a plaintext
internal listener, rather than deriving it from `spec.kafka.listeners[].name`.
Your `name` identifies the listener in the `Kafka` resource and in its status.
It is not what you match on when selecting a Service port.

---

## ArgoCD has two sync triggers, and they are easy to conflate

Came up in Task 0, while adding the Kafka internal listener. The question was
"where did I set `selfHeal: false`", and the real answer turned out to be that
`selfHeal` does not do what its position in the file suggests.

### The problem, before the mechanism

ArgoCD holds two pictures of every resource it manages:

```
declared state  =  what the Git repository says the resource should be
live state      =  what the cluster actually has right now
```

When the two differ, the Application is **OutOfSync**. That word describes a
condition. It says nothing about what ArgoCD will do next.

The two pictures can fall out of step for two completely different reasons, and
ArgoCD has a separate setting for each. Reading `syncPolicy` as one block hides
that.

| The setting | The trigger it answers | What it does |
|---|---|---|
| `automated:` present | **Git changed.** A new commit moved the tracked revision | Applies the new declared state to the cluster |
| `selfHeal:` | **The cluster changed.** Something wrote to a live resource directly | `true` reverts it. `false` reports it and waits |

So `selfHeal: false` means: *if I patch a live resource by hand, do not fight
me.* It is not "manual sync only". Git changes still sync automatically,
because `automated:` is present and is a different trigger.

### The nesting is what causes the confusion

`selfHeal` is a child of `automated`, so the block reads as though setting it to
`false` switches automation off:

```yaml
  syncPolicy:
    automated:          <-- this line turns auto-sync ON
      selfHeal: false   <-- this line only narrows what auto-sync reacts to
      prune: true
```

Three possible states. This project uses the middle one everywhere:

| YAML | On a Git change | On live cluster drift |
|---|---|---|
| no `automated:` at all | nothing. Sync by hand | nothing. Reports `OutOfSync` |
| **`automated:` with `selfHeal: false`** | **auto-applies** | **nothing. Reports `OutOfSync`** |
| `automated:` with `selfHeal: true` | auto-applies | auto-reverts |

`automated:` is not a default. Omit it and ArgoCD watches, reports, and never
acts.

`prune: true` is a third, independent option: when a resource disappears from
Git, delete it from the cluster rather than leaving an orphan behind.

### A worked example

Git declares `replicas: 3`. You run:

```bash
kubectl scale deployment/something --replicas=5
```

Live is now 5. Git still says 3. Two possible timelines:

**With `selfHeal: true`**

```
t = 0s     you scale to 5
t = ~5s    ArgoCD sees the difference and scales back to 3
t = 10s    you run kubectl get, and it reports 3
```

Your change is gone before you can look at it.

**With `selfHeal: false`, which is this project's setting**

```
t = 0s     you scale to 5
t = 1m     still 5. Application reads OutOfSync
t = 1h     still 5. Still OutOfSync
t = when you choose:   argocd app sync   ->  back to 3
```

ArgoCD notices, says so, and waits. The correction happens when you ask for it.

### Why this project chose `false`, in Phase 0, for a Phase 5 reason

Drill D patches `spec.job.state` to `suspended` on a live `FlinkDeployment`,
observes the patched state, then syncs and watches it revert.

With `selfHeal: true` the suspended state exists for a few seconds. You would
very likely run `kubectl get` after ArgoCD had already restored it, read
`running`, and conclude the patch had failed. **The Drill would look like it
passed while proving nothing.**

[ADR 0004](../adr/0004-gitops-from-phase-0.md) recorded this in Phase 0, long
before there was a `FlinkDeployment` to patch.

### Where it is set

All five Applications carry it. Every one names the Phase 5 Drill as the reason:

| File | Line |
|---|---|
| `manifests/argocd-apps/root.yaml` | 32 |
| `manifests/argocd-apps/strimzi.yaml` | 39 |
| `manifests/argocd-apps/strimzi-kafka-cluster.yaml` | 27 |
| `manifests/argocd-apps/minio-operator.yaml` | 28 |
| `manifests/argocd-apps/minio-tenant.yaml` | 23 |

### What "by hand" covers

Any write to a live resource that did not come from Git. `kubectl patch`,
`kubectl edit`, `kubectl scale`, an ArgoCD custom Lua action, or another
controller writing to the object. All of those produce drift. None of them
produce a Git change.

---

## ArgoCD reads GitHub, not your working tree

Came up in the same task, immediately after the section above, and it is the
practical half of the same idea.

`manifests/argocd-apps/strimzi-kafka-cluster.yaml` lines 16 to 18:

```yaml
    repoURL: https://github.com/sunyanl1236/k8s-personalization.git
    targetRevision: HEAD
    path: manifests/strimzi
```

Three consequences that are obvious in hindsight and surprising in the moment:

1. **An edited but uncommitted file is invisible.** `git status` showing
   ` M manifests/strimzi/kafka-cluster.yaml` means that change exists only on
   your disk. ArgoCD has no way to see it, and no amount of syncing will apply
   it.
2. **`targetRevision: HEAD` is the remote's default branch, not your current
   branch.** `git remote show origin` reports `HEAD branch: master`. Working on
   a feature branch and pushing there changes nothing in the cluster.
3. **A change must be committed and pushed before a sync means anything.** Once
   it is pushed, `automated:` picks it up on its own within roughly three
   minutes. `argocd app sync` only skips the wait.

This is the GitOps property working as designed, not an obstacle to it. The
cluster follows the repository. A file you have not published is not part of the
declared state.

### The check that saves the confusion

Before wondering why a sync did nothing:

```bash
git status --porcelain manifests/
git rev-list --left-right --count origin/master...HEAD
```

The first must be empty for the paths ArgoCD watches. The second must report
`0 0`, meaning your commit and the tracked branch are the same.

---

## Nothing runs except containers

Came up while writing Task 3, from the question "where are a Deployment, a
Namespace, and a ConfigMap running". The answer is nowhere, and the reason is
worth having straight before Task 4 installs an operator.

### The one idea

**Every Kubernetes object is a record in a database.** The database is `etcd`.
The only thing allowed to read or write it is the API server.

`Deployment`, `Namespace`, `ConfigMap`, `Service`, `Pod`: all records.
`kubectl get` is a query. `kubectl apply` is a write.

Only **containers** run, and they run on a node, started by that node's kubelet.
So asking where a Deployment runs has the same shape as asking where a row in a
table runs. It is stored, not executed.

The evidence is in the objects themselves:

```
Deployment  <name>   .spec.template.spec.nodeName   ''          empty
ConfigMap   <name>   .spec                          (no spec at all)
Namespace   <name>   .spec    {"finalizers":["kubernetes"]}
Pod         <name>   .spec.nodeName                 personalization-lab-worker3
```

Only the Pod has a node.

### What is genuinely running

Two groups of real processes.

On control-plane nodes, one of each per node, which is the HA built in Phase 0:

| Process | Job |
|---|---|
| `etcd` | the database |
| `kube-apiserver` | the only door to it. Validates and persists |
| `kube-controller-manager` | many controllers in one binary, each watching a record type |
| `kube-scheduler` | assigns pods to nodes |

On every node:

| Process | Job |
|---|---|
| `kubelet` | watches for pods assigned to its node, and makes them exist |
| container runtime (`containerd`) | actually starts the container processes |

### A worked example

Apply a Deployment named `web` with `replicas: 3`:

```
1. kubectl POSTs it to kube-apiserver.
   kube-apiserver validates it and writes ONE record to etcd.
   Nothing is running. No container exists.

2. The Deployment controller (inside kube-controller-manager) is watching
   Deployment records. It sees `web`. It writes ONE ReplicaSet record.

3. The ReplicaSet controller sees a ReplicaSet wanting 3 and finding 0.
   It writes THREE Pod records. Each has spec.nodeName = "" .
   Still nothing running.

4. kube-scheduler watches for Pods with an empty nodeName. It picks a node
   for each and writes nodeName back. Now 3 Pod records name 3 nodes.
   Still nothing running.

5. The kubelet on each of those nodes sees a Pod assigned to itself.
   It pulls the image and tells containerd to start the container.

   NOW something is running.
```

Five records were written. One process started per pod. Steps 1 to 4 were
entirely database writes, each by a controller reacting to the previous write.

That pattern is the whole design: **controllers watch records and write more
records, until a kubelet turns the last one into a process.**

### The three objects, specifically

**Deployment.** A record describing desired state. Never runs.
`kube-controller-manager` is the process that acts on it.

**Namespace.** A record that makes a name valid, plus a field
(`metadata.namespace`) on other records. It is a scope for names and a boundary
for permissions and quotas. Nothing runs, and it has essentially no spec: just a
finalizer so deletion can clean up what is inside it.

**ConfigMap.** Key-value data in etcd. It does nothing by itself. It becomes real
only when a pod references it, and then the **kubelet** projects it into the
container, in one of two ways with different behaviour:

| How it is used | What happens |
|---|---|
| mounted as a volume | kubelet writes the keys as files. Later edits to the ConfigMap propagate into the running container, after a delay |
| `envFrom` or `valueFrom` | read **once**, at container start. Later edits do nothing until the pod restarts |

That difference catches people. A ConfigMap change appearing to have no effect is
usually the second row.

### What a pod holds, and what it does not

A pod contains **containers**, never Deployments. The ownership arrow points the
other way:

```
Deployment  ->  ReplicaSet  ->  Pod  ->  Container
```

Not every pod has a Deployment above it. A StatefulSet or a third-party
controller can own one instead.

Inside a pod there are three things: one or more containers running for the
pod's whole life, zero or more init containers that run to completion first in
order, and a shared network namespace plus shared volumes. That last item is why
pods exist at all. Without them there would be no way to say "these processes
must be on the same node, share an IP, and live and die together".

Most pods hold one container. Observed in this cluster: `kafka` with an init
container `kafka-init`, `topic-operator` beside `user-operator` in one pod, and
`minio` beside `sidecar` with an init container `validate-arguments`.

### Why this is the section before Task 4

An **operator is just another controller**, written by someone other than the
Kubernetes project, running as an ordinary pod.

It watches a record type Kubernetes does not understand, and creates ordinary
Deployments, Services, and ConfigMaps from it. It sits at step 1 of the loop
above, and everything after it is the standard machinery.

So Task 4 installs the **running** part, and Task 5 writes the **record**.
Neither does anything alone. And it is why `spec.flinkConfiguration` reaches the
job as an ordinary file: the operator writes a ConfigMap, and the kubelet mounts
it at `/opt/flink/conf/config.yaml` in every JobManager and TaskManager pod.

---

## What Task 4 actually buys you

Without the operator, running Flink on Kubernetes means you write and maintain the JobManager Deployment, the TaskManager Deployment, the config ConfigMap, the HA ConfigMaps, and the Service. You also restart things by hand after a JobManager dies.

The operator replaces all of that with one custom resource. You write a FlinkDeployment. The operator reconciles it into the pods, and it keeps reconciling after a failure. Task 4 installs that operator and its CRDs. It writes no job.

---

## ArgoCD installs Helm charts without ever running `helm install`

Task 4. The question that started it: "shouldn't I `helm repo add` and
`helm install` the Flink Kubernetes Operator first, before writing the ArgoCD
Application?"

No. The Application **is** the install. Answering why took four layers, and each
one is a thing worth knowing on its own.

### The problem, before the mechanism

A Helm chart is a folder of YAML templates plus a `values.yaml` of knobs.
Turning it into concrete YAML is a pure function: chart plus values gives
manifests. That step is not the same as putting the manifests in a cluster.

Helm bundles the two steps together in one command, which is why they look like
one thing. They are not.

| Command | Fills in the templates | Writes to the cluster | Records a "these are mine" list |
|---|---|---|---|
| `helm template` | yes | **no** | no |
| `helm install` | yes | yes | **yes** |
| `helm uninstall` | no | deletes | reads that list |

ArgoCD uses row 1 and does rows 2 and 3 its own way.

```
chart + your values  ──render──>  25 concrete YAML objects
                                        │
              ┌─────────────────────────┴─────────────────────────┐
        helm install                                        argocd sync
        apply the 25 objects                                apply the 25 objects
        + save a copy in a Secret                           + stamp each object with
          sh.helm.release.v1.<name>.v1                        an annotation
```

25 is the real number for `flink-kubernetes-operator` 1.15.0 with this project's
values: 1 Deployment, 4 CRDs, 3 ServiceAccounts, 5 Roles, 5 RoleBindings, 1
Certificate, 1 Issuer, 2 webhook configurations, and 3 more.

### What a Helm release Secret actually is

`helm install cert-manager` created 46 objects. Later you type
`helm uninstall cert-manager`. Which 46?

Kubernetes cannot answer that. It has Deployments, Secrets, and CRDs. It has no
concept of "a release", and nothing in the API means "these belong together
because one command made them."

So Helm writes the list down itself. A file on your laptop would mean a second
machine could never uninstall, so Helm writes it into the cluster instead, as a
Secret of type `helm.sh/release.v1`:

```
$ kubectl get secret -n cert-manager | grep helm
sh.helm.release.v1.cert-manager.v1   helm.sh/release.v1   1   20d
```

Decoded, that Secret holds:

```
top-level keys: name, info, chart, config, manifest, hooks, version, namespace, apply_method
name: cert-manager | version: 1 | chart: cert-manager v1.21.1
manifest field: 1,037,637 characters, containing 46 objects
```

The `manifest` field is the exact YAML `helm install` applied, saved verbatim:

```yaml
---
# Source: cert-manager/templates/cainjector-serviceaccount.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: cert-manager-cainjector
  namespace: cert-manager
```

`helm uninstall` reads that field, splits on `---`, and deletes each object it
names.

To read it yourself, two `base64 -d` calls are needed. Helm stores the gzip as a
base64 string, and Kubernetes base64-encodes every Secret value again:

```bash
kubectl get secret sh.helm.release.v1.cert-manager.v1 -n cert-manager \
  -o jsonpath='{.data.release}' | base64 -d | base64 -d | gzip -d | head -c 400
```

The gzip is not decoration. A Secret's practical limit is about 1 MiB and the
manifest alone is 1.03 MB. Uncompressed it would not fit.

**Deleting that Secret does not stop cert-manager.** It is Helm's private
notebook. The scheduler never reads it, and no Deployment points at it. The pods
keep running. What is gone is Helm's memory: `helm list` shows nothing and
`helm uninstall` has nothing to work from.

### What `repo-server` is, and why it is a separate pod

ArgoCD must download third-party content and run a template engine over it. That
work needs network access and CPU, and it executes code written by strangers. It
should not happen inside the component holding write access to the cluster.

So ArgoCD is several pods with separate jobs:

```
argocd-server                     the web UI and API
argocd-repo-server                downloads repos and charts, renders YAML. NO cluster write access
argocd-application-controller     compares declared to live and applies. HAS cluster write access
argocd-redis                      cache of rendered manifests
argocd-dex-server                 SSO, unused here
argocd-applicationset-controller  generates Applications from templates, unused here
argocd-notifications-controller   sends alerts, unused here
```

`repo-server` is the renderer, and it renders by running the real `helm` binary:

```
$ kubectl exec -n argocd pod/argocd-repo-server-... -- sh -c 'which helm; helm version --short'
/usr/local/bin/helm
v4.2.1+gd591a19
```

The full sequence on a sync:

```
1. application-controller  ->  repo-server:  "manifests for Application flink-operator?"
2. repo-server downloads    flink-kubernetes-operator-1.15.0-helm.tgz  from repoURL
3. repo-server writes the Application's valuesObject into a temporary values.yaml
4. repo-server runs:
     helm template flink-kubernetes-operator <chart> \
       --namespace flink-operator -f values.yaml --include-crds
5. repo-server returns the printed YAML  ->  controller
6. controller applies each object, and adds
     argocd.argoproj.io/tracking-id  to every one
```

Step 4 is `helm template`, never `helm install`. That is the whole answer.

### Two memories, and why only one may exist

ArgoCD's equivalent of the release Secret is an annotation on each object:

```
$ kubectl get deployment strimzi-cluster-operator -n kafka \
    -o jsonpath='{.metadata.annotations}' | tr ',' '\n' | grep argo
"argocd.argoproj.io/tracking-id":"strimzi:apps/Deployment:kafka/strimzi-cluster-operator"
```

Read left to right: Application `strimzi`, kind `apps/Deployment`, object
`kafka/strimzi-cluster-operator`. This ArgoCD is v3.5.0, whose default tracking
method is the annotation rather than the older `app.kubernetes.io/instance`
label.

And no release Secret exists there, because no `helm install` ever ran:

```
$ kubectl get secret -n kafka | grep -c helm
0
```

Running `helm install` first and then applying the Application would create both
memories for one set of objects. Nothing errors at the time. The damage comes
later, and it is hard to read backwards:

- Remove the file from Git. ArgoCD prunes the objects. Helm's release Secret
  still lists them, so `helm list` reports a release whose objects are gone.
- Or run `helm uninstall`. Objects vanish that ArgoCD still declares, so the
  next sync recreates them. The pair flaps.

Neither tool is broken. The mistake was letting two of them own one set of
objects. `manifests/argocd-apps/strimzi.yaml` records the same lesson in its
first comment: "not the direct `helm install` used earlier and then undone."

### A label saying Helm does not mean Helm installed it

The Strimzi operator Deployment carries these labels, and it was installed by
ArgoCD:

```
{"app":"strimzi","chart":"strimzi-kafka-operator-1.1.0","heritage":"Helm","release":"strimzi"}
```

Those labels are written **inside the chart's own templates**, so they appear in
the rendered YAML no matter who applies it. `helm list` is the truth. Labels are
not.

### Why `bootstrap-phase0.sh` still uses plain Helm

Ordering, not inconsistency. The script says so at its lines 3 to 10: ArgoCD has
to be installed by something before it can manage anything else, and cert-manager
is bootstrapped alongside it before GitOps exists to take over.

The timestamps prove the sequence:

```
$ helm list -A
cert-manager   2026-08-12 11:39:00
argocd         2026-08-12 16:30:21
```

cert-manager went in 4 hours 51 minutes before ArgoCD existed. At 11:39 there was
no `repo-server` to render anything and no `application-controller` to apply it.
`helm upgrade --install` was the only tool available. It is `upgrade --install`
rather than `install` so a rebuild of the `kind` cluster can re-run the script
safely.

| Component | Installed by | Release Secret | Recorded in Git as |
|---|---|---|---|
| cert-manager | `helm upgrade --install`, in the script | yes | the script |
| argocd | `helm upgrade --install`, in the script | yes | the script |
| strimzi, kafka cluster, minio operator, minio tenant | ArgoCD Application | no | `manifests/argocd-apps/*.yaml` |
| flink operator | ArgoCD Application | no | `manifests/argocd-apps/flink-operator.yaml` |

The rule in one line: **only things that had to exist before ArgoCD could act are
installed by hand.** Everything after that is an Application.

### Reading a chart without installing it

Wanting to see a chart's knobs before writing `valuesObject` is the right
instinct, and it needs no install and no `helm repo add`. `--repo` takes the URL
inline, which also avoids writing to `~/.config/helm/repositories.yaml`:

```bash
helm show values flink-kubernetes-operator \
  --repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/ \
  --version 1.15.0
```

To see exactly what ArgoCD will apply, render it the same way `repo-server`
does. The braces need quoting, or bash expands them into two arguments and Helm
rejects the second:

```bash
helm template flink-kubernetes-operator \
  --repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/ \
  --version 1.15.0 \
  --namespace flink-operator \
  --set 'watchNamespaces={personalization-blue,personalization-green}' \
  --set rbac.nodesRule.create=false \
  --include-crds
```

Both are row 1 of the table at the top. They print YAML and touch nothing.

---

## What RBAC is

Task 4, while reading `rbac.nodesRule.create` in the operator chart's values.

**The problem.** A pod can talk to the Kubernetes API. The Flink JobManager will
create TaskManager pods. But so could any pod, including one running code you did
not write. Kubernetes needs a way to say who may do what.

RBAC is that way. It answers one question:

> May this identity perform this verb on this kind of object?

**Default is no.** An identity with no grant can do nothing.

### The four object kinds

There are two ideas, each in a namespaced and a cluster-wide flavor:

| | What it is | Scope |
|---|---|---|
| `Role` | a list of allowed actions | one namespace |
| `ClusterRole` | the same list | whole cluster |
| `RoleBinding` | attaches a Role to an identity | one namespace |
| `ClusterRoleBinding` | attaches cluster-wide | whole cluster |

A Role by itself grants nothing. It is only a list. The RoleBinding is what
connects the list to somebody.

The identity, for a pod, is a **ServiceAccount**.

```
ServiceAccount  <--- RoleBinding ---> Role
   (who)                              (what is allowed)
```

### What one rule looks like

Every rule has exactly three parts:

```yaml
- apiGroups: [""]          # which family of objects. "" means the core family
  resources: [pods]        # which kind
  verbs: [get, list]       # which actions
```

Read it as a sentence: *may `get` and `list` `pods`.*

The verbs are a fixed set: `get`, `list`, `watch`, `create`, `update`, `patch`,
`delete`, `deletecollection`.

---

## What the Application's own fields decide

Task 4. Three fields in `manifests/argocd-apps/flink-operator.yaml` look like
boilerplate and are not.

### `project: default`

Every `Application` must name a project. The field is required, and there is no
"no project".

An `AppProject` is a rule list. It says which repos an Application may pull from,
which clusters and namespaces it may write to, and which cluster-scoped kinds it
may create. On a shared cluster that is how one team is stopped from deploying
into another team's namespace.

The ArgoCD install creates one, and it is deliberately unrestricted:

```
$ kubectl get appproject default -n argocd -o jsonpath='{.spec}'
sourceRepos:              ["*"]
destinations:             [{namespace: "*", server: "*"}]
clusterResourceWhitelist: [{group: "*", kind: "*"}]
```

The third star matters for this chart. It creates 4 CRDs, and 2 webhook
configurations, all cluster-scoped. Under a restrictive project those kinds would
have to be whitelisted or the sync fails.

This lab has one cluster and one project, so there is nothing to partition.

### `server: https://kubernetes.default.svc`

Which **cluster**, not which namespace. One ArgoCD can manage several.

That string is the Kubernetes API as seen from inside the cluster. Read the DNS
name backwards: Service `kubernetes`, in namespace `default`, `.svc`.

```
$ kubectl get svc kubernetes -n default
NAME         TYPE        CLUSTER-IP   PORT(S)
kubernetes   ClusterIP   10.96.0.1    443/TCP
```

Every cluster has it, and it always points at its own API server. So the value
means "the same cluster ArgoCD runs in". All six Applications use it.

## Two identities, and where the operator's 25 objects land

Task 4, the follow-on to [what RBAC is](#what-rbac-is). Everything below is
`helm template` output for this project's values, not a guess.

### The operator pod

One `Deployment` named `flink-kubernetes-operator`, `replicas: 1`, in namespace
`flink-operator`. It produces one pod, and that pod holds **two containers** from
the same image, started with different commands:

```yaml
serviceAccountName: flink-operator
containers:
  - name: flink-kubernetes-operator
    image: ghcr.io/apache/flink-kubernetes-operator:79d730b
    command: ["/docker-entrypoint.sh", "operator"]     # the controller
  - name: flink-webhook
    image: ghcr.io/apache/flink-kubernetes-operator:79d730b
    command: ["/docker-entrypoint.sh", "webhook"]      # the admission webhook
```

The controller runs a loop: watch `FlinkDeployment` objects in the watched
namespaces, compare them to what exists, create or fix pods to match.

The webhook runs an HTTPS server on port 9443. The API server calls it **before**
storing a `FlinkDeployment`, to reject bad fields early. That call must be over
TLS, which is the entire reason cert-manager is a dependency of this chart.

`replicas: 1` is not a knob to turn. The chart's own comment says it must stay 1
unless leader election is configured, because two controllers reconciling one
object would fight.

The image tag reads `79d730b`, a commit sha, not `1.15.0`. That is what
`values.yaml` line 29 pins in the 1.15.0 chart. It looks wrong and it is correct.

### The full inventory

```
== flink-operator                    9 objects, the operator software itself
   ServiceAccount   flink-operator                    its identity
   Deployment       flink-kubernetes-operator         the pod above
   ConfigMap        flink-operator-config
   Role             flink-operator                    rights in its OWN namespace
   RoleBinding      flink-operator-role-binding
   Service          flink-operator-webhook-service
   Secret           flink-operator-webhook-secret
   Certificate      flink-operator-serving-cert
   Issuer           flink-operator-selfsigned-issuer

== personalization-blue              5 objects
== personalization-green             5 objects, identical
   ServiceAccount   flink                             the JOB's identity
   Role             flink                             what the JOB may do here
   RoleBinding      flink-role-binding
   Role             flink-operator                    what the OPERATOR may do here
   RoleBinding      flink-operator-role-binding

== cluster-scoped                    6 objects, belong to no namespace
   CustomResourceDefinition  flinkdeployments.flink.apache.org
   CustomResourceDefinition  flinkbluegreendeployments.flink.apache.org
   CustomResourceDefinition  flinksessionjobs.flink.apache.org
   CustomResourceDefinition  flinkstatesnapshots.flink.apache.org
   MutatingWebhookConfiguration    flink-operator-...-webhook-configuration
   ValidatingWebhookConfiguration  flink-operator-...-webhook-configuration
```

A CRD adds a new object type to the whole cluster, so `personalization-blue`
cannot own one.

### Why a job namespace gets two Roles

There is no one-Role-per-namespace limit. A namespace holds as many as you like.
`personalization-blue` has two because **two different identities act in it**:

```
identity                              its Role in personalization-blue
─────────────────────────────────     ────────────────────────────────
ServiceAccount flink                  Role/flink
  (lives in personalization-blue)
ServiceAccount flink-operator         Role/flink-operator
  (lives in flink-operator)
```

One Role per identity, not one per namespace.

What each grants, from the rendered output:

| | `Role/flink` (the job) | `Role/flink-operator` (the operator) |
|---|---|---|
| rules | 2 | 6 |
| core resources | `pods`, `configmaps` | `pods`, `services`, `events`, `configmaps`, `secrets` |
| apps resources | `deployments`, `deployments/finalizers` | plus `replicasets`, `deployments/scale` |
| `flink.apache.org` resources | **none** | `flinkdeployments`, `flinksessionjobs`, `flinkstatesnapshots`, and their `/finalizers` and `/status` |
| extra verbs | none | `deletecollection` |

The decisive row is the third. Only the operator may read and write
`FlinkDeployment` objects. That is its whole purpose.

The job's Role is deliberately small. A JobManager needs to create TaskManager
pods, write HA metadata into ConfigMaps, and manage its own Deployment. Nothing
more.

### A namespace is not a wall

The obvious objection: how can a pod in `flink-operator` create pods in
`personalization-blue`?

Because a namespace is a field on an object and a scope for names. It is not a
network boundary, not a process boundary, and not a machine boundary. Every pod
that talks to Kubernetes does the same thing: one HTTPS request to the one API
server, carrying its ServiceAccount token.

```
operator pod (in flink-operator)
      │  POST /api/v1/namespaces/personalization-blue/pods
      │  Authorization: Bearer <token of ServiceAccount flink-operator>
      ▼
  API server
      │  who is this?            -> ServiceAccount flink-operator in flink-operator
      │  may it create pods in   -> checks RoleBindings in personalization-blue
      │  personalization-blue?
      ▼
  yes, because RoleBinding/flink-operator-role-binding grants it
```

The request names the target namespace in its URL. Nothing about the caller's own
namespace restricts it. RBAC is the only thing that decides.

### Where the JobManager and the TaskManagers run

Both run as pods in `personalization-blue`, beside each other. Never in
`flink-operator`.

```
1. you apply a FlinkDeployment into personalization-blue
2. the operator pod (in flink-operator) notices it, and creates a
   Deployment for the JobManager in personalization-blue
3. the JobManager starts, and creates TaskManager pods in personalization-blue
```

Step 3 is the surprise. In `native` mode, which this project uses,
the operator does not create TaskManagers.
[ADR 0005](../adr/0005-autoscaling-two-deployment-modes.md) line 34 says it:

> In `native` mode the JobManager's Kubernetes ResourceManager allocates
> TaskManagers itself from the job's parallelism.

So the JobManager is itself a Kubernetes API client. That is exactly why the job
needs its own ServiceAccount with pod-create rights. Without `Role/flink`, the
operator would start a JobManager successfully, and the JobManager would then
fail to create a single TaskManager.

### The cross-namespace RoleBinding, and the name collision that hides it

A `Role` grants permission only inside its own namespace. There is no way to
write a Role in `flink-operator` that gives power over `personalization-blue`.
So the chart places a Role in every namespace where anything needs permission.

The two RoleBindings in `personalization-blue`, rendered:

```yaml
kind: RoleBinding
metadata:
  name: flink-role-binding
  namespace: personalization-blue
roleRef:
  kind: Role
  name: flink
subjects:
  - kind: ServiceAccount
    name: flink
    namespace: personalization-blue      # same namespace
```

```yaml
kind: RoleBinding
metadata:
  name: flink-operator-role-binding
  namespace: personalization-blue        # the RoleBinding is HERE
roleRef:
  kind: Role
  name: flink-operator                   # no namespace field exists here
subjects:
  - kind: ServiceAccount
    name: flink-operator
    namespace: flink-operator            # belongs to SUBJECTS, not roleRef
```

Two rules, and they are not the same rule:

1. A RoleBinding and the Role it references are **always** in the same namespace.
2. The **subject** it grants to may live anywhere.

`roleRef` cannot name a namespace at all. The API has no such field:

```
$ kubectl explain rolebinding.roleRef
FIELDS:
  apiGroup  <string> -required-
  kind      <string> -required-
  name      <string> -required-

DESCRIPTION:
  RoleRef can reference a Role in the current namespace
  or a ClusterRole in the global namespace.
```

"the current namespace" is the RoleBinding's own. So
`roleRef: Role/flink-operator` resolves to `Role/flink-operator` **in
`personalization-blue`**.

What makes this hard to read is a name collision. The string `flink-operator`
names three different things:

| The string | What it names |
|---|---|
| `namespace: flink-operator` | the namespace the operator pod runs in |
| `ServiceAccount flink-operator` | the operator's identity |
| `Role flink-operator` | a permission list, one copy in each of the three namespaces |

```
flink-operator namespace          personalization-blue namespace
┌────────────────────────┐        ┌──────────────────────────────────┐
│ ServiceAccount         │◄───────┤ RoleBinding flink-operator-...   │
│   flink-operator       │        │        ↓                         │
│ (the operator pod)     │        │ Role flink-operator              │
└────────────────────────┘        │                                  │
                                  │ ServiceAccount flink ◄─┐         │
                                  │ RoleBinding flink-...──┘         │
                                  │        ↓                         │
                                  │ Role flink                       │
                                  └──────────────────────────────────┘
```

The operator reaches across the boundary. The job does not.

### `watchNamespaces` does two jobs

The first is obvious. It tells the running operator where to look for
`FlinkDeployment` objects. One written anywhere else is ignored.

The second is not. It tells the chart where to create the RBAC. This is a literal
loop, `templates/flink/role.yaml` line 21:

```
{{- range .Values.watchNamespaces }}
```

One `Role` per entry. Two entries gives two Roles.

There is a security consequence, verified by rendering both ways: **with
`watchNamespaces` set, the chart creates no `ClusterRole` at all.** You get 5
namespaced Roles instead of cluster-wide power. Leave the list empty and it goes
the other way, and the `flink` ServiceAccount lands only in the release namespace.

### A ServiceAccount exists in both job namespaces, and is used in one

A ServiceAccount does not run. It is not a process and has no container. It is an
identity object, a name a pod claims by writing `serviceAccountName: flink`. On
its own it does nothing.

Because `watchNamespaces` has two entries, the chart creates one in each:

| | `personalization-blue` | `personalization-green` |
|---|---|---|
| ServiceAccount exists | yes | yes |
| A pod claims it | yes, Task 5's `spec.serviceAccount: flink` | no |
| Anything running | JobManager and TaskManager pods | nothing |

An unused ServiceAccount consumes nothing and grants nothing. That is the whole
reason Task 3 created both namespaces before Task 4: listing green now means
Phase 7 does not have to re-sync the operator to give the Standby Side its
permissions.

### What `rbac.nodesRule.create` actually adds

One rule, `list` on `nodes`. Understanding why it exists, and why this project
says `false`, takes six steps from the beginning.

#### 1. A pod's address is useless from the host

The JobManager runs as a pod. Kubernetes gives it an IP like `10.244.2.15`, and
the Flink web UI listens on port `8081`.

```
http://10.244.2.15:8081
```

That works from other pods. It does not work from a browser on the host.
`10.244.x.x` is an internal cluster network the host does not route to. So
something must expose it.

#### 2. A NodePort Service opens the same door on every machine

A Service of type `NodePort` picks one high port, `30011` here, and opens it on
**every** node. This cluster has six:

```
http://172.18.0.3:30011    http://172.18.0.5:30011    http://172.18.0.7:30011
http://172.18.0.4:30011    http://172.18.0.6:30011    http://172.18.0.8:30011
```

All six reach the same JobManager.

```
browser
  │  http://172.18.0.5:30011
  ▼
node worker1     kube-proxy sees port 30011, forwards to the Service
  ▼
JobManager pod   10.244.2.15:8081
```

Note what the JobManager does here: **nothing special**. It listens on 8081.
kube-proxy does the forwarding, and the JobManager never learns which node IP was
dialled. Routing never requires Flink to know a node IP.

#### 3. Two things could create that Service

**Option A.** You write the YAML. That is
`manifests/flink/blue/rest-nodeport.yaml`, and it is Task 5's choice.

**Option B.** The JobManager creates it, selected with the Flink config key
`kubernetes.rest-service.exposed.type: NodePort`.

#### 4. Why Option B is even possible: the JobManager is a Kubernetes API client

Start with what that phrase means. This program is a client of `example.com`:

```python
import requests
requests.post("https://api.example.com/orders", json={"item": "book"})
```

Nothing special happened. It sent an HTTP request to a URL. That is the whole
definition.

The Kubernetes API is an ordinary HTTPS API. `kubectl get nodes` sends:

```
GET https://172.18.0.8:6443/api/v1/nodes
Authorization: Bearer <token>
```

kubectl is not privileged software. It is a program that sends that request. So
is ArgoCD, and so is the Flink operator. "The JobManager is a Kubernetes API
client" therefore means only one thing: **it contains code that sends those
requests.**

That code ships inside the image. Opening `flink-dist-2.2.0.jar` from
`flink:2.2.0`:

```
total entries in the jar                          35,813
under org/apache/flink/kubernetes/                 7,984
the shaded Kubernetes client library
  org/apache/flink/kubernetes/shaded/io/fabric8/   5,872
```

Named classes, present:

```
org/apache/flink/kubernetes/entrypoint/KubernetesApplicationClusterEntrypoint.class
org/apache/flink/kubernetes/KubernetesResourceManagerDriver.class
org/apache/flink/kubernetes/kubeclient/decorators/ExternalServiceDecorator.class
org/apache/flink/kubernetes/kubeclient/services/NodePortService.class
```

Nearly 6,000 classes of Kubernetes client library live inside Flink itself. And
the JobManager is started as one of those classes, per
`/opt/flink/bin/flink-console.sh` in the same image:

```
(kubernetes-application)
    CLASS_TO_RUN=org.apache.flink.kubernetes.entrypoint.KubernetesApplicationClusterEntrypoint
```

The package is `org.apache.flink.kubernetes`. The JobManager process **is** the
Kubernetes-aware entrypoint.

Why Flink carries this at all comes down to the deployment mode:

| Mode | Who creates TaskManagers | Needs a Kubernetes client |
|---|---|---|
| `standalone` | you, with a Deployment YAML | no. Flink knows nothing about Kubernetes |
| `native` | the JobManager, on demand | **yes** |

This project uses `native`, per
[ADR 0005](../adr/0005-autoscaling-two-deployment-modes.md). Once Flink can
create TaskManager pods, creating a Service is the same call with a different
object type. `ExternalServiceDecorator` is that call.

#### 5. Option B has a side effect: printing the URL

If the JobManager creates the Service, it also wants to report where the UI is:

```
Web interface at http://???:30011
```

It knows `30011`. It does not know what to put in `???`. To fill that in, it asks
the API server for the list of nodes and their IPs. That request is `list` on
`nodes`, and it is the only reason `nodesRule` exists.

Flink's configuration documentation says it in those terms: with NodePort, node
IPs are filtered "for the connection string". A connection string is text to
display, not a route.

#### 6. Option A moves that lookup to a human

With a hand-written Service, nothing inside the cluster ever asks for the node
list. The lookup happens at a terminal, with an admin kubeconfig:

```bash
kubectl get nodes -o wide          # pick any node IP, for example 172.18.0.5
# open http://172.18.0.5:30011
```

So yes, the address is looked up by hand rather than printed by Flink. Three
things make that cheap:

1. It happens once, and the result goes into a runbook.
2. The IPs are stable here. `clusters/kind/.node-ips` records each `kind` node
   container's pinned IP, and `scripts/session-start.sh` restores it after a WSL
   restart.
3. `kubectl port-forward` skips node IPs entirely when only the UI is wanted.

#### What the knob actually inserts

Setting `rbac.nodesRule.create: true` adds these three lines to a Role, and
nothing else. Confirmed by rendering the chart twice and diffing:

```yaml
- apiGroups: [""]
  resources: [nodes]
  verbs: [list]
```

#### And here it would not even work

A bonus reason, not the main one. A `Role` grants permissions inside one
namespace. A `Node` belongs to no namespace, the same way a CRD does. So a rule
about `nodes` in a namespaced Role can never match anything. Granting node access
needs a `ClusterRole` plus a `ClusterRoleBinding`, and with `watchNamespaces` set
this chart creates no ClusterRole at all.

#### The trade, in one table

| | Option A, Task 5's choice | Option B |
|---|---|---|
| Who creates the Service | you, in a YAML file | the JobManager |
| Flink config | `ClusterIP`, the default | `NodePort` |
| Who looks up node IPs | you, at a terminal | the JobManager, in a pod |
| Why | to write the runbook once | to print a URL at every startup |
| Whose credentials | an admin kubeconfig | `ServiceAccount flink` |
| `nodesRule` needed | **no** | yes |

Traffic flows identically either way, because kube-proxy routes it in both. The
only difference is which identity has to compose the URL text.

### The value next to it that is destructive

`rbac.create` and `rbac.nodesRule.create` are two different knobs. Indentation is
the only thing separating them, and getting it wrong removes every permission the
chart creates.

#### 3. What `rbac.create` actually is

The master switch for everything RBAC in this chart. From `values.yaml`:

```yaml
rbac:
  create: true          # ALL RBAC in this chart
  nodesRule:
    create: false       # only the cluster-scoped node list permission
  operatorRole:
    create: true
  jobRole:
    create: true
    name: "flink"
```
