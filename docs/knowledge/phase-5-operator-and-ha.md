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

---

## The opening block of a `FlinkDeployment`, field by field

Came up in Task 5 Step 1. Four of the five fields look like boilerplate and are
not. Reading them top to bottom.

```yaml
spec:
  image: lab/personalization-pipeline:0.1-<sha>
  imagePullPolicy: IfNotPresent
  flinkVersion: v2_2
  mode: native
  serviceAccount: flink
```

### `imagePullPolicy`, and why the image is never pulled

The image was never in a registry. It took this path:

```
docker build   ->  the host's Docker daemon
kind load      ->  copies the bytes straight into each node's containerd store
```

`kind load` bypasses registries entirely. It is a file copy into the node
containers.

Look at how a node then names it:

```
IMAGE                                    TAG
docker.io/lab/personalization-pipeline   0.1-b606416-dirty
```

Note `docker.io/`. A name with no registry host defaults to Docker Hub, so the
node has recorded that this image *belongs to*
`docker.io/lab/personalization-pipeline`. **No such repository exists.** Anything
that tries to pull it goes to Docker Hub, finds nothing, and the pod lands in
`ErrImagePull`.

`IfNotPresent` means "only pull if it is not already here". It is already here,
so no pull happens, so the missing registry never matters. Set it to `Always` and
every pod start would try Docker Hub and fail, with the bytes sitting on the node
the whole time.

The default policy is already `IfNotPresent` for any tag other than `latest`, so
writing it changes nothing. It is written to make the reason visible rather than
resting on a rule the reader has to know.

### `mode`, or: who creates the TaskManagers

A Flink job needs TaskManagers, and something has to create those pods. That is
the entire difference between the two modes.

| | `standalone` | `native` |
|---|---|---|
| Who creates TaskManager pods | the **operator**, as an ordinary Deployment | the **JobManager itself**, through the Kubernetes API |
| Does Flink know it is on Kubernetes | no, it believes it is on bare machines | yes, it has a Kubernetes client |
| How the count is set | you write `spec.taskManager.replicas` | you do not. It is derived |

**A worked example with this project's numbers.** `parallelism: 6` and
`taskmanager.numberOfTaskSlots: 2`.

In `native`, the JobManager computes `6 / 2 = 3` and creates three pods. The
number three is never written anywhere.

In `standalone`, you would write `spec.taskManager.replicas: 3` yourself. Write
`2` by mistake and the job submits, then waits for slots that never arrive.
Nothing errors. It simply never runs.

### Why that makes [ADR 0005](../adr/0005-autoscaling-two-deployment-modes.md) true

In `standalone` there is a real object with a real replica count:

```
operator creates:  Deployment/<name>-taskmanager  replicas: 3
                              ^
KEDA ScaledObject scaleTargetRef

```

KEDA changes that number, pods appear, and Flink's reactive mode rescales.

In `native` that Deployment is never created. So a `ScaledObject` pointing at it
references an object that does not exist.

The intuition to correct is that Flink and KEDA would **fight** over the count.
They cannot. There is no shared object to fight over. That is what "a no-op, not
a conflict" means.

### Two things named after TaskManagers that are not the same thing

Easy to conflate, and only one of them is standalone-only.

| | `spec.taskManager.replicas` | `taskmanager.numberOfTaskSlots` |
|---|---|---|
| What it is | a **CR field** on the FlinkDeployment | a **Flink config key**, inside `spec.flinkConfiguration` |
| What it sets | how many TaskManager pods | how many slots **each** TaskManager offers |
| In `native` mode | inert | **required, and it is the dial** |

In native mode the pod count is derived, and slots is one of the two inputs:

```
TaskManagers = parallelism / numberOfTaskSlots

parallelism 6, slots 2  ->  3 pods      <- this project
parallelism 6, slots 1  ->  6 pods
parallelism 6, slots 3  ->  2 pods
```

The rest of the `taskManager` block is still needed in native mode.
`resource.memory` sizes each pod and the `podTemplate` carries the Zone spread
constraint. **Only `replicas` is the standalone-only field.**

### What this predicts about the pod tree

Because the JobManager creates TaskManager pods through the API server rather
than through a ReplicaSet, the `Deployment -> ReplicaSet -> Pod` chain does not
apply to them:

```bash
kubectl get deploy -n personalization-blue   # expect ONE, the JobManager
kubectl get rs     -n personalization-blue   # expect ONE, also the JobManager's
```

Whether the TaskManager pods carry an `ownerReferences` entry pointing at the
JobManager, for garbage collection, is a separate question and is not asserted
here. Read it from the cluster once they exist:

```bash
kubectl get pod -n personalization-blue -l component=taskmanager \
  -o jsonpath='{range .items[*]}{.metadata.name}{" -> "}{.metadata.ownerReferences[*].kind}{"\n"}{end}'
```

### `serviceAccount: flink` is a reference, not a choice

The name was chosen by the operator's Helm chart, in Task 4, from its own
defaults. `values.yaml`:

```yaml
rbac:
  jobRole:
    create: true
    name: "flink"
jobServiceAccount:
  create: true
  name: "flink"
```

Neither key was overridden. The chart loops over `watchNamespaces` and creates a
ServiceAccount of that name in each, which is why both job namespaces have one.

**Native mode is what makes this load-bearing.** The Role bound to that
ServiceAccount grants:

```
[""]      pods, configmaps   -> get list watch create update patch delete
["apps"]  deployments        -> get list watch create update patch delete
```

Those permissions are attached to the JobManager pod, and native mode needs
exactly them:

- `pods: create, delete`, so the JobManager can make TaskManagers.
- `configmaps: create, update, delete`, because Kubernetes HA keeps leader
  election and the recovery pointer in ConfigMaps. That is what
  `high-availability.type: kubernetes` means.

In `standalone` the JobManager would need none of this, because it never talks to
the API server.

Write any other name in `spec.serviceAccount` and the JobManager starts with a
ServiceAccount that has no Role bound. Its first attempt to create a TaskManager
fails with an RBAC `forbidden` error, and nothing in that message mentions the
name being wrong.

---

## How a value reaches a Helm chart through ArgoCD, and why nothing catches a typo

ArgoCD does not understand `watchNamespaces`. It never inspects it. It forwards
it.

```
Application
  spec.source.helm.valuesObject:
      watchNamespaces: [personalization-blue, personalization-green]
        |
        |  argocd-repo-server fetches the chart and renders it with those values
        v
the chart's own template reads it:
  templates/flink/service_account.yaml
      {{- if .Values.watchNamespaces }}
      {{- range .Values.watchNamespaces }}
        namespace: {{ . }}
        |
        v
plain YAML manifests
        |
        |  argocd-application-controller applies them
        v
sa/flink in personalization-blue, sa/flink in personalization-green
```

So `valuesObject` is a **passthrough**, not an ArgoCD feature that knows anything
about Flink. The chart author chose the key names. This is why a chart's own
`values.yaml` is the authoritative list of what may be set: nothing in ArgoCD's
documentation could tell you `watchNamespaces` exists.

### The demonstration

Rendering the real chart both ways. Correct spelling:

```
name: flink   namespace: personalization-blue
name: flink   namespace: personalization-green
```

The same command with one letter dropped, `watchNamespace`:

```
name: flink   namespace: default
exit code: 0
```

**No error.** The template's `{{- if .Values.watchNamespaces }}` found nothing,
took the other branch, and created one ServiceAccount in the release namespace.
The extra key sat there unread.

In this cluster the consequence would have been: no `flink` ServiceAccount in
either job namespace, cluster-scoped RBAC instead of namespaced, and a Task 5
JobManager failing with an RBAC `forbidden` error on its first TaskManager.
Nothing in that error would mention a typo.

### Why no layer catches it

A chart can ship a `values.schema.json`, which makes Helm reject unknown or
wrongly typed keys. **This chart does not ship one**, so there is no net at any
layer:

| Layer | Catches a misspelled value key? |
|---|---|
| ArgoCD | no, it forwards without inspecting |
| Helm | no, absent a `values.schema.json` |
| the chart template | no, a missing value is indistinguishable from an unset one |
| the API server | no, it only ever sees the rendered output |

The only check is looking at what actually landed:

```bash
for ns in personalization-blue personalization-green; do kubectl get sa flink -n $ns; done
```

This is the general reason the plan says a task is done when a verification
command produces real output that was read. `Synced` and `Healthy` would both
have been green with the typo in place.

---

The sections below were written together during Task 5, from questions asked
while writing `manifests/flink/blue/` and `manifests/argocd-apps/flink-job-blue.yaml`.
They are grouped by topic rather than by the order the confusion arrived.

Every observed value in them was read from this cluster on 2026-09-06.

## Eight version numbers, and the seam that creates each one

The opening lines of a `FlinkDeployment` carry three different version numbers
that look interchangeable and are not:

```yaml
apiVersion: flink.apache.org/v1beta1     # 1
kind: FlinkDeployment
spec:
  image: lab/personalization-pipeline:0.1-86a77e6
  flinkVersion: v2_2                     # 2, and the image tag is a third
```

### The principle

**A version number exists at every seam where two independently released things
must agree.** Count the seams and you have counted the versions. Nobody chose to
have this many. Each one appeared because two components needed to be swappable
without breaking the other.

A `FlinkDeployment` sits at the top of five handoffs:

```
   you write YAML
        │
        │◀─ seam 1 ─  apiVersion: flink.apache.org/v1beta1
        ▼             "what shape may this document have?"
   Kubernetes API server        validates it, stores it in etcd
        │
        │◀─ seam 2 ─  operator 1.15.0
        ▼             "which controller acts on it?"
   the operator pod             watches, decides, writes pod specs
        │
        │◀─ seam 3 ─  flinkVersion: v2_2
        ▼             "which Flink behaviour should the operator assume?"
   the pod spec it writes
        │
        │◀─ seam 4 ─  image: ... FROM flink:2.2.0
        ▼             "which Flink binary actually starts?"
   the JVM in the pod
        │
        │◀─ seam 5 ─  build.gradle  flinkVersion = '2.2.0'
        ▼             "which Flink API did the Java compile against?"
   the job classes
```

### What `apiVersion` is made of

```
flink.apache.org  /  v1beta1
       │                │
       │                └── the VERSION of that schema
       └─────────────────── the GROUP, a namespace for kinds
```

The group exists to prevent name collisions. Anyone can define a CRD called
`Deployment`. The group is a DNS name because DNS names are already globally
owned, so `flink.apache.org` cannot clash with anything.

The contrast is visible inside this repo. `manifests/minio/s3-nodeport.yaml`
opens with `apiVersion: v1`, no slash. That is the **core group**, whose name is
the empty string. It shipped with Kubernetes before groups existed and kept the
short form. Everything added later carries a group.

### The three in the file, compared

| | `v1beta1` | `1.15.0` | `v2_2` |
|---|---|---|---|
| Written where | `apiVersion:` in `flinkdeployment.yaml` | `targetRevision:` in `flink-operator.yaml` | `flinkVersion:` in `flinkdeployment.yaml` |
| Names | the **schema** of the YAML | the **controller binary** | the **runtime behaviour** the controller assumes |
| Read by | the Kubernetes API server | Helm, at install time | the operator |
| Installed by | the Helm chart, as a CRD | the Helm chart, as a Deployment | nothing. It is a value, not software |
| A wrong value gives | `no matches for kind` at apply time | nothing reconciles. The object sits in etcd forever | a pod spec the runtime rejects |
| The failure is | loud, immediate | silent | delayed, and reads like a Flink bug |

That last row is why three numbers are worth the confusion. They fail in three
different places, at three different times, and each one says where to look.

### The three that must agree, and what checks them

```
flinkVersion: v2_2                     (the FlinkDeployment)
FROM flink:2.2.0                       (the Dockerfile)
flinkVersion = '2.2.0'                 (build.gradle)
```

| Pair | Checked by | When |
|---|---|---|
| `build.gradle` 2.2.0 vs the API the code calls | the Java compiler | build time, loudly |
| `build.gradle` 2.2.0 vs `FROM flink:2.2.0` | **nothing** | never |
| `flinkVersion: v2_2` vs `FROM flink:2.2.0` | **nothing** | never |

The second row follows from `build.gradle` declaring almost everything
`compileOnly`:

```groovy
compileOnly "org.apache.flink:flink-streaming-java:${flinkVersion}"
runtimeOnly "org.apache.flink:flink-streaming-java:${flinkVersion}"
```

`compileOnly` means "compile against this, do not put it in the jar", which is
correct because `flink:2.2.0` already ships those classes in `/opt/flink/lib`.
That is also why the Shadow jar is 24 mb and carries only two allowlisted
dependencies. The consequence is that the job compiles against 2.2.0 classes and
then runs against whatever classes the image holds.

**Change those three together, always.** They are one decision written in three
files, and no tool reports it if only two are edited.

### The second row, observed for real on 2026-09-07

The first deploy of `flink-job-blue` crash-looped both JobManagers twelve times.
The whole diagnosis is one line of the stack trace:

```
java.lang.UnsupportedClassVersionError: lab/personalization/pipeline/PersonalizationJob
has been compiled by a more recent version of the Java Runtime (class file
version 65.0), this version of the Java Runtime only recognizes class file
versions up to 61.0
```

Class file version **65 is Java 21**. Class file version **61 is Java 17**.

| Side | Value | Where it is set |
|---|---|---|
| built with | Java 21 | `apps/pipeline/build.gradle:11`, `javaVersion = '21'`, and the same toolchain in `apps/domain` and `apps/generator` |
| runs on | Java 17 | `apps/pipeline/Dockerfile`, `FROM flink:2.2.0`, whose JVM is Temurin 17.0.19 |

**Nothing caught it, exactly as the table above predicts.** The Gradle build
succeeded. The Shadow jar was produced. The image built. `kind load` worked. The
server-side dry run passed. ArgoCD reported Synced and Healthy. The failure
appeared only when a JVM tried to load the class.

**And `:pipeline:run` against `MiniCluster` still works**, because that runs on
the host's Java 21. The local run never exercises this seam. That is what makes
the seam unchecked: the two sides only meet inside the container.

The fix that fits this phase's constraints is the image side, not the build
side. `apps/domain` is bundled into the Shadow jar (`bundled project(':domain')`
at `apps/pipeline/build.gradle:79`), so compiling for Java 17 would mean editing
`apps/domain/build.gradle` too, which the global constraints forbid. Apache
publishes per-JDK image variants, all confirmed present on 2026-09-07:

```
flink:2.2.0-java21   exists
flink:2.2.0-java17   exists
flink:2.2.0-java11   exists
```

One word in `apps/pipeline/Dockerfile` closes it, with no Java changes at all.

**The lesson generalises past Java.** A base image pins a JDK, a JVM, a libc, and
a set of system libraries. None of those appear in `build.gradle`, and no tool in
the chain compares them.

Verifying the agreement, once pods are healthy:

```bash
kubectl exec -n personalization-blue deploy/personalization -- flink --version
kubectl exec -n personalization-blue deploy/personalization -- java -version
```

## The anatomy of a `FlinkDeployment`

### Four kinds of thing in one tree

```
FlinkDeployment
└── spec
    ├── image, imagePullPolicy          ─┐
    ├── flinkVersion, mode               ├─ identity
    ├── serviceAccount                  ─┘
    │
    ├── flinkConfiguration: {}     ──▶ rendered to /opt/flink/conf/config.yaml in EVERY pod
    │
    ├── podTemplate: {}            ──▶ merged into BOTH roles
    │
    ├── jobManager
    │   ├── replicas: 2
    │   ├── resource: {memory, cpu}     ──▶ the container's Kubernetes limits
    │   └── podTemplate: {}             ──▶ merged ON TOP of spec.podTemplate, JobManagers only
    │
    ├── taskManager
    │   ├── resource: {memory, cpu}
    │   └── podTemplate: {}             ──▶ merged ON TOP of spec.podTemplate, TaskManagers only
    │
    └── job                       ──▶ present = Application cluster. Absent = Session cluster.
```

The four layers are easy to conflate, and each speaks a different language:

| Layer | Reaches | Written as |
|---|---|---|
| `flinkConfiguration` | the Flink runtime, inside the JVM | Flink config keys, all string values |
| `podTemplate` | the Kubernetes pod spec | ordinary Kubernetes YAML |
| `resource` | the container's requests and limits | the operator's own shorthand |
| `job` | the operator's lifecycle controller | operator fields, not Flink fields |

### `resource`, not `resources`, on operator 1.15.0

Upstream documentation on the operator's `main` branch marks `resource`
deprecated in favour of `resources`, which takes the Kubernetes
`ResourceRequirements` shape with `requests` and `limits`. **That field does not
exist on the CRD this cluster has installed.** Read on 2026-09-06:

```bash
kubectl get crd flinkdeployments.flink.apache.org \
  -o jsonpath='{.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.jobManager.properties}' \
  | python3 -c "import sys,json; print(list(json.load(sys.stdin).keys()))"
```

```
['podTemplate', 'replicas', 'resource']
```

Three keys. `resources` is absent. The question is settled by the installed CRD,
not by the docs, which describe a later release. This is the same discipline
Task 4 Step 5 applied to the `flinkVersion` enum.

### Why `mode: native` makes `spec.taskManager.replicas` meaningless

In `native` mode the JobManager is a Kubernetes API client and creates
TaskManager pods itself, as many as `parallelism / numberOfTaskSlots` needs. In
`standalone` mode the operator creates a fixed count from
`spec.taskManager.replicas`.

This is exactly what [ADR 0005](../adr/0005-autoscaling-two-deployment-modes.md)
records: a KEDA `ScaledObject` against a `native` deployment is a no-op, not a
conflict.

### Why there are two `podTemplate` levels

**The problem.** Two populations of pods with different jobs:

| | JobManager | TaskManager |
|---|---|---|
| Count | 2, one leader and one standby | 3 |
| Runs `main` | yes, leader only | never |
| Holds RocksDB state | no | yes |

Some settings are identical for both. The four environment variables are, since
the JobManager writes HA metadata and checkpoint metadata to MinIO while the
TaskManagers write checkpoint data. Same Secret, same keys, same names.

Some must differ. The Zone spread constraints differ in two ways at once:

```yaml
# JobManager
  whenUnsatisfiable: DoNotSchedule
  labelSelector: { matchLabels: { app: personalization, component: jobmanager } }

# TaskManager
  whenUnsatisfiable: ScheduleAnyway
  labelSelector: { matchLabels: { app: personalization, component: taskmanager } }
```

**Why one level could not express this.** A `topologySpreadConstraint` counts
the pods its `labelSelector` matches. To cover both roles from `spec.podTemplate`
the selector would have to be `app: personalization` alone, which treats all five
pods as one population. Three Zones, `maxSkew: 1`, five pods in one group:

```
zone-a: 2 pods    zone-b: 2 pods    zone-c: 1 pod
```

That is a valid spread. It says nothing about whether `zone-a` holds two
JobManagers, which is the exact thing the constraint was written to prevent. The
constraint is satisfied and HA is broken.

Two constraints, one per role, give the guarantee:

```
jobmanagers:   zone-a: 1   zone-b: 1   zone-c: 0
taskmanagers:  zone-a: 1   zone-b: 1   zone-c: 1
```

**Why not skip the shared level and write everything per role.** That duplicates
the four environment variables, 24 lines. Then a Secret key is renamed and one
copy is fixed. The JobManager writes HA metadata fine and the TaskManagers fail
on checkpoint with an S3 403 twenty minutes later.

The merge order, from the reference doc for `jobManager.podTemplate`: "It will be
merged with `FlinkDeploymentSpec.podTemplate`."

```
      operator's generated base pod
                  │
                  ▼
      merge  spec.podTemplate                 <- shared. Both roles.
                  │
                  ▼
      merge  spec.<role>.podTemplate          <- role specific. Wins on conflict.
                  │
                  ▼
             final pod
```

### Why the container must be named `flink-main-container`

**The problem.** The operator already built a pod spec with one container in it,
holding the image and the Flink entrypoint. A second, partial container spec
arrives from `podTemplate`. The operator must decide: is this a **patch to the
existing container**, or a **new sidecar**?

It decides by name. Nothing else. Names are the only stable identity a container
has.

`flink-main-container` is the fixed constant Flink's Kubernetes support uses for
that container. It is not configurable. The operator's own pod-template example
marks the line `# Do not change the main container name`.

The simplest possible version, merging two lists of dictionaries by a `name` key:

```python
base     = [{"name": "engine", "image": "car:1.0"}]
override = [{"name": "engine", "fuel": "diesel"}]
# merged  -> [{"name": "engine", "image": "car:1.0", "fuel": "diesel"}]
```

Now misspell the name in the override:

```python
base     = [{"name": "engine", "image": "car:1.0"}]
override = [{"name": "engnie", "fuel": "diesel"}]
# merged  -> [{"name": "engine", "image": "car:1.0"},
#             {"name": "engnie", "fuel": "diesel"}]
```

Two entries. No error. The first never got the fuel.

Mapping back: `engine` is `flink-main-container`, `fuel` is the four environment
variables, and the second entry is a container Kubernetes tries to start with no
image. It fails with a message about a missing image, never about Flink or
credentials.

**A merge nuance for later.** The operator's default for merging arrays is by
**position**, not by name. `kubernetes.operator.pod-template.merge-arrays-by-name`
switches it. This does not affect the main container, which Flink finds by its
constant name. It does matter if sidecars are ever added in more than one layer.

### The `job` block

**Its presence is a switch.** A `FlinkDeployment` **without** a `job` block is a
Session cluster: it starts JobManagers and TaskManagers and waits, and several
jobs share it. A `FlinkDeployment` **with** one is an Application cluster: the
cluster exists for exactly one job, `main` runs inside the JobManager, and when
the job ends the cluster ends.

| Field | Value here | What it does |
|---|---|---|
| `jarURI` | `local:///opt/flink/usrlib/pipeline.jar` | three slashes. `local://` means "already inside the image", then an absolute path. Matches the Dockerfile's `COPY` target |
| `parallelism` | `6` | subtasks per operator. With 2 slots per TaskManager this fixes the pod count at 3, one per Zone, which is what makes Drill C legible |
| `state` | `running` | the **desired** state, not the observed one. Drill D's Lua action flips it to `suspended` |
| `upgradeMode` | `savepoint` | how state survives a restart |
| `args` | one `--bootstrap-servers` | passed to `main` as `String[] args` |

**`upgradeMode`, the three values.** The problem: the spec changes, the operator
must stop the running job and start a new one, and five operators hold RocksDB
state built up over hours.

| Value | How it stops the job | State kept | Works on a failing job |
|---|---|---|---|
| `stateless` | cancels | **no, state is discarded** | yes |
| `savepoint` | takes a savepoint, then cancels | yes, from the savepoint | **no, the job must be running** |
| `last-state` | deletes the JobManager deployment | yes, from the last checkpoint via HA metadata | yes |

`savepoint` is chosen for Phase 7. Promotion suspends the Active Side with a
savepoint and resumes the Standby Side from it. That is also why
`execution.checkpointing.savepoint-dir` is set. Without it, `savepoint` mode has
nowhere to write and the upgrade fails.

**A behaviour to know before Drill D.** Resuming a suspended job inherits
`upgradeMode` from the last reconciled spec rather than from what was just
written, unless `stateless` is explicitly requested. The operator source is
explicit that it "strictly prohibits switching to stateless mode to avoid state
loss."

**Only `--bootstrap-servers` is passed.** Every other `PipelineConfig` default is
already correct on the cluster. Passing a value that is not changing is a place
for the two environments to silently diverge later.

**Fields not used here, worth recognising.**

| Field | What it does |
|---|---|
| `entryClass` | the main class. Omitted because the Shadow jar sets `Main-Class` in its manifest |
| `initialSavepointPath` | restore from this savepoint on **first** deploy |
| `allowNonRestoredState` | tolerate saved state that no longer maps to any operator |
| `savepointTriggerNonce` | change the number to take a savepoint by hand |
| `checkpointTriggerNonce` | change the number to take a checkpoint by hand |
| `savepointRedeployNonce` | change the number to redeploy from `initialSavepointPath`. **No rollback after this** |

A "nonce" is a number whose value is meaningless. Only the **change** matters.
This is how a declarative API expresses a one-off action: there is no verb, so
an edit is made that the controller notices exactly once.

## Heap, off-heap, and how Flink sizes a TaskManager

### The problem, before the mechanism

A TaskManager is a JVM inside a container. The container has a hard memory
limit. The JVM has a heap, plus off-heap regions the JVM itself does not count.
Size only the heap and the container is OOMKilled by memory the JVM never
reported. Size only the container and Flink has no number from which to give
RocksDB a budget.

### What the heap actually is

When Java runs `new byte[1000]`, that array goes in the **heap**. The garbage
collector owns the heap. `-Xmx` caps it. That is the only region `-Xmx` covers.
Everything else the process consumes is **off-heap**.

### The simplest possible demonstration

```java
public class Leak {
    public static void main(String[] args) throws Exception {
        java.util.List<java.nio.ByteBuffer> keep = new java.util.ArrayList<>();
        for (int i = 0; i < 500; i++) {
            keep.add(java.nio.ByteBuffer.allocateDirect(1024 * 1024));  // 1 mb, DIRECT
        }
        Thread.sleep(600_000);
    }
}
```

Run it with `java -Xmx100m Leak`:

| Measurement | Value | Why |
|---|---|---|
| Java heap used | about 10 mb | the `ArrayList` holds 500 small handle objects |
| Process RSS | about 510 mb | the 500 mb of buffers live outside the heap |

`allocateDirect` asks the operating system directly. The garbage collector never
sees those 500 mb. `-Xmx100m` is satisfied and reports no problem.

Put that in a container with `memory: 200Mi`. At about 200 mb of RSS the kernel
kills the process. Exit code **137**. No Java stack trace, no
`OutOfMemoryError`. The JVM had nothing to complain about.

### What lives off-heap

| Region | What puts it there |
|---|---|
| JVM Metaspace | class metadata, one entry per loaded class |
| Thread stacks | roughly 1 mb per thread, allocated by the OS |
| JIT code cache | compiled machine code |
| Direct byte buffers | `ByteBuffer.allocateDirect`, for network I/O without a copy |
| Native allocations | memory a C or C++ library asks for itself |

Two matter enormously in Flink. **Network buffers are direct byte buffers**,
because a shuffle writes to sockets and a direct buffer avoids a copy through the
heap. **RocksDB is a C++ library**, so its memtables, block cache, and index
blocks are native allocations the JVM cannot see. `state.backend.type: rocksdb`
puts every keyed state entry into that C++ heap.

### The layers, for a 2 gb TaskManager

```
┌──────────────────────────────────────────────────────────────────┐
│  TOTAL PROCESS MEMORY = 2048.0 mb                                │
│  = taskmanager.memory.process.size = the container limit         │
│                                                                   │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  TOTAL FLINK MEMORY = 1587.2 mb                            │  │
│  │                                                             │  │
│  │  ═══ JVM HEAP ══════════════════ capped by -Xmx ══════════ │  │
│  │    Framework Heap       128.0 mb   Flink's own objects     │  │
│  │    Task Heap            440.3 mb   the job's objects       │  │
│  │                         ───────                            │  │
│  │                          568.3 mb  ──▶ -Xmx568m            │  │
│  │                                                             │  │
│  │  ═══ OFF-HEAP, direct ═ capped by -XX:MaxDirectMemorySize ═ │  │
│  │    Framework Off-heap   128.0 mb                           │  │
│  │    Task Off-heap          0.0 mb                           │  │
│  │    Network              256.0 mb   ◀── min = max = 256mb   │  │
│  │                         ───────                            │  │
│  │                          384.0 mb  ──▶ -XX:MaxDirectMemorySize=384m
│  │                                                             │  │
│  │  ═══ OFF-HEAP, native ═ capped by NOTHING the JVM knows ══ │  │
│  │    Managed Memory       634.9 mb   ◀── RocksDB lives here. │  │
│  │                                        Flink passes this   │  │
│  │                                        number to RocksDB   │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                   │
│  ═══ OFF-HEAP, outside Flink's own budget ═══════════════════════ │
│    JVM Metaspace          256.0 mb  ──▶ -XX:MaxMetaspaceSize=256m │
│    JVM Overhead           204.8 mb  thread stacks, JIT cache.     │
│                                     Slack, sized at 10%           │
└──────────────────────────────────────────────────────────────────┘
```

**The key idea.** `process.size` is the only number given to Flink. Flink
subtracts downward from it and then **generates the JVM flags** on the right of
that diagram. That is how the JVM's view and the container's view are made to
agree.

### The derivation order, and why the base changes

```
1. JVM Overhead   = 10% of PROCESS size          = 204.8 mb
2. JVM Metaspace  = fixed default                = 256.0 mb
3. TOTAL FLINK    = 2048 - 204.8 - 256           = 1587.2 mb
4. Network        = min and max both 256mb       =  256.0 mb   (fraction ignored)
5. Managed        = 40% of TOTAL FLINK           =  634.9 mb
6. Framework      = 128 heap + 128 off-heap      =  256.0 mb
7. Task Heap      = 1587.2 - 256 - 634.9 - 256   =  440.3 mb   (whatever is left)
```

Step 1 uses **process** memory. Step 5 uses **Flink** memory. Different bases, so
raising `process.size` does not raise every region proportionally. **Task Heap is
the leftover**, never configured. Anything added elsewhere comes out of the job's
own objects.

Defaults confirmed from the Flink 2.2.0 configuration reference:
`jvm-overhead.fraction` 0.1 with min 192mb and max 1gb, `jvm-metaspace.size`
256mb, `managed.fraction` 0.4, `framework.off-heap.size` 128mb,
`memory.segment-size` 32kb.

### Why `network.min` and `network.max` are equal

Left alone, network memory is 10% of Total Flink Memory, clamped by min and max.
It then moves whenever any other memory number moves. The Flink doc states the
idiom exactly: "The exact size of Network Memory can be explicitly specified by
setting the min/max size to the same value." Pinning both ends makes it a
constant.

Buffer count follows from the segment size:

```
256 mb / 32 kb = 8192 buffers
```

Phase 4 Task 4 hit `Insufficient number of network buffers: required 17, but
only 0 available`. The requirement grows with the **square** of parallelism,
because every upstream subtask needs a channel to every downstream subtask. At
parallelism 6 one shuffle is 6 x 6 = 36 channels, and this graph has five `keyBy`
shuffles. 8192 buffers is deliberate headroom.

### Why the container must be at least as large

`process.size` is what Flink promises to stay under. The container limit is what
Kubernetes enforces. If the container limit is smaller, RocksDB grows into its
634.9 mb of managed memory, the container crosses its limit, and the kernel
OOMKills the pod. The Flink log shows nothing wrong, because Flink stayed inside
its own budget. The Flink doc puts it in one line: "In containerized setups, this
should be set to the container memory."

### Reading the real numbers instead of trusting the arithmetic

```bash
kubectl logs -n personalization-blue -l component=taskmanager --tail=300 \
  | grep -iE "memory|Xmx|MaxDirect|Metaspace"

kubectl exec -n personalization-blue deploy/personalization -- \
  grep -i memory /opt/flink/conf/config.yaml
```

The second command answers something worth knowing. `taskmanager.memory.process.size`
is set in `flinkConfiguration`, **and** the operator also derives it from
`spec.taskManager.resource.memory`. Both say 2048m here, so nothing breaks. Read
the rendered file to learn which one wins, before either is ever changed alone.

## Three Services in front of one JobManager

### A Service is not a property of a pod

A pod has an IP, and that IP changes when the pod restarts. A Service is a
separate object holding a **stable** address that forwards to whichever pods
match its `selector`.

The part that is easy to miss: **a pod can sit behind any number of Services at
once.** Services are not exclusive and do not own the pod. Each is a forwarding
rule that happens to point at the same place.

```
                    ┌──────────────────────────┐
Service A ─────────▶│                          │
Service B ─────────▶│   JobManager pod :8081   │
Service C ─────────▶│                          │
                    └──────────────────────────┘
```

### What Flink creates on its own in `mode: native`

In native mode the JobManager is a Kubernetes API client and creates objects.
Two are Services, both named from `kubernetes.cluster-id`, which the operator
sets to `metadata.name`:

```
<metadata.name>          -> headless, internal RPC        (6123, 6124)
<metadata.name>-rest     -> REST API and Web UI           (8081)
```

Evidence, from the operator's own documentation. The quick start uses
`kubectl port-forward svc/basic-example-rest 8081`. A real listing in the
development guide shows both rows:

```
NAME                         TYPE           CLUSTER-IP     PORT(S)
basic-session-example        ClusterIP      None           6123/TCP,6124/TCP
basic-session-example-rest   LoadBalancer   10.96.36.250   8081:30572/TCP
```

`CLUSTER-IP: None` is what headless means. No virtual IP, and DNS returns the pod
IPs directly. 6123 is JobManager RPC and 6124 is the blob server.

**Open question for this deployment.** With `high-availability.type: kubernetes`,
TaskManagers find the leader through the HA ConfigMap rather than through a
Service. Whether Flink then skips the internal `personalization` Service is not
verified for 2.2.0. Read `kubectl get svc -n personalization-blue` after Task 5
Step 9 and record the answer here.

### Why `kubernetes.rest-service.exposed.type: ClusterIP`

That key controls the type of exactly one Service, `<name>-rest`.

| Value | Effect | Usable here |
|---|---|---|
| `ClusterIP` | internal IP only | yes |
| `NodePort` | also opens a port on every node, **chosen by Flink** | no |
| `LoadBalancer` | asks the cloud for a load balancer | no, `kind` is not a cloud |

`KubernetesConfigOptions` in `flink-dist-2.2.0.jar` has
`kubernetes.rest-service.exposed.type` and `.exposed.node-port-address-type`, and
**no key for the port number at all**. So `NodePort` yields a random port like
31847. Host access needs exactly 30011, because `clusters/kind/kind-cluster.yaml`
published that specific port at cluster creation and `kind` port mappings are
fixed at creation time.

So the work is split. `ClusterIP` tells Flink to make the internal Service and
stop there. A separate hand-written Service pins 30011.

```
                        ┌── personalization        (headless, Flink made it)
                        │      used by: TaskManagers, for RPC
                        │
JobManager pod :8081 ◀──┼── personalization-rest   (ClusterIP, Flink made it)
                        │      used by: Web UI, health checks
                        │
                        └── the NodePort Service   (nodePort 30011, hand written)
                               used by: a browser, via localhost:30011
```

Both the hand-written Service and Flink's REST Service select
`app: personalization, component: jobmanager` and forward to the same pod. They
do not conflict, because a selector is a filter and not a claim of ownership.

**Do not name the hand-written Service `personalization-rest`.** Flink owns that
name, two objects cannot share a name in one namespace, and the apply would
either fail or fight reconciliation forever. Follow the shape
`manifests/minio/s3-nodeport.yaml` already uses: that Service is called
`minio-s3-api`, not `minio`.

## The two doors into MinIO, and where `s3.endpoint` comes from

### The Service nobody in this repo wrote

`manifests/minio/s3-nodeport.yaml` defines `minio-s3-api`, a NodePort on 30014.
That Service exists so the **host** can reach MinIO. A TaskManager pod cannot use
it, because `localhost` inside a pod is that pod and not the workstation.

The in-cluster address is a different Service, and it is not in this repo. **The
MinIO Operator creates it from the `Tenant` custom resource.** A `Tenant` is a
request, and the Operator turns it into ordinary Kubernetes objects. One is a
Service named exactly `minio`, in the tenant's namespace.

Observed on 2026-09-06, `kubectl get svc -n minio-tenant`:

```
NAME                      TYPE        CLUSTER-IP      PORT(S)          AGE
minio                     ClusterIP   10.96.241.160   80/TCP           21d
minio-s3-api              NodePort    10.96.142.170   9000:30014/TCP   13d
personalization-console   ClusterIP   10.96.64.25     9090/TCP         21d
personalization-hl        ClusterIP   None            9000/TCP         21d
```

Four Services. Exactly one, `minio-s3-api`, is hand written. The other three came
from the `Tenant`: the S3 API on 80, the console on 9090, and the headless
per-pod Service on 9000.

**Why port 80 and not 443.** `manifests/minio/tenant.yaml` sets
`requestAutoCert: false`. With TLS off the Service serves plain HTTP on 80. The
Operator's own `tenant create` output shows 443 for a TLS tenant, and its Ingress
example routes to `service: name: minio, port: number: 80` for the no-TLS shape.

### Reading the name

```
minio  .  minio-tenant  .  svc  .  cluster.local
  │            │            │           │
  │            │            │           └─ the cluster's DNS suffix
  │            │            └───────────── this is a Service, not a pod
  │            └────────────────────────── the namespace
  └─────────────────────────────────────── the Service name, made by the MinIO Operator
```

`http://` with no port written means port 80, so
`http://minio.minio-tenant.svc.cluster.local` resolves to that Service on 80,
which forwards to the MinIO pod on `targetPort: 9000`.

### The two doors, side by side

| Service | Written by | Used by | Address |
|---|---|---|---|
| `minio-s3-api` | this repo, `s3-nodeport.yaml` | the host: `kcat`, the Phase 3 signed requests | `localhost:30014` |
| `minio` | the MinIO Operator, from the Tenant CR | the JobManager and the three TaskManagers | `minio.minio-tenant.svc.cluster.local:80` |

Neither visits the other. Both end at the same pod on port 9000. This is the
in-cluster counterpart to the table at the end of
[phase-3-core-pipeline.md](phase-3-core-pipeline.md), which recorded
`http://localhost:30014` as the Phase 3 value and named the in-cluster Service
as the eventual replacement.

## Placing pods across Zones: three families of rule, and why this phase picked one

### The misconception to clear first

`topologySpreadConstraints` does not choose nodes. Neither does pod affinity.
The scheduler chooses the node. These fields only say which choices are legal
and which are preferred.

More precisely, a spread constraint does not care about nodes at all. It cares
about a **count per domain**. Whether that count lands on `worker` or `worker2`
inside `zone-a` is not its concern.

### Three families, and they answer three different questions

| Family | Looks at | The question it answers |
|---|---|---|
| `nodeSelector`, `nodeAffinity` | **node labels** | "which nodes am I allowed on?" |
| `podAffinity`, `podAntiAffinity` | **where other pods already are** | "must I be near, or away from, those pods?" |
| `topologySpreadConstraints` | **where other pods already are, counted per domain** | "how evenly is my group distributed?" |

`topologyKey` appears in the second and third families and nowhere in the first.
Both of those group nodes into domains before counting pods. That is the whole
of what `topologyKey` does:

```
topologyKey: topology.kubernetes.io/zone

  ┌── zone-a ──┐  ┌── zone-b ──┐  ┌── zone-c ──┐
  │  worker    │  │  worker2   │  │  worker3   │      3 domains
  └────────────┘  └────────────┘  └────────────┘

topologyKey: kubernetes.io/hostname

  ┌─ worker ─┐  ┌─ worker2 ─┐  ┌─ worker3 ─┐          one node per domain
  └──────────┘  └───────────┘  └───────────┘
```

"Choosing nodes in a topology domain" describes `nodeAffinity`, not either of
the other two.

### A relation versus a distribution

`podAntiAffinity` states a **relation between pairs**: this pod and that pod
must not share a domain.

`topologySpreadConstraints` states a property of **the whole population**: the
counts across domains must not differ by more than N.

The simplest possible version, two functions deciding whether a new pod may
enter a Zone:

```python
pods_per_zone = {"zone-a": 0, "zone-b": 0, "zone-c": 0}

def anti_affinity_ok(zone):                    # required anti-affinity
    return pods_per_zone[zone] == 0            # only ever asks: is it EMPTY?

def spread_ok(zone, max_skew=1):               # topology spread
    after = dict(pods_per_zone)
    after[zone] += 1
    return max(after.values()) - min(after.values()) <= max_skew
```

`anti_affinity_ok` can only count to one. `spread_ok` compares the whole
distribution. Six pods through both:

```
anti_affinity_ok                        spread_ok, max_skew = 1
─────────────────                       ───────────────────────
pod1 -> zone-a   (0 == 0, ok)           pod1 -> zone-a   1/0/0  skew 1  ok
pod2 -> zone-b   (0 == 0, ok)           pod2 -> zone-b   1/1/0  skew 1  ok
pod3 -> zone-c   (0 == 0, ok)           pod3 -> zone-c   1/1/1  skew 0  ok
pod4 -> ???      every zone has 1       pod4 -> zone-a   2/1/1  skew 1  ok
        PENDING, forever                pod5 -> zone-b   2/2/1  skew 1  ok
pod5 -> PENDING                         pod6 -> zone-c   2/2/2  skew 0  ok
pod6 -> PENDING
```

**Required anti-affinity has a hard ceiling equal to the number of domains.**
Three Zones means at most three pods. That is not a tuning problem, it is what
the rule means.

### Why "at most one per Zone" appears to work with either

It works with both **only because of the current pod counts.** It is a
coincidence, not an equivalence.

Anti-affinity says it directly, at any pod count: no two matching pods share a
Zone.

`maxSkew: 1` says something else that happens to coincide. It constrains
`max(counts) - min(counts) <= 1`, so for P pods over D eligible domains the
busiest domain holds `ceil(P / D)`. With D = 3:

| Pods | `ceil(P/3)` | Legal distribution | "At most one per Zone"? |
|---|---|---|---|
| 2 JobManagers | 1 | 1/1/0 | **yes** |
| 3 TaskManagers | 1 | 1/1/1 | **yes** |
| 4 TaskManagers | 2 | 2/1/1 | **no** |
| 6 TaskManagers | 2 | 2/2/2 | **no** |

Check the JobManager row by hand. Both in `zone-a` gives counts 2/0/0, a skew of
2, which exceeds 1 and is rejected. The only legal placements put one each in
two different Zones.

**The rule: `maxSkew: 1` collapses to "at most one per domain" exactly while
`P <= D`.** Above that it becomes "as evenly as possible", a different statement
that merely agreed with the first one for small P.

`maxSkew` is measured against the **global minimum**, the smallest count across
eligible domains. The API reference gives the canonical example: at 2/2/1 the
global minimum is 1, so with `maxSkew: 1` an incoming pod may only go to zone3,
making it 2/2/2. Sending it to zone1 would make the skew 3 minus 1, violating 1.

### Why this phase chose spread

Today the two tools are indistinguishable here: 2 JobManagers and 3 TaskManagers
against 3 Zones, and `P <= D` in both cases.

Phase 6 adds the Flink Job Autoscaler and pod counts stop being fixed. The
moment the autoscaler asks for a fourth TaskManager, required anti-affinity
leaves it `Pending` and the autoscaler's decision silently does nothing. Every
Drill in this phase measures recovery, so a placement rule that produces
permanently unschedulable pods above three is the wrong foundation for four
Drills.

A second, smaller reason, from the Kubernetes docs:

> Inter-pod affinity and anti-affinity require substantial amounts of processing
> which can slow down scheduling in large clusters significantly. We do not
> recommend using them in clusters larger than several hundred nodes.

Six nodes here, so this costs nothing today. The two tools are still not equal
in price.

For the JobManagers it genuinely does not matter, since `replicas: 2` is never
autoscaled. Using one mechanism for both roles is worth more than picking the
marginally tighter one for each.

### Spread constraints do not replace affinity

They cannot express two things at all:

| What you want to say | Only tool that can |
|---|---|
| "only on nodes with an SSD" | `nodeAffinity` |
| "co-locate me **with** the cache pods" | `podAffinity`. Spread has no attract mode |
| "keep me **away from** pods labelled `app: batch`" | `podAntiAffinity`. Spread balances its own group, it does not avoid a different one |
| "at most one of us per Zone" | either, while `P <= D` |
| "distribute us **evenly** over Zones" | `topologySpreadConstraints` only |

The clean summary: **affinity expresses attraction and repulsion between groups.
Spread expresses balance within one group.** Both Phase 5 constraints are pure
balance statements, so spread is the matching tool.

### Two optional fields, and a default worth knowing

Every constraint accepts these, from the API reference:

| Field | Default | What the default means |
|---|---|---|
| `nodeAffinityPolicy` | `Honor` | only nodes matching the pod's own nodeAffinity or nodeSelector are counted |
| `nodeTaintsPolicy` | `Ignore` | **node taints are ignored. All nodes are counted** |

That second default is a live trap in many clusters: a tainted node the pod can
never run on is still counted as a domain and can drag the skew calculation off.

### Why it does not bite this cluster

Observed 2026-09-06:

```
NAME                                 ZONE     TAINTS
personalization-lab-control-plane    <none>   node-role.kubernetes.io/control-plane
personalization-lab-control-plane2   <none>   node-role.kubernetes.io/control-plane
personalization-lab-control-plane3   <none>   node-role.kubernetes.io/control-plane
personalization-lab-worker           zone-a   <none>
personalization-lab-worker2          zone-b   <none>
personalization-lab-worker3          zone-c   <none>
```

Two independent mechanisms protect placement:

1. **The taint.** `node-role.kubernetes.io/control-plane` is `NoSchedule` and no
   Flink pod tolerates it. This is the one actually keeping pods off.
2. **No Zone label.** A node without `topology.kubernetes.io/zone` belongs to no
   domain under that `topologyKey`, so it cannot enter the skew calculation.

Mechanism 2 matters precisely because `nodeTaintsPolicy` defaults to `Ignore`.
Without the missing label, three tainted control planes would be counted as
domains. Here they drop out on the label rather than the taint, so the
arithmetic stays clean: three eligible domains, three workers.

The Kubernetes docs flag this exact hazard for the other family: "Pod
anti-affinity requires nodes to be consistently labeled with the specified
`topologyKey`; missing labels can lead to unintended behavior." This cluster is
inconsistently labelled by design, which is a further reason spread is the
safer of the two here.

**Three eligible domains is what makes `parallelism: 6` with
`numberOfTaskSlots: 2` land as one TaskManager per Zone.** That is the geometry
Drill C reads.

### Why `topology.kubernetes.io/zone` and not `zone`

`topologyKey` is a **node label key**, matched literally, so it must be exactly
what the label on the node is called. `clusters/kind/kind-cluster.yaml:28` sets
it:

```yaml
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "topology.kubernetes.io/zone=zone-a"
```

A label key has two parts, and the prefix exists for the same reason an API
group does:

```
topology.kubernetes.io  /  zone
         │                  │
         │                  └── the NAME, up to 63 chars
         └───────────────────── the PREFIX, an optional DNS subdomain
```

`topology.kubernetes.io` is owned by the Kubernetes project, so nobody else
defines a conflicting `zone` under it. A bare `zone` lives in the unprefixed
space where any chart or engineer can also write `zone` and mean something else.

A bare `zone` **would work**. `topologyKey` accepts any node label key and the
scheduler has no special knowledge of the well-known one. The docs recommend
against it:

> Topology spread constraints utilize node labels to identify topology domains.
> It is recommended to use well-known label keys like `topology.kubernetes.io/zone`
> and `topology.kubernetes.io/region` over private label keys.

Three reasons that matter here:

| Reason | Consequence for this lab |
|---|---|
| Cloud providers set it automatically | on EKS or GKE the AZ arrives already labelled, so the same manifest works unchanged off `kind` |
| Other subsystems already read it | CSI storage topology, the Downward API (`fieldPath: metadata.labels['topology.kubernetes.io/zone']`), and kube-scheduler's own defaults |
| kube-scheduler's built-in defaults name it | `maxSkew: 3` on `kubernetes.io/hostname` and `maxSkew: 5` on `topology.kubernetes.io/zone` |

For a project whose ubiquitous language calls this a **Zone**, the standard key
means the manifest reads the same on `kind` as it would in production.

## Two ways a pod goes away, and why only one can be refused

### The distinction

Kubernetes has two ways a pod can go away, and they are not the same event:

| Kind | Examples | Can it be refused? |
|---|---|---|
| **Involuntary** | `kubectl delete pod`, node crash, OOMKill, preemption | **no** |
| **Voluntary** | `kubectl drain`, node upgrade, cluster autoscaler scale-down | **yes** |

A `PodDisruptionBudget` only governs the second kind. It is a rule the
**eviction API** consults. `kubectl drain` goes through that API.
`kubectl delete pod` does not.

### Why that distinction shapes all four Drills

The Drills split exactly along that line:

```
Drill A   kubectl delete pod  (a TaskManager)   INVOLUNTARY  -> PDB does nothing
Drill B   kubectl delete pod  (leader JM)       INVOLUNTARY  -> PDB does nothing
Drill C   kubectl drain       (a whole Zone)    VOLUNTARY    -> PDB refuses
Drill D   patch job.state     (no pod involved) n/a
```

`manifests/flink/blue/pdb.yaml` carries a comment saying so. Without it, Drills A
and B read as the PDB failing to protect anything, when in fact it was never
asked.

### What `minAvailable: 1` does in Drill C

```
2 JobManagers, one in zone-a, one in zone-b.

kubectl drain the zone-b worker
  -> eviction API asks the PDB: after this, how many remain?
  -> 1.  minAvailable is 1.  1 >= 1.  ALLOWED.

drain the zone-a worker too, before the first came back
  -> after this, 0 remain.  0 < 1.  REFUSED.
     drain blocks and prints why.
```

Drill C Step 3 is titled "Drain it, and watch the PDB refuse". This file is what
produces that refusal.

The healthy reading once pods exist:

```bash
kubectl get pdb -n personalization-blue
```

`ALLOWED DISRUPTIONS: 1` with 2 JobManagers running. `0` means the budget is
already at its floor and any drain will be refused.

## `selector` versus `labelSelector`

### They are two different data types that share a field name

The same intent, written in two objects:

```yaml
# Service                     # PodDisruptionBudget
spec:                         spec:
  selector:                     selector:
    app: personalization          matchLabels:
    component: jobmanager           app: personalization
                                    component: jobmanager
```

Same information. One extra level of nesting on the right.

### In Python, so the shapes are unmistakable

```python
# what a Service's selector IS:
selector = {"app": "personalization", "component": "jobmanager"}

# what a PDB's selector IS:
selector = {"matchLabels": {"app": "personalization",
                            "component": "jobmanager"}}
```

The Service's is **a dictionary of labels**.

The PDB's is **a dictionary that contains a dictionary of labels**, under the key
`matchLabels`.

### Why the extra box exists

Because the box has room for a second thing:

```python
selector = {
    "matchLabels":      {"component": "jobmanager"},
    "matchExpressions": [{"key": "tier", "operator": "In", "values": ["cache"]}],
}
```

`matchExpressions` supports `In`, `NotIn`, `Exists`, `DoesNotExist`. A bare
dictionary of labels can only say "this equals that". The box is what makes room
for more. The two fields are combined with AND.

### Why a Service does not have the box

**`Service` is older than the box.** It is a core/v1 object from the earliest
Kubernetes, and its `selector` was a plain dictionary because nothing better
existed yet. Adding the box now would break every Service manifest ever written.

Everything invented afterwards uses the box:

| No box, plain dictionary | Has the box |
|---|---|
| `Service.spec.selector` | `Deployment.spec.selector` |
| `Pod.spec.nodeSelector` | `PodDisruptionBudget.spec.selector` |
| `ReplicationController.spec.selector` | `NetworkPolicy.spec.podSelector` |
| | `topologySpreadConstraints[].labelSelector` |
| | `podAffinity` / `podAntiAffinity` `labelSelector` |

Both shapes appear in `manifests/flink/blue/`: bare labels in
`rest-nodeport.yaml`, `matchLabels` in the two spread constraints and the PDB.

### Getting it wrong fails loudly, and the errors name the types

Tested against this cluster on 2026-09-06 with `--dry-run=server`.

```
# PDB written the Service way (bare labels)
unknown field "spec.selector.app", unknown field "spec.selector.component"
```

The box has a fixed list of allowed keys, and `app` is not one of them. Only
`matchLabels` and `matchExpressions` are.

```
# Service written the PDB way (with matchLabels)
cannot unmarshal object into Go struct field
ServiceSpec.spec.selector of type string
```

A plain dictionary of labels requires every value to be a **string**.
`matchLabels` has a dictionary as its value, and a dictionary is not a string.

This is the rare case where a mistake is caught early and clearly. Contrast it
with the misspelled Helm value in Task 4, which no layer caught at all.

### One footgun worth memorising

Leaving the box empty does not mean "select nothing". The API reference:

> An empty label selector matches all objects. A null label selector matches no
> objects.

```yaml
selector: {}     # on a PDB: matches EVERY pod in the namespace
```

On this project's PDB, `selector: {}` would guard all five Flink pods as one
group, and `minAvailable: 1` across five pods is a far weaker promise than
across the two JobManagers.

For a `Service`, the parallel case is different again: **no** `selector` at all
means no automatic EndpointSlices, which is the documented way to point a
Service at an externally managed address.

## `upgradeMode` decides the way down. The way up follows.

### The thing that confuses

The field is called `upgradeMode`. An "upgrade" sounds like one event: the job
goes down, then comes back up. So the field looks like it should control both
halves.

It controls only the way down. The way up is then forced, by what the way down
left behind.

### The simplest possible version

A program that can shut down two ways:

```python
def shutdown(mode):
    if mode == "snapshot":
        write_file("/backups/snapshot.bin")    # writes a file
    elif mode == "autosave_only":
        pass                                   # writes nothing new


def startup(mode):
    if mode == "snapshot":
        load("/backups/snapshot.bin")          # reads THAT file
    elif mode == "autosave_only":
        load(find_latest_autosave())           # reads something else
```

Run this sequence:

```python
shutdown("snapshot")        # /backups/snapshot.bin now exists

# time passes, you change your mind

startup("autosave_only")    # goes looking for an autosave
                            # there isn't a fresh one
                            # CRASH
```

The startup mode had to match the shutdown mode that **actually ran**, not the
one preferred now. The file on disk was decided minutes ago and cannot be
re-decided.

So the only safe version remembers:

```python
last_shutdown_mode = None

def shutdown(mode):
    global last_shutdown_mode
    ...
    last_shutdown_mode = mode        # REMEMBER IT

def startup(requested_mode):
    mode = last_shutdown_mode        # IGNORE the request, use the memory
    ...
```

That last line is the surprising part. `startup` throws away what was asked for
and uses what it remembers.

### The same thing in Flink

| Python | Flink |
|---|---|
| `shutdown(mode)` | the operator suspending the job |
| `/backups/snapshot.bin` | a savepoint in `s3://checkpoints/phase-5-savepoints` |
| `find_latest_autosave()` | the last checkpoint, located through HA metadata |
| `last_shutdown_mode` | `lastReconciledSpec.job.upgradeMode` |
| `startup(requested_mode)` | the operator resuming the job |

What each mode writes on the way down:

| `upgradeMode` | Written on the way down | Read on the way up |
|---|---|---|
| `savepoint` | a savepoint | that savepoint |
| `last-state` | nothing new. The last checkpoint is all there is | that checkpoint, via HA metadata |
| `stateless` | nothing | nothing |

**One choice, made on the way down. The way up follows automatically.**

### The timeline, with this project's file

```
t0   file says:  upgradeMode: savepoint,  state: running
     job RUNNING
     operator remembers: savepoint

t1   Drill D patches state: suspended
     operator reads its memory  ->  savepoint
     -> writes a savepoint to phase-5-savepoints
     -> cancels the job
     operator still remembers: savepoint

t2   YOU EDIT THE FILE:  upgradeMode: last-state,  state: running

t3   operator resumes.
     Your file says last-state.
     Operator uses its memory instead  ->  savepoint
     -> restores from the savepoint written at t1
```

The edit at `t2` was ignored **for this resume**. It takes effect at the next
shutdown.

**And that is correct.** At `t1` a savepoint was written. Honouring `last-state`
at `t3` would send the operator hunting for HA metadata to find a checkpoint,
when the thing that actually exists is a savepoint. Same crash as the Python
example.

### The operator source, and the one exception

```java
if (currentDeploySpec.getJob().getUpgradeMode() != UpgradeMode.STATELESS) {
    currentDeploySpec.getJob()
        .setUpgradeMode(lastReconciledSpec.getJob().getUpgradeMode());
}
```

Read it as: overwrite what was written with what I remember, **unless**
`stateless` was written.

`stateless` is honoured because it needs no artifact. "Start with empty state"
works no matter what is or is not on disk, so there is nothing to disagree
about.

This is **not** the same as the docs' statement that the operator "strictly
prohibits switching to stateless mode to avoid state loss". That sentence is
about the operator never *choosing* stateless on its own for an unhealthy job.
Written explicitly, it is obeyed.

### Three events, and only one reaches the operator

`upgradeMode` is a field on the `FlinkDeployment`, so it is easy to read it as
governing every way the job can go down. It does not. It governs one of the
three, and the other two never involve the operator at all.

| Event | Who reacts | Restores from | `upgradeMode` involved? |
|---|---|---|---|
| TaskManager dies | Flink's restart strategy, inside the running job | last **checkpoint** | No |
| Leader JobManager dies | Kubernetes HA, standby takes leadership | HA metadata + last **checkpoint** | No |
| You edit the spec | the **operator**, via reconciliation | per `upgradeMode` | **Yes, only here** |

Failure recovery is Flink's own job, and the operator never sees it. That is
exactly what Drills A and B (Tasks 7 and 8) are built to observe.

The operator reads the field in one place, `AbstractJobReconciler.getJobUpgrade()`,
and the reconcile loop reaches that method only when the spec has changed:

```java
boolean specChanged =
        DiffType.IGNORE != diffType || reconciliationState == ReconciliationState.UPGRADING;
```

Two consequences follow, and both matter later.

**A spec edit on a running job is the trigger, not a failure.** Changing
`spec.flinkConfiguration` or `spec.job.args` is enough. With `upgradeMode:
savepoint` the operator then cancels with a savepoint and restores from it. Any
edit that changes the job graph or the key group count makes that restore fail,
and the error names state mapping, which reads like a Flink fault rather than a
decision someone made.

**Changing `upgradeMode` on its own triggers nothing.** The field is annotated as
an `IGNORE`-level diff, so on its own `diffType` is `IGNORE`, `specChanged` is
`false`, and no reconciliation runs. It takes effect only when it travels with a
change that is not ignored.

### Three prefixes, three recovery paths

| Event | Restores from | Prefix |
|---|---|---|
| Drill A, TaskManager killed | the last **checkpoint** | `phase-5` |
| Drill B, leader JobManager killed | the last **checkpoint**, found via HA metadata | `phase-5` + `phase-5-ha` |
| Drill D, suspend then resume | the **savepoint** | `phase-5-savepoints` |

That is why Task 5 Step 12 checks `phase-5-ha` separately from `phase-5`. Drill B
depends only on the HA prefix, and an empty one would let Drill B quietly prove
nothing.

## Why one job runs across all three TaskManagers

### The model to replace

Three TaskManagers are not three copies of the job. Flink splits the **data**,
not the program.

```
Web servers      3 copies of the PROGRAM,  each handling different REQUESTS
Flink            1 program,  split into slices, each handling different KEYS
```

### Each slot already runs the whole job

From the Flink docs on scheduling:

> Each TaskManager will have one or more task slots, each of which can run **one
> pipeline of parallel tasks**. A pipeline consists of multiple successive tasks,
> such as the *n-th* parallel instance of a MapFunction together with the *n-th*
> parallel instance of a ReduceFunction.

A slot is not "one operator". A slot holds one vertical slice of the **entire**
pipeline.

```
parallelism: 6,  numberOfTaskSlots: 2   ->  6 slots,  3 TaskManagers

TM1              TM2              TM3
┌────┬────┐      ┌────┬────┐      ┌────┬────┐
│ s0 │ s1 │      │ s2 │ s3 │      │ s4 │ s5 │
└────┴────┘      └────┴────┘      └────┴────┘

Each of s0..s5 runs ALL FIVE operators.
What differs is WHICH SHOPPERS each one owns.
```

So "why not one job per TaskManager" has a direct answer: **each slot already
runs the whole job**, six times over. They differ only in which slice of the data
they see.

### Why the data must be split and not duplicated

Shopper 4711 produces three Clicks. The job counts Clicks per Shopper in a
Browsing Session and emits one Recommendation.

Three genuinely independent jobs, each reading a third of the Kafka partitions:

```
Job A sees clicks 1 and 3   ->  emits "Shopper 4711: 2 clicks"
Job B sees click 2          ->  emits "Shopper 4711: 1 click"
Job C sees nothing          ->  emits nothing

Result: TWO Recommendations for one Shopper. Neither says 3.
```

One job with `keyBy(userId)`:

```
click 1 enters at s4  ┐
click 2 enters at s0  ├─ keyBy(4711) -> hash -> ALWAYS s2
click 3 enters at s5  ┘

s2 counts 3.  ONE Recommendation.  Correct.
```

`keyBy` is a **shuffle**. It routes every record for a given Shopper to the same
slot, whichever slot read it from Kafka. That is the only way a per-Shopper count
is correct when the reading is spread out.

### State is partitioned, not replicated

```
Shopper 4711's window state  ->  s2's RocksDB, on TM1.  Nowhere else.
Shopper 8899's window state  ->  s5's RocksDB, on TM3.  Nowhere else.
```

`ExecutionConfig.getMaxParallelism` ties this to a value worth knowing about: the
maximum parallelism "defines the number of **key groups** used for partitioned
state". Key groups are the buckets `keyBy` hashes into, and they are dealt out to
subtasks.

**This is why the PodDisruptionBudget reasoning came out as it did.** Kill TM1
and Shopper 4711's state is gone from memory. TM2 and TM3 cannot cover for it,
because they never had it. The job must stop and restore every key's state from
the last checkpoint. There is no "two thirds of the job still working".

### Why three TaskManagers and not one with six slots

Six slots in one TaskManager runs the identical job. The Flink docs state the
tradeoff:

> Running more smaller TaskManagers with one slot each is a good starting point
> and leads to the **best isolation** between tasks. Dedicating the same
> resources to fewer larger TaskManagers with more slots can help to increase
> resource utilization, at the cost of **weaker isolation** (more tasks share the
> same JVM).

For this lab, isolation is the point:

| Layout | Blast radius of losing one pod | Zones covered |
|---|---|---|
| 1 TM, 6 slots | 100% of the job | 1 |
| **3 TMs, 2 slots** | **33%** | **3** |
| 6 TMs, 1 slot | 17% | 3, unevenly |

Three is what makes Drill C legible: drain one Zone and exactly one third of the
slots go, visibly. With one TaskManager there is nothing to observe, because
draining its Zone takes everything.

## The Application does not contain the manifests. It contains their address.

### The idea

`manifests/argocd-apps/flink-job-blue.yaml` is twenty lines and mentions none of
the three files it deploys. What it holds is a **three-part coordinate**:

```yaml
source:
  repoURL: https://github.com/sunyanl1236/k8s-personalization   # WHICH repo
  targetRevision: HEAD                                          # WHICH commit
  path: manifests/flink/blue                                    # WHICH directory
```

Everything else is ArgoCD following that pointer. Nothing reads the working
tree. `targetRevision: HEAD` means the repository's **default branch**, not the
locally checked-out one, which is the trap recorded at the end of this section.

### Step 1: `repo-server` turns a directory into a list of objects

```
repo-server:
  git clone <repoURL>
  git checkout <the commit targetRevision resolves to>
  cd <path>
```

Then it asks what kind of directory this is:

| If it finds | It runs |
|---|---|
| `Chart.yaml` | `helm template` |
| `kustomization.yaml` | `kustomize build` |
| **neither** | **plain directory mode** |

`manifests/flink/blue/` holds `flinkdeployment.yaml`, `pdb.yaml`, and
`rest-nodeport.yaml`, with no `Chart.yaml` and no `kustomization.yaml`. So
"rendering" means nothing more than reading every `.yaml` recursively and
parsing each document. Three objects out, byte for byte what was written, no
templating.

This is why `flink-operator.yaml` looks so different: it points at a Helm repo,
so the same step runs `helm template` with its `valuesObject`. One step, three
possible tools, chosen by what is in the directory.

### Step 2: `application-controller` diffs

```
desired = the objects from step 1
live    = what the API server currently holds for those names
diff    = desired - live
```

### Step 3: `application-controller` applies

It calls the Kubernetes API for each differing object, the same operation
`kubectl apply` performs from a different client. Two things happen to the
manifests on the way through.

**A missing namespace is filled in** from `destination.namespace`. An object
that declares its own namespace keeps it. This is why `pdb.yaml` would have
worked without its `namespace:` line under ArgoCD, and why relying on that was
still wrong: a hand `kubectl apply` would have put it in `default`.

**A tracking annotation is stamped.** Observed on this cluster, 2026-09-06:

```
argocd.argoproj.io/tracking-id: minio-tenant:/Service:minio-tenant/minio-s3-api
                                └────┬─────┘ └──┬──┘ └───────┬────────────┘
                                  app name    kind      namespace/name
```

**That annotation is how `prune: true` works.** Delete `pdb.yaml` from Git and
the next sync asks the cluster what carries a `tracking-id` for this app, finds
a PodDisruptionBudget no longer in the desired list, and deletes it. Without the
stamp it could not tell this app's objects from anyone else's.

### Which identity does the applying

```bash
kubectl get statefulset argocd-application-controller -n argocd \
  -o jsonpath='{.spec.template.spec.serviceAccountName}'
```

Read 2026-09-06: **`argocd-application-controller`**, in the `argocd` namespace.
`repo-server` runs as a different ServiceAccount, `argocd-repo-server`, and
never touches the cluster's API at all. It only reads Git.

What that identity is allowed to do:

```bash
kubectl get clusterrole argocd-application-controller \
  -o jsonpath='{range .rules[*]}apiGroups={.apiGroups} resources={.resources} verbs={.verbs}{"\n"}{end}'
```

```
apiGroups=["*"] resources=["*"] verbs=["*"]
```

**Cluster-admin in all but name**, through a `ClusterRoleBinding` of the same
name. That is not an accident of this install. A GitOps controller must be able
to create arbitrary kinds in arbitrary namespaces, because the whole point is
that a new file in Git can introduce a kind nobody anticipated.

The consequence worth stating plainly: **write access to the watched Git
repository is equivalent to cluster-admin on this cluster.** The `project:
default` field is where that would be narrowed in a real deployment, by an
`AppProject` restricting which repos, namespaces, and kinds an Application may
touch. This project leaves it at `default`, which permits everything. See "What
the Application's own fields decide" earlier in this file.

### Step 4: the ordinary controllers take over

ArgoCD's job ends when the object is in etcd:

```
Service              -> endpoints controller builds the EndpointSlice
                     -> kube-proxy on every node writes the forwarding rules
PodDisruptionBudget  -> the disruption controller starts counting healthy pods
FlinkDeployment      -> the Flink operator reconciles it into pods
```

### The whole path

```
laptop
   git push
      │
      ▼
   GitHub                      <- the only copy anything reads
      │
      │  repo-server: clone, checkout, cd into path,
      ▼                        detect the tool, produce a list of objects
   [FlinkDeployment] [Service] [PDB]
      │
      │  application-controller: diff against live, then apply as
      ▼                        argocd-application-controller, filling in
   Kubernetes API server       namespace and stamping tracking-id
      │
      ▼  etcd
   endpoints controller, kube-proxy, disruption controller, Flink operator
```

### Why there are two Application files at all

**One installs the machine. The other gives the machine work to do.**

```
flink-operator     installs the thing that knows HOW to run Flink jobs
flink-job-blue     says WHICH job to run
```

Four reasons to keep them apart.

**They change at different speeds.** The operator is upgraded maybe twice a
year. The job changes every time the code changes. As one Application, every
code push would also be an operator upgrade.

**One machine, many jobs.** Phase 7 adds `flink-job-green`, using the same
operator. If the operator were bundled inside `flink-job-blue`, green would need
a second operator, and two operators watching the same namespace fight each
other.

**Order matters.** The `FlinkDeployment` kind does not exist until the
operator's CRD is installed. Two Applications make that dependency visible. One
Application would try to apply the CRD and the `FlinkDeployment` together and
fail, because the second names a kind the cluster has not learned yet.

**Deleting them means different things.** Delete `flink-job-blue` and the job
stops. Delete `flink-operator` and the CRDs go, which garbage-collects every
`FlinkDeployment` in the cluster. Very different actions, and they should not
share one switch.

This is the pattern every component in this project follows:

```
minio-operator    +  minio-tenant
strimzi           +  strimzi-kafka-cluster
flink-operator    +  flink-job-blue
```

Left side installs a controller. Right side hands it something to control.

### Two Applications, same schema, different everything else

| | `flink-operator.yaml` | `flink-job-blue.yaml` |
|---|---|---|
| Source kind | a **Helm repository** | a **Git directory** |
| `repoURL` | `https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/` | this project's own repo |
| Needs `chart:` | yes, `flink-kubernetes-operator` | no |
| `targetRevision` | `1.15.0`, a pinned chart version | `HEAD`, the default branch |
| Needs `path:` | no | yes, `manifests/flink/blue` |
| Step 1 runs | `helm template` with `valuesObject` | plain YAML parsing |
| What it installs | the **controller**: an operator Deployment, CRDs, webhooks, RBAC | the **custom resources** that controller reconciles |
| `CreateNamespace=true` | yes, it creates `flink-operator` | **no**, Task 3 created the namespace by hand |
| `ServerSideApply=true` | yes, the CRD's `last-applied-configuration` was at 166871 of 262144 bytes | no, three small manifests |
| `ServerSideDiff=true` | yes, the chart omits `priority` on printer columns and the API server defaults it | no, no such field |

**Why `targetRevision` differs is the sharpest line in that table.** A pinned
version is right for a third-party chart, where an unannounced upgrade is a
risk. `HEAD` is right for the repository that is the source of truth, where the
whole point of GitOps is that a push deploys. `root.yaml` records the same
reasoning for itself.

This pairing, one Application for the controller and one for its resources, is
the convention every component in this project follows: `minio-operator` with
`minio-tenant`, `strimzi` with `strimzi-kafka-cluster`, `flink-operator` with
`flink-job-blue`.

### The trap: `HEAD` is not your branch

Observed 2026-09-06, while `flink-job-blue` was failing to appear at all:

```
root.yaml targetRevision:     HEAD
root last synced revision:    86a77e6

origin/master     86a77e6      <- what ArgoCD was reading
origin/phase-2    e018cbd
local HEAD        b0705e0      <- where the work actually was
```

`git status` was clean, so everything **was** committed. It was committed to a
branch nothing watches. `targetRevision: HEAD` resolves to the repository's
default branch, which is `master`. Committing to a feature branch forever
deploys nothing, and produces no error anywhere: the Application simply never
gets created, because `root` never sees the file.

### What Kustomize is, and when this project will want it

`repo-server` picks its tool by looking for `Chart.yaml` or `kustomization.yaml`.
Neither exists in `manifests/flink/blue/`, so plain directory mode wins. Here is
what the missing third option would have done.

**The problem.** You have a folder of YAML that works. Soon you need a second
folder that is almost identical. Phase 7 needs a `green` copy of `blue`. Only
the namespace and a couple of paths change.

Copy the folder, and there are now two files to fix every time you fix a bug.
One will get forgotten.

**What Kustomize does.** You keep one copy. You write a small file that says
"same as that folder, but change these two things."

```
base/          the three files, unchanged
blue/          "use base, but namespace = personalization-blue"
green/         "use base, but namespace = personalization-green"
```

The `green` change file lists only what differs:

```yaml
kind: FlinkDeployment
metadata:
  name: personalization
spec:
  flinkConfiguration:
    execution.checkpointing.dir: s3://checkpoints/phase-7-green
```

Kustomize matches that to the base by `kind` plus `name`, then merges by
structure. Anything not mentioned stays as the base has it.

**Why it is not Helm.** Helm turns the YAML into a template full of `{{ }}`
holes. After that `kubectl apply -f` no longer works on the file, because it is
not real YAML until Helm fills the holes.

Kustomize never edits the source files. The Kustomize glossary states the design
goal directly:

> Kustomize is a command-line tool designed for **template-free, structured
> customization** of declarative configuration for Kubernetes-style objects.

and its overview adds the consequence:

> customizing Kubernetes resource configuration without relying on templates or
> DSLs... leaving the original source files **untouched and usable as-is**.

So `base/flinkdeployment.yaml` stays a normal file that `kubectl apply -f`
accepts.

**One line each:**

```
Helm       text templating.  {{ .Values.x }} -> render -> YAML
Kustomize  structured merge. valid YAML + valid YAML -> merged YAML
```

**When to revisit this.** Plain directory mode is right for Phase 5: one
environment, three files, nothing to deduplicate. The moment Phase 7 creates
`green/` as a near-copy of `blue/`, plain directories mean two files kept in sync
by hand. Kustomize is built into `kubectl` as `kubectl apply -k` and
`kubectl kustomize`, so adopting it adds no tooling to the machine, which matters
under this project's no-permanent-machine-state constraint.

## What port-forward is

**It is a running program that copies bytes between two sockets.** Nothing more.
It is not a setting, not a rule, not a configuration. It is a process.

### Start from the toy

Forget Kubernetes. Two plain programs on your own machine.

**The shop** is a web server on port 8000. It holds one file.

```
$ curl http://localhost:8000/note.txt
hello from the shop
```

**Port 9999 is empty.** Nothing listens there.

```
$ curl http://localhost:9999/note.txt
exit 7, nothing there
```

Now start `relay.py`, 16 lines, no libraries:

```python
listener = socket.create_server(("127.0.0.1", 9999))
while True:
    client, _ = listener.accept()
    shop = socket.create_connection(("127.0.0.1", 8000))
    threading.Thread(target=pipe, args=(client, shop), daemon=True).start()
    threading.Thread(target=pipe, args=(shop, client), daemon=True).start()
```

Read those five lines as English:

1. Listen on port 9999.
2. When someone connects, open a second connection to the shop on 8000.
3. Copy everything the caller says into the shop.
4. Copy everything the shop says back to the caller.

And `pipe` is just a copy loop:

```python
data = a.recv(4096)
b.sendall(data)
```

With that program alive:

```
$ curl http://localhost:9999/note.txt
hello from the shop
```

Port 9999 now "has" the shop. It does not. The shop is still only on 8000. A
program in the middle is carrying the bytes.

Kill the relay, and only the relay:

```
$ curl http://localhost:9999/note.txt     ->  exit 7, gone
$ curl http://localhost:8000/note.txt     ->  hello from the shop
```

The shop never noticed. It was never told about port 9999. It has no idea the
relay existed.

**That relay is exactly what `kubectl port-forward` is.**

### Now map it back

The same shape, observed against this cluster's MinIO Console on 2026-09-06:

| Toy | This cluster |
|---|---|
| the shop on port 8000 | the Console pod, port 9090 |
| `relay.py` on port 9999 | `kubectl port-forward`, PID 1789845 |
| `curl localhost:9999` worked | `curl localhost:9090` gave HTTP 200 |
| kill the relay, 9999 empty | kill kubectl, `localhost:9090` empty |
| the shop kept running | the Console pod kept running |

One difference, and it is the only interesting one.

### The one difference

The toy relay could open its second connection itself, with
`socket.create_connection(("127.0.0.1", 8000))`. The host cannot do that. A
`curl` straight at the Console's ClusterIP, `10.96.64.25:9090`, times out after
4s with `HTTP 000`, because the host's default gateway has never heard of
`10.96.0.0/12`.

So kubectl cannot make the second connection directly. It asks the API server to
make it instead:

```
your host                          |  inside the cluster
                                   |
curl -> :9090                      |
          |                        |
     kubectl process               |
          |                        |
          +--- HTTPS to the API server ---+
                                   |      |
                                   |   kubelet on the pod's node
                                   |      |
                                   |   Console pod :9090
```

The copy loop is split across the boundary. Your half runs in the `kubectl`
process. The other half runs inside the cluster, where `10.96.64.25` is a real
address.

kubectl reaches the API server through `https://127.0.0.1:37855`, the external
load balancer. That connection already works, because the kubeconfig is built on
it. Port-forward rides on a road that is already open.

### Why the port numbers in the command

```bash
kubectl port-forward svc/personalization-console -n minio-tenant 9090:9090
```

- Left `9090`: the port the relay opens **on your host**. Your choice, freely.
- Right `9090`: the port it connects to **on the pod**. Fixed by the pod.

They are equal here only by habit. `8080:9090` would work identically, and you
would open `http://localhost:8080`.

### The two consequences worth remembering

1. **It dies when the process dies.** Ctrl+C, a closed terminal, a restarted
   pod. There is no reconnect, because there is no configuration anywhere that
   remembers it existed.
2. **Its traffic goes through the API server.** That is the control plane, not a
   data path. Fine for a browser session. Wrong for the pipeline's checkpoint
   writes, which is why `minio-s3-api` is a NodePort on 30014 instead.

## HA metadata is two objects, and only one of them is in Kubernetes

### The problem it guards against

Drill B kills the leader JobManager. The standby has to take over and resume the
job. It can only do that if the JobGraph and the checkpoint pointers survived
somewhere outside both JobManager processes.

If that storage is empty, the standby has nothing to recover from. Drill B would
still "pass" visually, because a fresh JobManager starting an empty job also ends
up `RUNNING`. **You would prove nothing.**

That is why Task 5 Step 12 checks the `phase-5-ha` prefix **separately** from
`phase-5`, rather than treating one healthy-looking bucket as evidence for both.

### Why the metadata is not in Kubernetes

A ConfigMap is small. A JobGraph is not. The Flink 2.2.0 Kubernetes HA
documentation states the split directly:

> JobManager metadata is persisted in the file system
> `high-availability.storageDir` and **only a pointer to this state is stored in
> Kubernetes**.

So HA is two objects working together.

| | Holds | Where |
|---|---|---|
| `personalization-cluster-config-map` | the leader lease, and a pointer | Kubernetes |
| `s3://checkpoints/phase-5-ha/` | the JobGraph and checkpoint metadata | MinIO |

Both must be non-empty. **A present ConfigMap with an empty S3 prefix is the
exact failure this step catches.**

### The two checks

**1. The ConfigMap side.**

```bash
kubectl get cm -n personalization-blue
```

Look for `personalization-cluster-config-map`. It also appears in the JobManager
log, in the `KubernetesLeaderElector` lines at startup.

**2. The S3 side, which is the one that matters.** Follow the README recipe:

```bash
kubectl port-forward svc/personalization-console -n minio-tenant 9090:9090
```

Then open `http://localhost:9090`, log in with the `storage-configuration`
credentials, open the `checkpoints` bucket, and browse to `phase-5-ha/`.

Expect a subdirectory named after the cluster-id, so
`phase-5-ha/personalization/`. Flink stores HA artifacts under
`HA_STORAGE_DIR/HA_CLUSTER_ID`, a layout that has been in place since 1.10.

### What a pass looks like

`phase-5-ha/personalization/` contains at least one blob. Not a `chk-N`
directory. Those live under `phase-5/`. If the prefix does not exist at all,
`high-availability.type: kubernetes` did not take effect and Drill B is not yet
meaningful.

**Verified on 2026-09-07.** Both sides carry data.

## Cordon, drain, and why the Drill separates them

### Cordon means "no new arrivals"

```bash
kubectl cordon personalization-lab-worker2
```

That sets `spec.unschedulable: true` on the node and adds the taint
`node.kubernetes.io/unschedulable:NoSchedule`. The scheduler stops placing new
pods there. **Every pod already running stays running.** Nothing moves.

`kubectl get nodes` then shows:

```
personalization-lab-worker2   Ready,SchedulingDisabled
```

`uncordon` reverses it.

### Drain is cordon plus eviction

```
cordon   no new arrivals
drain    no new arrivals  +  everyone currently here leaves
```

`drain` performs the cordon itself, then evicts every pod. So `drain` includes
`cordon`, and running `drain` alone leaves the node cordoned afterwards whether
it finished or not.

### Why Drill C makes you cordon as a separate step

There are two separate claims to test:

1. **Cordon moves nothing.** It only stops future placements.
2. **Eviction is what moves pods.**

Run `drain` alone and both happen in one burst of output. Pods move, but nothing
in what you observed says which half of the command moved them.

Run them separately and each claim gets its own verification:

```bash
kubectl cordon personalization-lab-worker2
kubectl get nodes                                    # SchedulingDisabled
kubectl get pods -n personalization-blue -o wide     # IDENTICAL. Claim 1 proven.

kubectl drain personalization-lab-worker2 --ignore-daemonsets --delete-emptydir-data
kubectl get pods -n personalization-blue -o wide     # now they have moved. Claim 2 proven.
```

Every movement in the second half is attributable to the eviction, because the
first half already showed the cordon moving nothing.

This is the plan's own standard applied to a two-part command:

> A task is done when its verification command produces real output you have read.

### The practical reason, which matters more in a hurry

If a drain hangs on a PodDisruptionBudget refusal and you interrupt it, **the
node stays cordoned**. Having typed the cordon yourself makes the cleanup
obligation obvious: there is an `uncordon` owed.

Run `drain` alone, interrupt it, and the cordon is a side effect you never typed.
That is how a node ends up `SchedulingDisabled` for three days, with Phase 6's
autoscaling behaving strangely and the cause well behind you.

Check before walking away:

```bash
kubectl get nodes        # no SchedulingDisabled
```

## What the TaskManagers do during a JobManager failover

**The pods survive. The work stops.** Those are two different things, and Drill B
showed both.

### The pods survive

Observed after the 19:05 leader kill on 2026-09-07:

```
NAME                              RESTARTS   START
personalization-taskmanager-2-7   0          2026-09-07T18:46:25Z
personalization-taskmanager-2-8   0          2026-09-07T18:52:09Z
personalization-taskmanager-2-9   0          2026-09-07T18:52:09Z
```

All three predate the kill and all have **0 restarts**. No TaskManager was
killed, restarted, or rescheduled.

The new leader re-adopted them rather than requesting new ones:

```
19:05:12,935  ActiveResourceManager - Recovered worker personalization-taskmanager-2-8 ... registered
19:05:12,935  ActiveResourceManager - Recovered worker personalization-taskmanager-2-9 ... registered
```

### The work stops

Inside those same surviving pods, every task was cancelled 5 seconds after the
kill:

```
19:05:05,930  Attempting to cancel task Source: click-stream (2/6)#3
19:05:05,931  Source: click-stream (2/6)#3 switched from RUNNING to CANCELING.
19:05:05,933  Source: click-stream (2/6)#3 switched from CANCELING to CANCELED.
19:05:05,935  Attempting to cancel task Source: click-stream (6/6)#3
...
```

### Why the tasks cannot simply continue

A TaskManager is a worker, not a decision maker. The JobMaster inside the
JobManager is what:

- injects checkpoint barriers into the sources
- coordinates watermarks across subtasks
- decides which subtask sends to which

With no JobMaster, no checkpoint can complete. A pipeline that keeps consuming
Kafka while unable to checkpoint is building state it can never recover from, so
continuing would be worse than stopping.

### The sequence

```
19:05:05   leader killed
19:05:05   TaskManager POD alive, its tasks CANCELED        <- processing stops
19:05:12   standby acquires the lease (leaseDuration PT15S)
19:05:12   new leader re-adopts the same TaskManager pods
19:05:15   tasks redeployed from chk-920                    <- processing resumes
```

About **15 seconds of no processing, and zero TaskManager restarts.**

### Then what did HA actually buy?

The job restarts either way, so the value is not "no restart".

**Without HA the job would be gone.** No JobGraph, no record of the last
completed checkpoint, nothing to resume from. Someone would have to resubmit by
hand and pick a checkpoint path themselves.

With HA, a standby that was already running acquired the lease, read the pointer
from `s3://checkpoints/phase-5-ha`, and resumed the **same job id** in 15 seconds
with no human involved. That is the difference, and it is why Task 5 Step 12
checks the `phase-5-ha` prefix separately from `phase-5`.

### The contrast with Drill A, in one table

| | Drill A, TaskManager killed | Drill B, leader JobManager killed |
|---|---|---|
| TaskManager pods | rebuilt, new pods scheduled | **survive, 0 restarts, re-adopted** |
| Tasks | cancelled and redeployed | cancelled and redeployed |
| Job id | unchanged | unchanged |
| Restored from | a checkpoint under `phase-5` | a checkpoint under `phase-5`, located via `phase-5-ha` |
| Signature log line | `Restoring job ... from Checkpoint N` | `Job ... was recovered successfully` |
| Recovery | 15 seconds | 15 seconds |

Both restore from a checkpoint. Only Drill B goes through the HA store to find
out **which** checkpoint.

## Why `opt/` does not work, and why the folder name under `plugins/` is arbitrary

### `opt/` is on no list

The usual explanation is "Flink does not load from `opt/`", stated as a rule. It
is not a rule. It is an absence.

`bin/config.sh` builds the classpath from exactly one directory:

```bash
constructFlinkClassPath() {
    ...
    done < <(find "$FLINK_LIB_DIR" ! -type d -name '*.jar' -print0 | sort -z)
```

`$FLINK_LIB_DIR` is `/opt/flink/lib`. That `find` never looks anywhere else.

Separately, the plugin manager scans `/opt/flink/plugins`, one subdirectory per
plugin.

```
/opt/flink/lib/         scanned by constructFlinkClassPath
/opt/flink/plugins/*/   scanned by the plugin manager
/opt/flink/opt/         scanned by NOTHING
```

`opt/` is a shipping crate. The distribution puts optional jars there so they
exist in the image without being active. Nothing loads from it, so a jar left
there produces no error and no effect. That is why
[ADR 0001](../adr/0001-plugin-directory-move.md) moves the file rather than
configuring a path.

### The folder name is arbitrary because it becomes the plugin id

`apps/pipeline/Dockerfile` writes to `/opt/flink/plugins/s3-fs-hadoop/`. The name
`s3-fs-hadoop` is not matched against anything. The plugin manager takes each
subdirectory name as that plugin's **id**, which is why the JobManager log reads:

```
Plugin loader with ID not found, creating it: s3-fs-hadoop
```

Any directory name would work. What is **not** optional is that the jar sits in a
subdirectory of its own rather than directly in `plugins/`. Each subdirectory
gets its own classloader, and that isolation is the entire point here: the
`flink-s3-fs-hadoop` shaded jar carries `com.amazonaws.*` classes that are **not**
relocated. Loading them on a shared classpath is how they collide with anything
else pulling the AWS SDK.

### `ENABLE_BUILT_IN_PLUGINS` exists and was rejected anyway

The 2.2.0 entrypoint does support it:

```bash
# /docker-entrypoint.sh
35:  if [ -z "$ENABLE_BUILT_IN_PLUGINS" ]; then
40:  for target_plugin in $(echo "$ENABLE_BUILT_IN_PLUGINS" | tr ';' ' '); do
```

It moves a named jar out of `opt/` into `plugins/` at container start. So it
would replace the Dockerfile's `RUN mkdir && cp` for the S3 filesystem.

It was still rejected, and the reason is worth recording so nobody re-evaluates
it: **it handles one of the two files this image needs.** The job jar has to be
`COPY`ed in regardless. Using the environment variable would mean maintaining two
different mechanisms for two files that arrive at the same time, in exchange for
removing two lines. One `RUN` and one `COPY`, side by side in the same file, is
easier to read than a Dockerfile plus an environment variable set somewhere else.

## The Shadow jar allowlist, and why an exclusion list fails silently

### The problem

A fat jar for a Flink job must contain your code and the few dependencies the
runtime does not already ship. It must **not** contain Flink itself. `flink-dist`
is already in `/opt/flink/lib`, and shipping a second copy inside the job jar
gives two versions of every Flink class, resolved by classloader order rather
than by intent.

There are two ways to arrange that.

### Exclusion list: name what to leave out

```
everything on the compile classpath, MINUS the things I listed
```

Add a dependency later and it is bundled by default. Nobody is told. The jar
grows, a duplicate class ships, and the symptom arrives weeks later as a
`NoSuchMethodError` or a subtly wrong classloader resolution.

**The failure is silent and delayed**, and the thing that caused it, adding a
dependency, looked completely routine.

### Allowlist: name what to put in

`apps/pipeline/build.gradle` declares its own configuration and Shadow reads only
that:

```groovy
    dependencyScope('bundled')
    resolvable('bundledClasspath') { extendsFrom configurations.bundled }
    implementation.extendsFrom configurations.bundled
    ...
    configurations = [project.configurations.bundledClasspath]
```

Exactly two entries go in it:

```groovy
    bundled project(':domain')
    bundled "org.apache.flink:flink-connector-kafka:${kafkaConnectorVersion}"
```

Everything else is `compileOnly` plus `runtimeOnly`, which compiles against a
library without shipping it.

Add a dependency later and it is **not** bundled. The build succeeds, and the job
fails at class load with a `NoClassDefFoundError` naming the exact missing class.

**The failure is loud and immediate**, and it names its own fix.

### The trade, stated plainly

Both lists require maintenance. The difference is what happens when you forget.

| | Forgetting costs you |
|---|---|
| exclusion list | a silent duplicate, surfacing later as a runtime error that does not mention jars |
| **allowlist** | a `NoClassDefFoundError` on the next deploy, naming the class |

The 24 mb jar size is the visible result. A jar bundling Flink would be several
times that.

## Drill A, B, and C side by side

Three Drills, three different things destroyed, three different recovery paths.
All three observed on 2026-09-07.

| | **A: TaskManager killed** | **B: leader JobManager killed** | **C: Zone drained** |
|---|---|---|---|
| Command | `kubectl delete pod` | `kubectl delete pod` | `kubectl drain` |
| Disruption kind | involuntary | involuntary | **voluntary** |
| PDB consulted | no | no | **yes** |
| What is destroyed | a slice of RocksDB state | the coordinator | nothing, pods are moved |
| Job status | RUNNING to CREATED to RUNNING | RUNNING to CREATED to RUNNING | **stays RUNNING** |
| Restores from | a checkpoint under `phase-5` | a checkpoint, located via `phase-5-ha` | nothing, no restart |
| TaskManager pods | rebuilt, new pods | **survive, 0 restarts, re-adopted** | one moved |
| JobManager pods | untouched | one replaced, leadership moves | one moved |
| Primary evidence | `latest.restored.id` | **job id unchanged** plus `leaderTransitions` +1 | the eviction refusal, and placement |
| Signature log line | `Restoring job ... from Checkpoint N` | `Job ... was recovered successfully` | `Cannot evict pod ...` |
| Recovery time | 15s, twice | 15s | no job interruption |
| Gap / duplicates | 0 / 0 | 0 / 0 | 0 / 0 |

### Reading the table

**Only Drill C consults a PodDisruptionBudget.** A PDB constrains the eviction
API. `kubectl delete pod` does not use it, so Drills A and B could not have been
refused by any budget.

**Only Drill B goes through the HA store.** Both A and B restore from a
checkpoint under `phase-5`. The difference is how the checkpoint is **found**: A
still has a JobMaster that knows, B has to read the pointer out of `phase-5-ha`.
That is why Task 5 Step 12 checks the two prefixes separately.

**Only Drill C left the job running.** One of six slots moved, and Flink
redeployed that subtask without failing the job. A and B both lost something the
job could not continue without.

**The gap check was 0 in all three, and proves the least.** Committed Kafka
records do not disappear, so `comm -23 BEFORE AFTER` is structurally near-empty
whatever happens. The duplicate check and each Drill's own specific evidence are
what carry the result.
