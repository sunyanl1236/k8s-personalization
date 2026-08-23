# explain kubelet, etcd, kube-apiserver, kube-controller-manager, kube-scheduler HAProxy
The analogy: a warehouse company

Think about a company that ships orders. The company has one office and many warehouses.

┌─────────────────────────┬────────────────────────────┬─────────────────────────────────────────────────────┐
│          Part           │       In the analogy       │                    What it does                     │
├─────────────────────────┼────────────────────────────┼─────────────────────────────────────────────────────┤
│ etcd                    │ The ledger                 │ One book. It holds every fact about the company     │
├─────────────────────────┼────────────────────────────┼─────────────────────────────────────────────────────┤
│ kube-apiserver          │ The front desk clerk       │ The only person who can open the ledger             │
├─────────────────────────┼────────────────────────────┼─────────────────────────────────────────────────────┤
│ kube-scheduler          │ The dispatcher             │ Chooses which warehouse gets each job               │
├─────────────────────────┼────────────────────────────┼─────────────────────────────────────────────────────┤
│ kube-controller-manager │ The supervisors            │ Compare the wanted count to the real count          │
├─────────────────────────┼────────────────────────────┼─────────────────────────────────────────────────────┤
│ kubelet                 │ The foreman in a warehouse │ Does the physical work. Reports what is true        │
├─────────────────────────┼────────────────────────────┼─────────────────────────────────────────────────────┤
│ HAProxy                 │ The switchboard            │ One phone number. Connects to any clerk who answers │
└─────────────────────────┴────────────────────────────┴─────────────────────────────────────────────────────┘

Each part

etcd (the ledger).
etcd is a database. It holds the full state of the cluster. It holds two kinds of facts. The first kind is the wanted state, for example "run 3 TaskManagers". The second kind is the observed state, for example "2 TaskManagers are ready". Nothing else in the cluster keeps a permanent record.

kube-apiserver (the clerk).
The kube-apiserver is the only part that talks to etcd. Every other part talks to the kube-apiserver. No part talks to another part directly. The scheduler does not call the kubelet. Both of them only read from and write to the kube-apiserver. This is why the design is safe. There is one door and one lock.

kube-scheduler (the dispatcher).
The kube-scheduler watches for pods that have no node. For each such pod, it picks a node. It obeys the rules on the pod, such as the resource request, the Zone label, and the taints. Then it writes the choice back to the kube-apiserver. The kube-scheduler does not start the pod. It only writes a name into a field.

kube-controller-manager (the supervisors).
This is one process that holds many small controllers. Each controller runs the same loop:

1. Read the wanted state from the kube-apiserver.
2. Read the observed state from the kube-apiserver.
3. If the two are different, ask the kube-apiserver to make a change.
4. Go back to step 1.

The Flink Kubernetes Operator uses this same loop. It is a custom controller, not part of the kube-controller-manager. A Promotion is that loop at work. You write a new wanted state. The operator suspends the Active Side with a savepoint. Then it restores the Standby Side from that savepoint.

kubelet (the foreman).
The kubelet runs on each worker node. It does three things:

1. It registers the node with the kube-apiserver.
2. It sends a heartbeat, so the node stays Ready.
3. It watches for pods assigned to its node. It then pulls the images and starts the containers.

The kubelet is the only part that touches a real container. It is also the only part that reports the truth about a node.

HAProxy (the switchboard).
You run three control-plane nodes for high availability. Each one has its own kube-apiserver. But the kubelets need one address. HAProxy gives them that address. It sends each connection to a kube-apiserver that is alive. If one control-plane node dies, HAProxy sends traffic to the other two.

How they connect

   kubectl / Flink Operator / kubelets
                 |
                 v
          +--------------+
          |   HAProxy    |     one address, many clerks
          +--------------+
            |     |     |
            v     v     v
     +----------------------+
     |   kube-apiserver x3  |  the only door
     +----------------------+
                 |
                 v
            +---------+
            |  etcd   |       the ledger (raft quorum of 3)
            +---------+

   All of these talk ONLY to the kube-apiserver:

     kube-scheduler ......... "this pod goes on node X"
     kube-controller-manager  "I want 3, I see 2, make 1 more"
     kubelet (node X) ....... "pod is Running", heartbeat, heartbeat
     |   kube-apiserver x3  |  the only door
     +----------------------+
                 |
                 v
            +---------+
            |  etcd   |       the ledger (raft quorum of 3)
            +---------+

   All of these talk ONLY to the kube-apiserver:

     kube-scheduler ......... "this pod goes on node X"
     kube-controller-manager  "I want 3, I see 2, make 1 more"
     kubelet (node X) ....... "pod is Running", heartbeat, heartbeat

One Decoy Workload, step by step

1. You apply the Decoy Workload Deployment. The kube-apiserver writes it to etcd.
2. A controller sees a Deployment with no pods. It creates the pods.
3. The pods have no node. The kube-scheduler sees them.
4. The kube-scheduler finds no node with free capacity. The pods stay Pending.
5. Karpenter sees the Pending pods. It adds a node.
6. The kube-scheduler now assigns the pods to that new node.
7. The kubelet on that node starts the containers.

Step 7 is the step that a kwok node breaks.

Back to kwok

A kwok node has no machine and no kubelet. The node object is in etcd, but no foreman exists in that warehouse.
     kube-scheduler ......... "this pod goes on node X"
     kube-controller-manager  "I want 3, I see 2, make 1 more"
     kubelet (node X) ....... "pod is Running", heartbeat, heartbeat

One Decoy Workload, step by step

1. You apply the Decoy Workload Deployment. The kube-apiserver writes it to etcd.
2. A controller sees a Deployment with no pods. It creates the pods.
3. The pods have no node. The kube-scheduler sees them.
4. The kube-scheduler finds no node with free capacity. The pods stay Pending.
5. Karpenter sees the Pending pods. It adds a node.
6. The kube-scheduler now assigns the pods to that new node.
7. The kubelet on that node starts the containers.

Step 7 is the step that a kwok node breaks.

Back to kwok

A kwok node has no machine and no kubelet. The node object is in etcd, but no foreman exists in that warehouse.

The kwok controller does two jobs that a kubelet would normally do:

1. It sends the heartbeat, so the node stays Ready.
2. It sets each assigned pod to Running.

It does not do the third job. It never starts a container. This is why the Decoy Workload never carries real work. It exists only to make Pending pods, so that Karpenter reacts. Steps 1 to 6 above are all real. Only step 7 is fake.

kwok is load-bearing for the project, but it never runs your application. It runs the decoy pressure that makes real capacity appear, and your real testing (steps 1 through 6 from before) targets the TaskManager pods on the real nodes that pressure produced, never the Decoy Workload itself.

So the causal chain is:

Decoy Workload pods (on kwok nodes, fake, cheap)
        |
        v
Scheduler can't place something ELSE that needs real capacity
        |
        v
Karpenter provisions a real node
        |
        v
Real TaskManager pods land on that real node, run for real

# what is kubelet
kubelet does this work:

1. kubelet adds the node to the cluster.
2. kubelet asks the API server for the pods of this node.
3. kubelet tells containerd to start the containers.
4. kubelet sends the status of the node to the API server.

kubelet does not select a node for a pod. The scheduler selects the node. kubelet then obeys.

#  what is kubectl
kubectl is the client, not a cluster part

Going back to the ledger analogy: kubectl is your phone. It's how you, a person, talk to the front desk clerk (kube-apiserver). It runs on your laptop, not inside the cluster.

   you type a command
        |
        v
   kubectl  (reads ~/.kube/config for which cluster + credentials)
        |
        v
     HAProxy               (if HA control plane, as in your project)
        |
        v
  kube-apiserver
        |
        v
      etcd

# explain hostPort, nodePort, Service port, containerPort one by one

Let me go bottom-up, from the application outward. Each layer wraps the one below it, so that order is the only one that makes sense.

---
1. containerPort

The port the application process listens on, inside the pod.

Who chooses it: the application, or its image. Not you.

ArgoCD's server binds 0.0.0.0:8080. Grafana binds 3000. Prometheus binds 9090. Flink's JobManager REST API binds 8081. Those numbers are baked into the software.

Where you see it:

# in the Pod spec
containers:
  - name: argocd-server
    ports:
      - containerPort: 8080

The surprise: that field does almost nothing. It is documentation.

The application listens on 8080 whether or not you write that line. Deleting it does not close the port. Adding a wrong number does not move it. It exists so that humans and tools can see what the pod uses. Kubernetes does not enforce it.

Scope: the pod's own network namespace. Another pod reaches it at podIP:8080.

---
2. Service port

A stable address for a set of pods that keep dying and changing IP.

The problem it solves: pods are disposable. Every restart gives a new pod IP. You cannot hardcode 10.244.1.7 anywhere.

A Service gives you a fixed virtual IP and a DNS name that never change:

argocd-server.argocd.svc.cluster.local

Two numbers, not one:

kind: Service
spec:
  ports:
    - port: 80          # the port ON the Service. Callers dial this.
      targetPort: 8080  # the port on the POD. Must equal containerPort.

So another pod connects to argocd-server:80, and kube-proxy rewrites that to podIP:8080.

Why they differ: port is a convenience for callers (80 is "web"). targetPort is a fact about the app (8080 is what it binds). The Service translates between the convention and the fact.

Who chooses it: the Helm chart, usually. You rarely touch it.

Scope: inside the cluster only. A type: ClusterIP Service is unreachable from your laptop. This is the default type.

##  I don't understand service port, is it the same for all pods? for example, when user calls argocd-server.argocd.svc.cluster.local:80, it reaches a random pod and map the pod to 8080
That is the entire point of a Service. One address, one port, in front of a set of pods.

                    Service argocd-server
                    ClusterIP 10.96.4.21 : 80
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼              ▼
       Pod 10.244.1.5  Pod 10.244.2.9  Pod 10.244.3.4
            :8080           :8080           :8080

Three pods. One Service port. The caller dials :80 and never knows or cares which pod answered, or that there are three.

Sharpening 1: which pods, exactly

Not arbitrary pods. The Service has a selector that matches pod labels:

kind: Service
metadata:
  name: argocd-server
  namespace: argocd
spec:
  selector:
    app.kubernetes.io/name: argocd-server   # ← finds pod
  ports:
    - port: 80
      targetPort: 8080

Kubernetes continuously watches for pods matching that selector and maintains an EndpointSlice listing their IPs. Two things to know about that list:

- Only Ready pods are in it. A pod failing its readiness probe is removed from the list. Traffic stops going to it. This is how rolling updates
avoid dropping requests.
- It updates automatically. A pod dies, its IP leaves the list. A new pod starts, its IP joins. Nothing you wrote has to change.

Sharpening 2: the ClusterIP is fake

This is the part that surprises most people.

10.96.4.21 is not assigned to any machine. No network interface has it. No process binds it. You cannot find a server holding that address, because there isn't one.

It exists only as an iptables rule. kube-proxy writes rules on every node that say "if you see a packet for 10.96.4.21:80, rewrite the
destination".

Conceptually, the rules look like this:

# match the Service
-d 10.96.4.21/32 -p tcp --dport 80        -j KUBE-SVC-ARGOCD                                                                                  
# pick one of three backends with equal probability
KUBE-SVC-ARGOCD  --probability 0.333      -j KUBE-SEP-A
KUBE-SVC-ARGOCD  --probability 0.500      -j KUBE-SEP-B     # 0.5 of the remaining 2/3
KUBE-SVC-ARGOCD                           -j KUBE-SEP-C     # whatever is left

# rewrite destination to the actual pod
KUBE-SEP-A  -j DNAT --to-destination 10.244.1.5:8080
KUBE-SEP-B  -j DNAT --to-destination 10.244.2.9:8080                                                                                          KUBE-SEP-C  -j DNAT --to-destination 10.244.3.4:8080

Note the probability cascade. 0.333, then 0.5 of what remains, then everything left. That produces an even 1/3 split across three pods.       
There is no proxy process in the data path. Despite its name, kube-proxy does not carry your traffic. It writes rules and then gets out of the way. The kernel does the rewrite. This is why Services add almost no latency.

Sharpening 3: "random" is per-connection, not per-packet
                                                                                                                                              Your word "random" is accurate for iptables mode, with on

The choice happens once per new TCP connection. After that, conntrack pins every packet of that connection to the same pod. Otherwise packets from one request would scatter across three pods and noth

So: 100 separate HTTP connections spread roughly 33/33/33sends everything to one pod.

This matters for Flink in Phase 5. A Kafka client or a Flink RPC connection is long-lived. It pins to a single backend and stays there. Service-level load balancing does not rebalance existing connections, which is why Flink does its own partition assignment rather than relying on a Service to spread work.

A correction to what I told you earlier

In §8.1 of the knowledge doc I said containerPort is "purtrue in the common case, but there is one exception, andit is exactly here.

targetPort can be a name instead of a number:

ports:
  - port: 80
    targetPort: http     # ← a name, not a number

Then each pod resolves that name from its own spec:

containers:
  - ports:
      - name: http
        containerPort: 8080

With a named targetPort, the containerPort declaration doce looks the name up per pod. This also lets differentpods behind one Service listen on different numbers.

Rare in practice, but it's the reason charts declare cont omitting it.

---
3. nodePort

A hole punched from outside the cluster into a Service.

When it exists: only when the Service is type: NodePort. A ClusterIP Service has no nodePort at all.

kind: Service
spec:
  type: NodePort        # ← without this, no nodePort exists
  ports:
    - port: 80
      targetPort: 8080
      nodePort: 30080   # ← the new layer

What happens: kube-proxy opens port 30080 on every node in the cluster. Not one node. All of them.

Traffic arriving on any node at 30080 gets forwarded to the Service, then to a pod. The pod can be on a completely different node. kube-proxy handles the hop.

The range is 30000-32767. This is a default on the API server (--service-node-port-range). You cannot use 8080 as a nodePort without changing that flag. This is why every number in your list starts with 30 or 31.

Who chooses it: you. If you omit it, Kubernetes assigns a random free one, which is useless here because kind's port maps are fixed when the cluster is created. Pin it.

Scope: reachable from anything that can route to a node's IP.

---
4. hostPort

This word means two different things. That is most of your confusion, and it is not your fault.

4a. hostPort in kind's extraPortMappings (what you are writing)

A Docker port publish on your laptop, attached to one node container.

extraPortMappings:
  - containerPort: 30080   # port on the NODE. Must equal the nodePort you pin later. UNIT 5B's door (one specific node).
    hostPort: 30080        # port on YOUR machine. the BUILDING's front door

This is not Kubernetes. It is Docker. kind runs the equivalent of docker run -p 30080:30080 on that node container.
the word because a kind node is a container.

Who chooses it: you. And you must choose at cluster creation time, because Docker publishes are fixed when a container starts.

4b. hostPort in a Kubernetes Pod spec (different thing, avoid it)

containers:
  - ports:
      - containerPort: 8080
        hostPort: 8080     # binds directly on whichever node runs this pod

This is a real Kubernetes feature. It binds the pod's port straight onto the node's network, skipping Services entirely.

You will find this when you search for "hostPort", and it is not what your kind file is doing. It is generally discouraged, because only one pod per node can hold a given port, which breaks scheduling.

## nodePort vs hostPort: the core difference
nodePort is cluster-wide. hostPort is node-local.

nodePort 30080:
  client → ANY node : 30080
                │
                │  kube-proxy DNAT, via the Service's EndpointSlice
                ▼
           any Ready pod, possibly on a different node


hostPort 8080:
  client → THAT ONE node : 8080
                │
                │  CNI portmap DNAT, direct
                ▼
           THE pod on that node. Nowhere else. No failover.

With nodePort you hit any node and Kubernetes finds a healthy pod for you. With hostPort you must know which node the pod is on, and if that pod is sick you get nothing.

---

Ignore 4b for this project.

        │  LAYER 4a  kind extraPortMappings  { hostPort: 30080, containerPort: 30080 }
        │            Docker publish, on ONE worker node container
        ▼
  worker node container : 30080
        │
        │  LAYER 3   nodePort: 30080
        │            kube-proxy, opened on every node by a NodePort Service
        ▼
  Service argocd-server  (ClusterIP 10.96.4.21) : 80
        │
        │  LAYER 2   port: 80  →  targetPort: 8080
        ▼
  Pod argocd-server-7d9f  (podIP 10.244.2.5) : 8080
        │
        │  LAYER 1   containerPort: 8080
        ▼
  the ArgoCD process, listening

## analogy
The building

┌─ APARTMENT BUILDING (your WSL machine) ──────────────────────┐
│  Front door of the building faces the street.                │
│                                                                │
│   Front door #30080 ← the DOORMAN holds this.                │
│         │              ***THIS IS hostPort***                │
│         │  doorman escorts the visitor to...                 │
│         ▼                                                     │
│  ┌─ UNIT 5B (the "worker" node container) ──────────────────┐│
│  │  Its own private front door, only inside the building.    ││
│  │                                                             ││
│  │   Unit door #30080 ← the UNIT'S BUTLER holds this.        ││
│  │         │             ***THIS IS nodePort/containerPort***││
│  │         │  butler escorts the visitor to...                ││
│  │         ▼                                                   ││
│  │  ┌─ BEDROOM (the "argocd-server" pod) ──────────────────┐ ││
│  │  │  Bedroom door #8080 ← ARGOCD PERSON answers this one. │ ││
│  │  │            ***THIS IS containerPort in the Pod spec***│ ││
│  │  └───────────────────────────────────────────────────────┘ ││
│  └─────────────────────────────────────────────────────────────┘│
└────────────────────────────────────────────────────────────────┘

Who owns what

┌──────────────────────────────┬───────────┬────────────────┬────────────────────────────────┐
│            Layer             │  Number   │  Who decides   │      Where you declare it      │
├──────────────────────────────┼───────────┼────────────────┼────────────────────────────────┤
│ 1. containerPort             │ 8080      │ the image      │ nowhere, it is informational   │
├──────────────────────────────┼───────────┼────────────────┼────────────────────────────────┤
│ 2. Service port / targetPort │ 80 / 8080 │ the Helm chart │ Service manifest               │
├──────────────────────────────┼───────────┼────────────────┼────────────────────────────────┤
│ 3. nodePort                  │ 30080     │ you            │ Helm values, or the Service    │
├──────────────────────────────┼───────────┼────────────────┼────────────────────────────────┤
│ 4. hostPort (kind)           │ 30080     │ you            │ kind-cluster.yaml, at creation │
└──────────────────────────────┴───────────┴────────────────┴────────────────────────────────┘

Only layer 3 has to match on two sides. You write 30080 in the kind file today, and 30080 in ArgoCD's Helm values in a later step. Those two must agree. Nothing else does.

See each layer yourself

Once the cluster is running, these three commands show you layers 4, 3-2, and 1:

# layer 4a: what Docker published, on which node container
docker port personalization-lab-worker

# layers 3 and 2: nodePort, port, targetPort, all in one view
kubectl get svc -A -o wide

# layer 1: what the pod claims to listen on
kubectl get pod <name> -o jsonpath='{.spec.containers[*].ports}'

Run those after Phase 0 and the four layers stop being abstract.

## Correct wrong config
### Wrong Port Mapping
```yaml
- role: worker
  image: kindest/node:v1.34.8@sha256:02722c2dedddcfc00febf5d27fbeb9b7b2c14294c82109ff4a85d89ac9ba3256
  labels:
    zone: zone-a
  extraPortMappings:
  # ArgoCD
  - containerPort: 30010
    hostPort: 80
  # Flink
  - containerPort: 30011
    hostPort: 80
  # Prometheus
  - containerPort: 30012
    hostPort: 80
  # Grafana
  - containerPort: 30013
    hostPort: 80
  # MinIO
  - containerPort: 30014
    hostPort: 80
  # Kafka
  - containerPort: 30015
    hostPort: 80
```

Reason:
Why the port collision happens

You're conflating two different things: "hostPort" as a concept (yes, it always means your machine) and "hostPort 80" as a specific resource. The concept being consistent doesn't make the resource shareable.

Think about a phone number. "My phone number" is a consistent concept, but a specific number like 555-0100 can only ring one physical phone. Someone can't dial 555-0100 and have it ring six different phones depending on... what? There's no information in the dial to disambiguate.

Same thing here. When a packet arrives at your machine's port 80, it is just a packet addressed to port 80. It carries no signal saying "this one's for ArgoCD" or "this one's for Flink." The only way Docker knows where to send it is the single rule you wired up when you ran -p. If you try to wire port 80 to six different destinations, Docker can't pick one, so it refuses to create the second rule at all:

Bind for 0.0.0.0:80 failed: port is already allocated

"Already allocated" is the literal mechanism. The first mapping (ArgoCD, say) claims port 80 and binds a real socket to it. The second mapping (Flink) tries to bind that exact same socket and finds it taken.

The part that is shareable, and this is probably what you were half-remembering, is containerPort. Six different node containers can each have something listening on port 8080 internally with zero conflict, because each node container has its own private number line. That's not the field that's broken here. It's hostPort, because there's only one machine and one number line for it.

The rule: every hostPort in the whole file must be unique. containerPort values can repeat across different nodes freely.

Correct version:
```yaml
- role: worker
  image: kindest/node:v1.34.8@sha256:02722c2dedddcfc00febf5d27fbeb9b7b2c14294c82109ff4a85d89ac9ba3256
  kubeadmConfigPatches:
  - |
    kind: JoinConfiguration
    nodeRegistration:
      kubeletExtraArgs:
        node-labels: "topology.kubernetes.io/zone=zone-a"
  extraPortMappings:
  # ArgoCD
  - containerPort: 30010
    hostPort: 30010
    protocol: TCP
  # Flink
  - containerPort: 30011
    hostPort: 30011
    protocol: TCP
  # Prometheus
  - containerPort: 30012
    hostPort: 30012
    protocol: TCP
  # Grafana
  - containerPort: 30013
    hostPort: 30013
    protocol: TCP
  # MinIO
  - containerPort: 30014
    hostPort: 30014
    protocol: TCP
  - containerPort: 30015
    hostPort: 30015
    protocol: TCP
  # Kafka external listener (Phase 1, ADR 0002). Strimzi's nodeport
  # listener needs one port for bootstrap plus one PER BROKER, because a
  # Kafka client is redirected to a specific broker after the metadata
  # request. A single bootstrap port is not enough.
  - containerPort: 30016 # bootstrap
    hostPort: 30016
    protocol: TCP
  - containerPort: 30017 # broker-0
    hostPort: 30017
    protocol: TCP
  - containerPort: 30018 # broker-0
    hostPort: 30018
    protocol: TCP
  - containerPort: 30019 # broker-0
    hostPort: 30019
    protocol: TCP
```

# why do we need cert-manager?
What an admission webhook actually is

When you kubectl apply a FlinkDeployment, the request doesn't go straight into etcd. It passes through a chain, and the operator inserts itself into that chain:

kubectl apply -f flinkdeployment.yaml
        │
        ▼
  kube-apiserver
        │
        │  "before I persist this, let me ask the operator if it's valid"
        ▼
  HTTPS call to the operator's webhook Service
        │
        │  operator checks the spec: valid image? sane parallelism?
        │  known upgrade mode? etc.
        ▼
  operator responds: allow / deny / mutate
        │
        ▼
  kube-apiserver persists to etcd (or rejects the request outright)

Without this, a broken FlinkDeployment (bad checkpointInterval, null job jar, a rescale request the running job can't actually satisfy) gets written to etcd, the operator's control loop picks it up on the next reconcile, then fails, sometimes visibly, sometimes as a confusing status
field three layers deep. The webhook catches it at kubecn error message right there in your terminal.

Why this specifically requires TLS, not just "best pract

This part isn't the operator's choice, it's baked into Kubernetes itself. kube-apiserver refuses to call a ValidatingWebhookConfiguration or MutatingWebhookConfiguration endpoint over plain HTTP. Only HTTPS, and only if the certificate the webhook presents matches a CA bundle the API server was explicitly told to trust, via the caBundle field on the webhook configuration object.

The reasoning: this webhook sees the full body of every FlinkDeployment create or update in the cluster before it's persisted, and it has the power to reject or silently rewrite that object. An unauthenticated HTTP endpoint doing that would be a trivial way for anything running in the cluster to man-in-the-middle every deployment. Kubernetes closes that door structurally, rather than trusting each operator author to remember to add TLS.

What you'd be doing by hand without cert-manager

This is the actual cost cert-manager is removing, concretely:

1. Generate a CA keypair.
2. Generate a server cert signed by that CA, for the exact in-cluster DNS name the webhook Service will be reached at (flink-operator-webhook-service.<namespace>.svc), since TLS validates the hostname too.
3. Base64-encode the CA cert into the caBundle field of the webhook configuration.
4. Mount the server cert and key into the operator's pod as a Secret.
5. Track the cert's expiry yourself, and repeat steps 2 through 4 before it lapses, usually annually.

Step 5 is the one that actually bites people in production. A silently expired webhook cert doesn't fail loud, it fails as "every kubectl apply to a FlinkDeployment starts timing out or getting refused," and the root cause is three hops away from the error message. cert-manager exists to make this a non-event: you declare a Certificate resource once, it issues it, and renews it automatically on a timer, well before expiry.

# cert-manager, issuer, certificate
## Step 1 only: who generates the CA keypair, and what the Issuer actually is

Two different objects are involved, and they play very different roles. One is *consulted*, the other *acts*.

```yaml
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: selfsigned-issuer
spec:
  selfSigned: {}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: my-selfsigned-ca
spec:
  isCA: true
  secretName: root-secret
  issuerRef:
    name: selfsigned-issuer
    kind: ClusterIssuer
```

## The `Certificate` object is a request, not the cert itself

This is the same declarative-vs-actual distinction your own notes already nail for `kube-controller-manager`, "read the wanted state, read the observed state, if different, act." `Certificate` here is the *wanted* state: "I want a CA cert to exist, with these properties." Applying this YAML writes it to etcd via the API server. Nothing cryptographic has happened yet. No key exists. No bytes exist. It's just a row in a database saying what should be true.

## `cert-manager` (the controller pod) is the actor

This is the piece doing actual work, and it's running the *exact same reconcile loop* as `kube-controller-manager`, just watching a different object type. It's a controller in the generic Kubernetes sense, cert-manager isn't special machinery, it's an ordinary custom controller that happens to do cryptography when it acts.

It watches for `Certificate` objects. It sees `my-selfsigned-ca` exists but has no corresponding valid cert yet. To fulfill it, it needs an answer to one question: *sign this with what, using whose authority?*

## The `Issuer` answers that one question, and does nothing else

`Issuer`/`ClusterIssuer` is pure configuration, a strategy the controller looks up, not a thing that runs or acts on its own. Its `spec` says what signing method to use:

- `selfSigned: {}` means: there is no external authority. Generate a private key, and sign the certificate with that same key.

This is worth sitting with for a second, because it sounds circular but isn't a mistake: **every root CA in existence is self-signed.** That's the literal definition of "root", there's no higher authority above it to sign with instead. The trust in a root CA doesn't come from who signed it, it comes from you deciding to trust it, which in this project happens automatically via `cainjector` in step 3, not through some external authority vouching for it.

## So, concretely, who does what in step 1

1. You apply the `ClusterIssuer` and `Certificate` YAML. Pure declaration, no crypto yet.
2. `cert-manager` (the controller pod) notices the new `Certificate`, follows `issuerRef` to `selfsigned-issuer`, sees the type is `selfSigned`.
3. Because of that type, `cert-manager` itself generates the private key and self-signs the certificate, no external call, no other component involved.
4. `cert-manager` writes both the private key and the resulting certificate into a `Secret` named `root-secret`, the exact name given in `spec.secretName`.

**One-line answer to your question: the `cert-manager` controller pod creates the actual certificate. The `Issuer` is not an actor at all, it's a piece of configuration the controller reads to decide *how* to create it.**

Does this match what you had, or is there a part of the actor/config distinction that's still fuzzy before we go to step 2?

## Step 2: the server cert, signed by the CA from step 1

```yaml
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: my-ca-issuer
  namespace: flink-operator
spec:
  ca:
    secretName: root-secret     # ← the Secret cert-manager wrote in step 1
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: webhook1-certificate
  namespace: flink-operator
spec:
  secretName: webhook1-certificate
  dnsNames:
  - flink-operator-webhook-service.flink-operator.svc
  issuerRef:
    name: my-ca-issuer
```

## Two keypairs exist here, and they are completely unrelated to each other

This is the part worth being precise about. There is the CA's keypair, from step 1, and there is a brand new keypair for this leaf certificate. The new one is not derived from the CA's key in any way, it's generated fresh, from scratch, the same as flipping a coin twice gives you two unrelated results.

The CA's private key only enters the picture **after** the new keypair already exists. It plays one narrow role: putting a signature on top of the new keypair's public half, plus some metadata (the DNS name). It's not building the new keypair. It's stamping a document that happens to mention it.

## Why bother with a second keypair at all, instead of just reusing the CA's

Two separate reasons, and they answer two separate questions.

**Why not let the webhook pod use the CA's key directly?** Because the CA's key is the most sensitive secret in this whole system, anyone holding it can vouch for *anything*. The webhook pod is a live, network-facing process, exactly the kind of thing you don't want holding a key that powerful. A separate, narrow-purpose key means if the webhook pod is ever compromised, the damage is limited to impersonating that one pod, not forging trust for the entire cluster.

**Why not have the new keypair just sign its own certificate, instead of asking the CA?** Because self-signing proves nothing to a stranger. Picture writing a note that says "trust me, I'm honest," and signing your own name to it, that convinces no one, since literally anyone could write the same note about themselves. It only means something once someone the reader *already trusts* signs off on it instead. The apiserver has agreed, in advance, to trust exactly one thing, the CA's public key, delivered via `caBundle`. A certificate only means anything to the apiserver if its signature traces back to that specific CA. A self-signed leaf cert, however new and however real the key behind it, traces back to nothing the apiserver ever agreed to trust.

So: the **new keypair** exists to prove, live, during the actual conversation, that the webhook pod really holds the private key it claims to. The **CA's signature** exists to give that claim meaning to someone who wasn't there when the key was created.

## What "signing" mechanically does to that content

The CA takes the certificate's content, the new public key plus the DNS name plus other metadata, computes a hash of it (a fingerprint that changes completely if even one byte changes), and encrypts that hash using its own private key. That encrypted hash is the signature.

Anyone holding the CA's public key, which is freely distributed, can reverse this: decrypt the signature to recover the hash, independently recompute the hash of the certificate's actual content, and compare. A match proves two things: only the CA's private key could have produced a signature that decrypts correctly, and the content hasn't been tampered with since it was signed.

## How the resulting certificate reaches the webhook pod

`cert-manager` writes the new private key and the signed certificate into the Secret named in `spec.secretName`, `webhook1-certificate`. From there it's ordinary Kubernetes plumbing, the same mechanism as any Secret reaching any pod: the operator's Helm chart mounts that Secret into the webhook pod's filesystem via a `volumeMounts` entry, `kubelet` performs the actual mount when the pod starts, and the webhook server process running inside that pod reads the mounted `tls.crt` and `tls.key` files to configure its own HTTPS listener, watching them for changes so it can reload without a restart.

**The apiserver never holds a copy of this certificate ahead of time.** It only holds the CA's public key, via `caBundle`, so that whatever certificate gets handed to it live, during the handshake, can be checked.

## The handshake itself

When the apiserver needs to call the webhook, it does not look up a DNS name. It reads the `Service` object directly, the same way any controller reads any Kubernetes object, gets its `ClusterIP`, and dials that address directly.

Only afterward, during the TLS handshake itself, does it separately check: does the certificate just presented carry the identity `flink-operator-webhook-service.flink-operator.svc`, the same string it independently expects, built from the same Service reference it used to find the IP. If the certificate's `dnsNames` says something else, the handshake is refused right there, regardless of whether the signature itself was valid.

## Concretely, who does what, in order

1. You apply the `Issuer` (pointing at `root-secret`, the CA's key from step 1) and the `Certificate` (with `dnsNames` set, no `isCA` flag).
2. `cert-manager` notices the new `Certificate`, follows `issuerRef`, sees the issuer type is `ca`.
3. `cert-manager` generates a brand new, unrelated keypair for this leaf cert.
4. `cert-manager` reads the CA's private key out of `root-secret`, and uses it to sign the new keypair's public key plus the DNS name.
5. `cert-manager` writes the new private key and the signed certificate into `webhook1-certificate`.
6. The operator's Helm chart mounts that Secret into the webhook pod. `kubelet` performs the mount. The webhook process reads the files and starts serving HTTPS with them.

**One-line summary: a brand new keypair is generated for the leaf, independent of the CA's own keypair. The CA's private key is used once, afterward, purely to sign that new keypair's public half, proving to anyone who trusts the CA that this specific key really belongs to `flink-operator-webhook-service`.**

## Step 3, with the actual code alongside it

There is a form. In real terms, this form is a Kubernetes object called `ValidatingWebhookConfiguration`. It tells the apiserver two things: where to send a request before it saves a FlinkDeployment, and what stamp to check for when the answer comes back.

Here is that form, right after it's first created, before anyone has touched it:

```yaml
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: webhook1
  annotations:
    cert-manager.io/inject-ca-from: flink-operator/webhook1-certificate
webhooks:
- name: webhook1.example.com
  admissionReviewVersions:
  - v1
  sideEffects: None
  clientConfig:
    service:
      name: flink-operator-webhook-service
      namespace: flink-operator
      path: /validate
      port: 443
    # caBundle is either missing here, or an empty string.
    # Nobody has filled it in yet.
```

The blank line, in code terms, is `caBundle`. Right now it's empty or not even present. The `service:` block right above it is the "where to send the request" part, and it's already filled in, that's the address. But there's nothing yet to check the answer against.

An empty `caBundle` is a real problem. The apiserver knows where to send its request, but it has no picture of the stamp it's supposed to look for on the reply.

Filling in that blank line is the entire job of a separate helper, a different pod called `cainjector`. It does nothing else. It doesn't make certificates, doesn't sign anything. It only watches for objects like this one with an empty `caBundle`, and fills them in.

How does `cainjector` know where to find the right stamp? Look back at the top of the YAML:

```yaml
metadata:
  annotations:
    cert-manager.io/inject-ca-from: flink-operator/webhook1-certificate
```

That annotation is the note. In plain terms it says "the stamp you need is over there, with the passport you already made", `webhook1-certificate` is the name of the leaf certificate from step 2. `cainjector` reads this note, goes and opens that certificate's Secret, and finds a stamp copy already sitting inside it, a field called `ca.crt`, which `cert-manager` put there automatically back when it signed the leaf cert. `cainjector` never has to go hunting somewhere else, the stamp copy is already packaged right alongside the passport.

Once `cainjector` does its work, here is the same form again, now complete:

```yaml
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: webhook1
  annotations:
    cert-manager.io/inject-ca-from: flink-operator/webhook1-certificate
webhooks:
- name: webhook1.example.com
  admissionReviewVersions:
  - v1
  sideEffects: None
  clientConfig:
    service:
      name: flink-operator-webhook-service
      namespace: flink-operator
      path: /validate
      port: 443
    caBundle: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUM5akND...
```

That one new line, `caBundle`, is the entire output of step 3. It's the stamp, copied over.

The note (the annotation) and the blank line (`caBundle`) sit on the same object, but they're read by two different things, for two different reasons. `cainjector` reads the annotation. The apiserver reads `caBundle`. The note only exists to tell `cainjector` where to look, it's never read by the apiserver at all.

`cainjector` doesn't do this once and walk away, either. It keeps watching. If the CA's stamp ever changes, it notices, and updates `caBundle` again on its own. Nobody has to remember to go fix this by hand.

And with that, the picture from the very first list is actually complete: make the CA's stamp (step 1), make a passport and sign it with that stamp (step 2), copy the stamp onto the form so it can be checked later (step 3, this one), the passport already gets handed to the webhook pod through an ordinary Secret mount (folded into step 2), and the whole thing renews itself automatically before anything expires (mentioned back when we first talked about why cert-manager exists at all).

# what is argo

ArgoCD solves exactly the problem you just ran into with cert-manager, so let's start there.

## The problem: what's running and what should be running can silently drift apart

When you ran `helm install cert-manager ...` by hand, two things happened: the cluster now has cert-manager running, and nowhere is there a durable record of that fact. If you'd instead run a slightly different command next week, or someone else ran a *different* command against the same cluster, nothing would notice or complain. The cluster's live state and "what we intended" are two separate things with nothing keeping them in sync. This is called **config drift**, and it's a well-known, real operational problem, not a hypothetical one.

## GitOps: make Git the single source of truth, and automate closing the gap

GitOps is a specific discipline for fixing this: files in a Git repository describe what *should* be running, and a piece of software, continuously, automatically, makes the live cluster match those files. Not "someone remembers to run a command after committing," an actual controller whose entire job is watching both sides and reconciling them.

This is the **exact same reconcile loop** you already know from `kube-controller-manager`:

```
1. Read the wanted state.
2. Read the observed state.
3. If they differ, change the observed state to match the wanted state.
4. Go back to step 1.
```

`kube-controller-manager` runs this loop against Kubernetes objects. The Flink Operator runs this same loop for a `FlinkDeployment`. **ArgoCD runs this identical loop, except the "wanted state" comes from files in a Git repository instead of from another Kubernetes object.**

```
   Git repo                          Live cluster
   (wanted state)                    (observed state)
        │                                   │
        │◀──────────── ArgoCD watches both ─┤
        │                                   │
        ▼                                   ▼
   "here's what should                "here's what's
    be running"                        actually running"
                    │
                    ▼
            do they match?
             │            │
            yes           no
             │            │
             ▼            ▼
          nothing      ArgoCD changes
          to do        the cluster to
                        match Git
```

## What ArgoCD concretely is

It's not a command-line tool that runs once and exits, like `helm install`. It's software that runs **inside the cluster**, as its own pods, `argocd-server` (the API and UI), `argocd-application-controller` (the actual reconcile loop), `argocd-repo-server` (fetches and renders Git content), and a few supporting pieces. Once installed, it just keeps running, forever, doing that loop continuously.

Its main object type is a **CRD called `Application`**, same concept as `FlinkDeployment`, a custom resource a controller watches. An `Application` object says, roughly: "here's a Git repo, here's a path in it, here's which cluster and namespace it belongs in." That's it. ArgoCD's controller does the rest: fetch, render (plain YAML, a Helm chart, several other formats), diff against live state, and either report the difference or fix it, depending on the sync policy.

Two things it tracks per `Application`, worth knowing the names of since you'll see them in the UI constantly:

- **Sync status**: does live state match Git right now? `Synced` or `OutOfSync`.
- **Health status**: is what's running actually working? `Healthy`, `Degraded`, `Progressing`, ArgoCD's own judgment, separate from just "does the object exist."

## Why this project adopted it specifically

Two things already decided, worth connecting explicitly:

**It directly fixes what just confused you.** Once something is installed as an ArgoCD `Application`, the `Application` manifest is a YAML file that lives in this repo. *That* is the durable record "cert-manager should be installed," not a command that scrolls off your terminal history. Everything from Phase 1 onward gets this property automatically, per [ADR 0004](../adr/0004-gitops-from-phase-0.md), cert-manager and ArgoCD itself are the only two things installed the old way, by hand, precisely because ArgoCD has to exist before it can manage anything else.

**It's also a concept the project deliberately wants to exercise, not just a convenience.** Your design spec's Phase 5 plan uses ArgoCD's drift-correction behavior directly as a drill: a Lua action patches a running `FlinkDeployment` live, and on ArgoCD's next sync, it notices the live state no longer matches Git and reverts the patch automatically. That's this exact reconcile loop, made visible on purpose.

One more piece you'll meet soon, briefly: an `Application` can itself manage *other* `Application` objects, a "root" `Application` that watches a directory of `Application` manifests and creates them. That's how "ArgoCD manages itself plus everything else" gets bootstrapped from a single initial install, rather than you hand-creating every `Application` one at a time. We'll get there once ArgoCD itself is up.

## App-of-apps, summarized

Applying a child `Application` by hand once and letting ArgoCD take it from there *would* work, but it just moves the "forgot to record it" problem down one layer, from the component to the `Application` object pointing at it. A root `Application` removes that last manual step too: it watches a folder (`manifests/argocd-apps/`), and every file committed there gets picked up and turned into a real `Application` automatically. Recreating the cluster from scratch then becomes "reinstall cert-manager and ArgoCD, apply one root file," not "remember and reapply N files in the right order," which mattered directly once this project actually had to recreate its cluster after the etcd IP/cert bug.

Two corrections worth keeping precise: the root file is a **pointer** to a folder, not a list of applications, the real list is whatever's actually committed there at any moment. And it isn't a one-time "on cluster create" trigger, it's the same reconcile loop running continuously, forever, on an already-live cluster exactly as much as a fresh one.

**What the root file actually tracks**: one repo, one revision (`HEAD`, deliberately not a pinned SHA, since pinning your own actively-changing repo would defeat continuous deployment), one path. Nothing else in the repo, not `CONTEXT.md`, not the ADRs, is watched by it. Each child `Application` it creates has its *own* separate `source.path`, so this is really a tree of independent watches, not one flat scan.

**How the watching actually happens**: polling, not push-triggered, every 3 minutes by default. Webhooks exist to eliminate that delay but require GitHub to reach *into* the cluster directly, not possible for a `kind` cluster on a laptop with no public IP. So a `git push` here has a real, expected delay before anything happens, not instant reactivity.

**Full field-by-field spec lives as inline comments in `manifests/argocd-apps/root.yaml` itself**, not duplicated here. Two fields worth remembering the reasoning for: `selfHeal: false` (needed for the Phase 5 drift-correction drill to be observable, not instant), `prune: true` (standard app-of-apps setting, lets children get created *and* removed as files appear/disappear, also means removing a child's file can cascade to deleting what it manages, not silent).

## Debugging the root Application: a real, multi-layer failure

Worth recording since it's a good example of not trusting the first plausible-sounding cause. Three distinct things looked like the same "SYNC STATUS: Unknown" symptom in sequence:

1. **`kubectl apply` only creates the object locally; it never touches Git.** The first "Unknown" was just `root.yaml` existing on disk but never being committed and pushed, so ArgoCD's repo-server had nothing to compare against.
2. **Once pushed, k9s itself briefly looked like the problem** but wasn't. k9s's namespace selection is sticky across resource views, if you were scoped to `default` while looking at pods, switching to the `Application` view keeps that same scope, showing `[0]` results correctly, just not what was meant. `0` for all-namespaces, `:ns` to pick one explicitly.
3. **The real, persistent cause: the GitHub repo was private.** ArgoCD's repo-server clones anonymously by default. GitHub's error for a private repo accessed without credentials is deliberately vague, `"authentication required: Repository not found"`, the same message whether the repo doesn't exist or you're just not authorized, specifically so an unauthorized request can't even confirm a private repo exists. Two legitimate fixes: make the repo public (simplest), or register credentials declaratively as a `Secret` labeled `argocd.argoproj.io/secret-type: repository` in the `argocd` namespace with a GitHub Personal Access Token (the more production-realistic pattern, since real company GitOps repos are almost always private).

**The technique that actually found it**: `kubectl describe application root -n argocd`, reading the `Conditions` and `Events` sections instead of just re-checking the top-line status. The events history specifically showed a sync that had *already succeeded* once before regressing back to `Unknown` with the identical error, which narrowed the search to "something about repo access changed after it briefly worked," not "it never worked." Confirmed independently of ArgoCD entirely with `GIT_TERMINAL_PROMPT=0 git ls-remote <repo-url>` run from the shell, the same anonymous-access check the repo-server itself does, git prompting for a username is proof positive a repo isn't actually public yet, regardless of what the GitHub UI is believed to say.

One more small, related lesson: `argocd.argoproj.io/refresh=hard` is a one-time, self-clearing trigger annotation, ArgoCD removes it after acting. It does not belong committed into `root.yaml`, that would create a permanent, meaningless drift the moment ArgoCD strips it back off the live object. Same imperative-vs-declarative split as `kubectl create deployment drill-check` back in Phase 0: a live nudge from the terminal, never saved anywhere.

## NodePort exposure and "node" terminology, applied to ArgoCD

Reusing the four-layer port model from earlier in this doc, applied concretely: `extraPortMappings` in `kind-cluster.yaml` only tunnels your laptop to one node container, it existed before ArgoCD was even installed and knows nothing about Kubernetes. The Helm chart's `server.service.type=NodePort` plus `server.service.nodePortHttp` is the separate, Kubernetes-side piece that actually claims that port inside the cluster. Both have to exist; neither substitutes for the other, a tunnel to an empty room is still just an empty room. The specific port number, `30010` for ArgoCD in this project's `kind-cluster.yaml`, not `30080`, the chart's own default, only "agrees" because both sides were deliberately set to match, not because of any built-in connection between them.

Also worth keeping precise: "node" in "kube-proxy opens this port on every node" and "node container" (the Docker container `kind` creates) are the same physical thing in this project, but different layers of abstraction. Kubernetes' `Node` object is deliberately blind to what's actually underneath it, a Docker container here, a real VM on a real cluster, which is exactly what lets the same YAML work unchanged in both places.

Small related note from the same conversation: Helm chart version and application version are independent numbers. ArgoCD's chart (`argo/argo-cd`) is maintained in a separate repo from ArgoCD itself, so several chart versions can share one unchanged app version underneath, unlike cert-manager, which deliberately keeps the two in lockstep. `--version` on `helm install` always means the chart version, never the app version.

## What is Strimzi, summarized

The same operator pattern as everything else in this stack (cert-manager, the Flink Operator, ArgoCD itself), applied to Kafka: declare a `Kafka` (and, in current versions, `KafkaNodePool`) custom resource, and a Cluster Operator reconciles the real brokers, Services, and config, no hand-written `StatefulSet`. A nested **Entity Operator** (Topic Operator + User Operator) manages `KafkaTopic`/`KafkaUser` the same way, one level deeper, an operator deployed and supervised by another operator.

Confirmed directly from Strimzi's docs rather than assumed: **ZooKeeper is gone, not just optional**, "Kafka 4.0 exclusively uses KRaft mode... Strimzi versions 0.46 and later no longer support ZooKeeper-based Kafka clusters." KRaft folds Kafka's own cluster coordination into Kafka itself via Raft, the same underlying idea as etcd's own Raft quorum from Phase 0, just for Kafka's metadata instead of Kubernetes'.

Why this project specifically uses it, not just "it's popular": it's the exact mechanism behind the four reserved Kafka ports in `kind-cluster.yaml` (`ADR 0002`, Strimzi's `nodeport` listener creates one `NodePort` Service per broker plus bootstrap, not one shared port), it keeps the whole stack's architectural pattern consistent (operator-managed everything, no exceptions), and it's a real CNCF project underlying Red Hat's commercial Kafka product, not a lab-only convenience.

