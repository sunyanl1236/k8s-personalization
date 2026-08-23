claude --resume 5f15a50b-b95f-467e-a60e-97d7c528b942

# High availability, zero downtime deployment
1. Two separate kind clusters
2. One cluster, two namespaces

# For the blue/green cutover itself, is a short, bounded processing pause acceptable (standard Flink upgrade: stop blue with a savepoint, start green from it, redirect input), or do you want true zero-gap cutover (green consumes and catches up in parallel while blue keeps running, then an atomic switch)?
1. Bounded pause via savepoint
Stop-with-savepoint on blue, start green from that savepoint, then redirect Kafka consumption to green. Sub-second to low-seconds gap, exactly-once preserved. This is the standard, well-documented Flink upgrade pattern and much simpler to build correctly.
2.  True zero-gap dual-running cutover
Green starts from a savepoint and catches up to blue's current offset while blue keeps processing live traffic, then an atomic switch moves the output sink from blue to green with no gap at all. Meaningfully harder: needs offset-tracked catch-up detection, dual consumer groups, and careful exactly-once handling across the switch.

# How should the blue→green cutover itself be triggered and orchestrated? The existing ArgoCD Lua actions (suspend/resume/restart) do single-field patches; a savepoint-based cutover needs multiple steps (trigger savepoint, capture its path, update green's FlinkDeployment with that path, verify healthy, suspend blue) and Lua here has no string library to parse the savepoint path.
1. Scripted GitOps promotion runbook
     1. kubectl annotate flinkdeployment/blue-job savepoint-trigger      
     2. Poll status.jobStatus.savepointInfo for path             
     3. Edit green/flinkdeployment.yaml: spec.job.initialSavepointPath = <path> 
     4. git commit + push (GitOps repo)                
     5. ArgoCD syncs green, waits for RUNNING   
     6. kubectl patch flinkdeployment/blue-job spec.job.state=suspended

    Promotion runbook (a shell script, run manually or from CI, living in the repo as scripts/promote-green.sh):

    1. kubectl patch flinkdeployment/blue -n personalization-blue --type merge \
        -p '{"spec":{"job":{"state":"suspended"}}}'
    (requires upgradeMode: savepoint on the FlinkDeployment — this makes the
    operator take a savepoint AND tear down blue's TaskManagers as one step)
    2. Poll until status.jobStatus.state == "SUSPENDED" and
    status.jobStatus.savepointInfo.lastSavepoint.location is non-empty.
    Timeout -> abort: patch blue's state back to "running", nothing lost,
    green was never touched.
    3. Edit manifests/green/flinkdeployment.yaml:
        spec.job.initialSavepointPath = <captured path>, spec.job.state = running
    4. git commit + git push
    5. Poll green until status.jobStatus.state == "RUNNING".
    Timeout here is the one failure mode with no automatic safe path: blue
    is already suspended and green isn't up, so there IS real downtime until
    a human intervenes (resume green, or resume blue from the same
    savepoint as a fallback).

 The "bounded pause" is the wall-clock gap between step 2 finishing and step 5 finishing, which is what you already agreed was acceptable.

2. Extend ArgoCD Lua actions    

# Flink's Kafka source doesn't hand off partition ownership through Kafka's native consumer-group rebalance protocol — it assigns partitions itself and tracks offsets in its own checkpoint/savepoint state; any consumer-group offset commit to Kafka is just for external lag monitoring, not for coordinating who's "active." So there's no ownership handoff mechanism to lean on at all. The only thing that actually prevents dual-processing is: never start green until blue's TaskManagers are confirmed gone.

How a "normal" Kafka consumer group works

This is probably the mental model you have in mind, and it's correct for plain KafkaConsumer clients (or Kafka Streams).

1. Multiple consumer processes start up, all configured with the same group.id.
2. Each one sends periodic heartbeats to a Kafka broker acting as the "group coordinator" for that group.
3. The coordinator tracks group membership. When a consumer joins or a consumer's heartbeat times out (crash, network partition, whatever), the coordinator triggers a rebalance.
4. During a rebalance, the coordinator recomputes which partitions go to which surviving consumer, using a partition-assignment strategy, and hands out the new assignment.
5. Offsets are committed back to Kafka itself, into an internal topic called __consumer_offsets, keyed by (group.id, topic, partition). Whichever consumer picks up a partition looks up that stored offset and resumes from there.

In this model, "join the group" really does mean something. Kafka itself referees who owns which partition, and a newcomer can take over from a crashed member because the offset is stored centrally in Kafka.

How Flink's KafkaSource actually works

Flink does not use this mechanism for the thing that matters here (partition assignment and recovery offsets). It only optionally touches it for monitoring.

1. When a Flink job starts, a component called the KafkaSourceEnumerator (runs inside the JobManager) queries Kafka directly for the topic's partition list.
2. It assigns partitions itself, directly, to the job's own parallel source subtasks (e.g., "subtask 0 gets partitions 0–3, subtask 1 gets 4–7"). This decision is made entirely inside the Flink job. Kafka's group coordinator is not involved in this assignment at all.
3. Each subtask tracks "the next offset to read" for the partitions it owns as ordinary Flink operator state, the same mechanism that holds your keyed state and window state.
4. That offset state gets swept into every checkpoint and savepoint, atomically, alongside everything else in the job graph. When the job restores from a savepoint, it resumes each partition from exactly the offset recorded in that snapshot. This is what gives Flink's Kafka integration exactly-once behavior: the "how far did we get" bookkeeping rides in the same atomic snapshot as the rest of the job's state, not in a separate system.
5. Flink can optionally also commit offsets to Kafka's __consumer_offsets topic, for the same group.id you'd configure on a normal consumer. But this exists purely so external tools, like a Grafana panel showing consumer lag, can see "how far behind is this pipeline." Flink itself never reads that value back to 

Why this breaks the "hand-off" assumption

Because of point 2, two separate Flink jobs (blue's and green's), each with their own KafkaSourceEnumerator, do not coordinate with each other through Kafka at all, even if you give them the identical group.id. Each one independently believes it owns the entire topic and assigns all of its partitions to itself. There is no broker-side referee watching both and saying "blue already has these partitions, green can't have them too."

So if blue is still running when green starts, both are, at the same time, each fetching the full topic from their own checkpointed offsets, and both are writing results to the same sink. Every message gets processed twice. Kafka will not stop this, because from Kafka's point of view these are two unrelated client applications, not two members of one cooperating group.

The only thing that actually prevents that is a fact external to Kafka entirely: whether blue's TaskManager processes still exist and are still issuing fetch requests. Once kubectl and the Flink Operator have actually torn those processes down (step 2 in the corrected runbook, confirmed via status.jobStatus.state == "SUSPENDED"), there is nothing left generating fetches on blue's behalf, and it becomes safe to start green. That's an operational/lifecycle guarantee you have to enforce yourself through the ordering of the runbook, not something Kafka's protocol gives you for free.

WSL2 Fedora host
│
└── docker daemon
    │
    ├── personalization-lab-external-load-balancer
    │     └── HAProxy, round-robins to the 3 API servers
    │
    ├── personalization-lab-control-plane          ← "node" 1
    │     ├── containerd
    │     ├── kubelet
    │     └── static pods: etcd, kube-apiserver,
    │                      kube-controller-manager, kube-scheduler
    │
    ├── personalization-lab-control-plane2         ← "node" 2, etcd member 2
    ├── personalization-lab-control-plane3         ← "node" 3, etcd member 3
    │
    ├── personalization-lab-worker                 ← zone-a
    │     ├── containerd
    │     ├── kubelet
    │     └── your pods run HERE, nested one level deeper
    ├── personalization-lab-worker2                ← zone-b
    └── personalization-lab-worker3                ← zone-c




