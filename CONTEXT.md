# Flink on Kubernetes Personalization Lab

A synthetic real-time e-commerce personalization pipeline, built to learn the
Flink DataStream API and self-managed highly available Kubernetes together. The
domain is deliberately Alibaba-shaped (real-time recommendation off a
clickstream) at lab scale.

## Language

### Domain

**Shopper**:
A simulated person browsing the store. The partition key for all per-person
state.
_Avoid_: User, customer, visitor, account

**Product**:
An item in the catalogue. The partition key for the price-change branch.
_Avoid_: Item, SKU, listing

**Click**:
A single observed shopper action against a product, carrying an event time that
may arrive out of order.
_Avoid_: Event, interaction, click event, pageview

**Product Change**:
A snapshot of one Product's price and stock at a point in time, carrying the
values it replaced. A fact about a Product, never about a Shopper. A *price
drop* is a Product Change whose `price` is below its `previousPrice`; it is a
question you ask of the record, not a separate kind of record. See
[ADR 0008](docs/adr/0008-product-change-as-a-state-snapshot.md).
_Avoid_: Price Change, Stock Change, inventory event, stock update

**Browsing Session**:
A run of one Shopper's Clicks with no gap longer than the session gap. Always
qualify as "browsing session" in prose, never bare "session".
_Avoid_: Session, visit, journey

**Promo Rule**:
A discount or promotion condition, broadcast to every subtask and replaceable
while the job runs.
_Avoid_: Rule, discount, promotion

**Signal**:
The output of one analytical branch about one Shopper, before any decision is
made. Session aggregates, CEP pattern matches, and enriched Clicks are all
Signals.
_Avoid_: Insight, feature, intermediate result

**Recommendation**:
The pipeline's final per-Shopper output, written to the sink. Covers both
product suggestions and triggered discounts, which are not modelled as separate
things.
_Avoid_: Offer, suggestion, personalization, result

**Unmatched Click**:
A closed Browsing Session whose candidate Product found no Product Change inside
the join interval, although its Clicks arrived on time. Distinct from a Late
Click, which is a timing failure rather than a matching outcome. Counted per
Browsing Session, not per Click, because `intervalJoin` is an inner join and
never calls back for a Click that did not match.
_Avoid_: Dropped click, miss

**Cart Abandonment**:
The Signal raised when a Shopper views a Product, adds that same Product to the
cart, and does not check out within 30 seconds. Detected by CEP.
_Avoid_: Abandoned basket, drop-off, lost sale

**Out of Stock**:
A candidate Product whose Product Change reports `stock` of zero. Its
Recommendation is suppressed rather than emitted, and routed to its own side
output so the suppression can be counted.
_Avoid_: Sold out, unavailable

**Late Click**:
A Click whose event time fell behind the watermark and missed its window.
_Avoid_: Dropped click, straggler

### Operations

**Active Side**:
The namespace whose job is currently RUNNING and producing Recommendations.
Exactly one side is ever Active.
_Avoid_: Live, primary, blue

**Standby Side**:
The namespace whose job is currently SUSPENDED. Holds no state of its own
between promotions.
_Avoid_: Green, secondary, passive, backup

**Promotion**:
Moving Active Side status from one namespace to the other by suspending with a
savepoint and restoring the other side from it. Directionless: `blue` and
`green` are namespace names, not roles.
_Avoid_: Cutover, failover, switch, going green

**Zone**:
A `topology.kubernetes.io/zone` label on a worker node. Simulated. It drives
real scheduling decisions but is not a real failure domain, since every node is
a container on one host.
_Avoid_: AZ, availability zone, region

**Drill**:
A deliberate, repeatable act of breaking something to observe recovery. Killing
a control-plane node, killing a TaskManager, killing the OTel Collector, forcing
a promotion timeout.
_Avoid_: Test, chaos, experiment

**Load Ramp**:
A scripted increase in generator throughput used to provoke backpressure,
consumer lag, and unschedulable pods.
_Avoid_: Load test, stress test, benchmark

**Native Variant**:
The `mode: native` FlinkDeployment. Carries the HA work, the Job Autoscaler, and
blue/green. The main line.
_Avoid_: Main deployment, the real one

**Standalone Variant**:
The `mode: standalone` FlinkDeployment. Exists only to demonstrate KEDA plus
reactive scaling. Never promoted.
_Avoid_: Reactive deployment, KEDA deployment

**Decoy Workload**:
A pause-image Deployment that tolerates the Karpenter NodePool taint purely to
generate unschedulable pods. Never carries real work, because kwok nodes have no
kubelet behind them.
_Avoid_: Dummy pods, filler, ballast
