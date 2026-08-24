# Solace Spring Boot Request/Reply

Guaranteed-messaging request/reply over **Solace PubSub+** with **Spring Boot + JCSMP**, no
Spring Cloud Stream layer. The worked example is train seat reservation.

The API deliberately mirrors **Spring Kafka**: `ReplyingSolaceTemplate.sendAndReceive(...)` on
the requestor, `@SolaceListener` + `@SendTo` on the replier. If you know
`ReplyingKafkaTemplate`, you already know this.

> Sample code, not an officially supported Solace product. Vendor it into your own package.

---

## Quickstart

Needs Docker and a JDK 17+.

```bash
# 1. broker  (~60s to become healthy)
docker compose -f docker/docker-compose.yml up -d

# 2. app
./mvnw -q -DskipTests install
java -jar booking-demo/target/booking-demo-0.1.0-SNAPSHOT.jar

# 3. book a berth
curl -s -X POST http://localhost:8091/api/bookings \
  -H 'Content-Type: application/json' \
  -d '{"zone":"nr","trainNo":"12951","journeyDate":"2026-09-15",
       "seatClass":"AC3","passengerName":"A Sharma","passengers":2}' | jq
```

```json
{
  "reservation": { "pnr": "0841866636", "status": "CONFIRMED", "coach": "B3", "berths": "1,2" },
  "latency":     { "totalMicros": 23322, "publishConfirmMicros": 12581 },
  "requestTopic": "cris/booking/seatReserve/request/v1/nr/12951",
  "partitionKey": "12951-2026-09-15-3a"
}
```

**macOS:** port 55555 is reserved by the OS, so the broker publishes SMF on **55565**.

---

## Latency test

One command, a histogram, then it exits. No Prometheus, no Grafana, nothing to install.

```bash
java -jar booking-demo/target/booking-demo-0.1.0-SNAPSHOT.jar \
  --spring.profiles.active=loadtest \
  --loadtest.count=4000 --loadtest.concurrency=48 --loadtest.warmup=300
```

```
Seat reservation latency
4,000 requests · concurrency 48 · CLOSED_LOOP · 300 warmup discarded
completed in 14.1s · 283 req/s

OUTCOMES
  success                 4,000  100.00%
  timeout                     0    0.00%

TOTAL ROUND TRIP
       p50       8.8 ms
       p99      24.6 ms
     p99.9      33.9 ms
       max     271.1 ms

DISTRIBUTION
      4 -     8 ms     1,181  █████████████████▎
      8 -    16 ms     2,323  ██████████████████████████████████
     16 -    32 ms       490  ███████▏
     32 -    64 ms         3  ▏

SEGMENTS                      p50        p99
  publish confirm          4.3 ms    13.5 ms
  queue dwell              4.3 ms    13.0 ms
  handler                  0.1 ms     0.3 ms
  dispatch delay           0.0 ms     0.0 ms

ORDERING
  sequence gaps                   0   no message loss
  out-of-order                    0   in order
```

Percentiles are **exact**, not interpolated from buckets. A bounded run can retain every sample —
100,000 longs is 800 KB and sorts in ~10 ms — so histogram estimation buys nothing here.

The **SEGMENTS** block is what makes it actionable. `p99 = 24.6 ms` is a fact; *13 ms of it was
queue dwell* says add repliers, whereas the same figure in `handler` would say look at the
database.

**Read the closed-loop caveat.** With bounded in-flight requests, a slowdown makes the generator
issue *fewer* requests, so slow periods are under-sampled and the tail flatters reality. That
measures **service time at a concurrency**, not latency at an arrival rate. Use
`--loadtest.mode=OPEN_LOOP --loadtest.rate=500` for the latter. The report always prints which
mode produced it.

---

## Topic taxonomy, and what wildcards buy you

Following Solace's `Domain/Noun/Verb/Version/Properties` template, properties ordered by
ascending cardinality:

```
Request   cris/booking/seatReserve/request/v1/{zone}/{trainNo}
Reply     cris/booking/seatReserve/reply/v1/{zone}/{trainNo}/{instanceId}
```

`*` matches exactly one level (and also works as a prefix within a level: `trn*`).
`>` matches one or more trailing levels, and is only a wildcard as the final level.

| Subscription | Purpose |
|---|---|
| `…/request/v1/>` | one replier pool takes every booking |
| `…/request/v1/nr/>` | shard by zone — Northern Railway only |
| `…/request/v1/*/12951` | ops taps one train during an incident |
| `…/reply/v1/nr/*/client-0` | **this instance's replies — replaces a hostname selector** |
| `…/reply/v1/nr/12951/>` | every reply for one train, across all instances |

### If you are replacing a JMS selector

A common pattern routes replies with `selector: "hostname = '${HOSTNAME}'"`. Solace evaluates
that **per message on the broker**, and the topic-architecture guidance lists message-property
filtering as an anti-pattern: put the discriminator in the topic instead.

```diff
- consumer:
-   selector: "hostname = '${HOSTNAME}'"        # broker filters every message
+ # the instance id IS a topic level, so the broker never sends what you did not ask for
+ cris/booking/seatReserve/reply/v1/nr/*/client-0
```

It is also a **hard prerequisite** for partitioned queues: JCSMP carries
`SELECTORS_NOT_SUPPORTED_ON_PARTITIONED_QUEUE` (subcode 99). The two features are mutually
exclusive at the broker.

### Reply topics carry request levels

`{trainNo}` appears in the reply topic even though the replier only echoes `replyTo` verbatim.
The *requestor* builds the concrete reply-to — it already knows the train — and subscribes once
with `*` at that position:

```
subscription   cris/booking/seatReserve/reply/v1/nr/*/client-0
reply-to       cris/booking/seatReserve/reply/v1/nr/12951/client-0
```

Zero coupling added, and per-train latency analysis needs no payload parsing. Configure with
`reply.per-request-placeholders` plus a `per-request-placeholder-expressions` entry — without the
expression the level renders as `unknown`, which still matches but tells you nothing.

---

## Architecture in one paragraph

Requests go to **one shared non-exclusive queue** that many repliers compete over. Replies go to
**a queue per requestor instance**, because the `CompletableFuture` awaiting a reply lives in one
JVM's heap and no other instance can complete it. Both legs are PERSISTENT.

That asymmetry is the design. A direct topic subscription on the replier side would fan out —
every instance receiving, and acting on, every request. For bookings that means one request
reserving N seats.

### The two-stage future

```java
RequestReplyFuture<SeatReservation> f = template.sendAndReceive(topic, key, req, SeatReservation.class);

f.getSendFuture().get(2, SECONDS);   // the broker has SPOOLED the request
SeatReservation r = f.get(5, SECONDS);  // somebody answered
```

A request/reply call has two independent failure points and one future cannot express both.
Collapse them and "the broker rejected my publish" is indistinguishable from "the replier is
down" — both arrive as the same timeout, pointing at the wrong half of the system.

```bash
# proves it: the request was spooled, nobody answered -> 504 with publishConfirmed=true
curl -s -X POST localhost:8091/api/bookings -H 'Content-Type: application/json' \
  -d '{...,"simulate":"timeout"}' | jq '{error, publishConfirmed}'
```

### Guaranteed delivery introduces a double-booking risk

At-least-once, not exactly-once. On a non-exclusive queue an unacknowledged message is
redelivered to another consumer, so a replier that reserves a seat and dies before acknowledging
will see the same request again. Two things prevent a second reservation:

1. **Idempotent receiver** — the correlation id is stored with the reservation; a repeat returns
   the original reply. See `SeatInventoryService.reserveOnce`.
2. **Acknowledge last** — process, publish the reply, wait for the broker to confirm it,
   *then* ack. Acking first loses the request if the process dies in between: the seat is taken
   and the customer is told it failed, with nothing to redeliver.

---

## Configuration

Connection settings use `solace.java.*`, the namespace the official
`solace-java-spring-boot-starter` already binds — nothing is reinvented. Pattern behaviour lives
under `solace.request-reply.*`. See [booking-demo/src/main/resources/application.yml](booking-demo/src/main/resources/application.yml).

The two switches worth knowing:

| Setting | Default | Why |
|---|---|---|
| `reply.endpoint-type` | `TEMPORARY` | No provisioning, nothing to clean up. `DURABLE` for production: its subscription is a broker-side object and survives reconnects outright. |
| `replier.partitioning.partition-count` | `0` (flat) | Flat keeps everything in JCSMP with no SEMP and no loss of selectors, browsing or replay. Above 0 serialises same-train bookings — but needs SEMP, because JCSMP cannot express a partition count at any version. |

### The temporary-queue hazard

A temporary reply queue survives a disconnect for 60 s (180 s across an HA failover), then is
destroyed. On reconnect the broker recreates it **without its topic subscription**: session up,
flow bound, nothing logged, and every request times out for ever.

`recreate-on-reconnect` redoes the whole sequence, and `/api/diagnostics/reply-path` turns that
state into an answer rather than a mystery. Use `DURABLE` in production and this cannot happen.

### Provisioning modes

`CREATE_IF_MISSING` (default), `VALIDATE`, `OFF`. Safe to ship enabled because drift is **loud**:
`FLAG_IGNORE_ALREADY_EXISTS` suppresses only "already exists", while a queue whose properties
differ raises `PropertyMismatchException` naming the offending property. Verified against a live
broker — see [spike/README.md](spike/README.md).

---

## Distributed tracing (optional, off by default)

Disabled unless you ask for it, and inert even then if the OpenTelemetry libraries are absent —
so a customer who wants nothing to do with tracing changes no configuration and carries no
dependency.

```yaml
solace:
  request-reply:
    tracing:
      enabled: false          # default. true activates it
      propagate-context: true # carry W3C trace context between processes
```

Three conditions must all hold for it to activate: the flag is true, the OpenTelemetry API is on
the classpath, and no `TracingContextBridge` bean is already defined. Fail any one and the library
uses a no-op bridge that captures nothing and wraps nothing.

`GET /api/diagnostics/endpoints` reports both `configuredEnabled` and `active`, which are
deliberately separate — tracing can be switched on and still be inert.

To turn it on, add the dependencies and flip the flag:

```xml
<dependency>
  <groupId>io.opentelemetry</groupId><artifactId>opentelemetry-api</artifactId>
</dependency>
<dependency>
  <!-- JCSMP has its OWN integration, distinct from the newer Java API's -->
  <groupId>com.solace</groupId><artifactId>solace-opentelemetry-jcsmp-integration</artifactId>
</dependency>
```

### What it does that metrics cannot

Two different jobs, and the first is easy to overlook:

- **Capture and restore** fixes *parent attribution*. `future.complete()` runs its dependents on
  the thread that completed it, holding the **reply's** context — but the span that should parent
  the continuation is the one active when the request was issued. Without this the trace is
  connected and wrongly parented, which is harder to spot than a broken one. The request's context
  is stored in `PendingRequest` and restored on completion.
- **Inject and extract** carries trace context in the message, so requestor and replier appear in
  one trace rather than two.

Cross-process propagation needs the Solace integration jar; without it the in-process half still
works and a warning explains what is missing. Broker-side spans (which put both ends of a queue
dwell on the *broker's* clock, eliminating clock skew) additionally need a telemetry profile on the
broker and an OpenTelemetry Collector with the Solace receiver.

### One interaction worth knowing

This design moves the handler and future completion onto bounded executors, off the JCSMP dispatch
thread. OpenTelemetry context lives in a thread-local, and the Java agent propagates it across
executors by an **exact class-name allowlist** — `ThreadPoolExecutor` is on it, a custom
`Executor` is not. Both pools here are therefore plain JDK pools: the ordinary choice is also the
one that keeps traces whole. Substituting a custom `Executor` would silently break propagation.

---

## Coming from Spring Kafka

| Spring Kafka | Here |
|---|---|
| `ReplyingKafkaTemplate` | `ReplyingSolaceTemplate` |
| `sendAndReceive(...)` → `RequestReplyFuture` | same names |
| `getSendFuture()` → `SendResult` | `getSendFuture()` → `PublishResult` |
| `ProducerRecord` **key** | `QUEUE_PARTITION_KEY` user property |
| `@KafkaListener(topics=…)` | `@SolaceListener(queue=…, topics=…)` |
| consumer `groupId` | `queue` — a non-exclusive queue **is** the consumer group |
| `@SendTo` | `@SendTo` — Spring's own annotation, reused |
| `KafkaHeaders.CORRELATION_ID` | `SolaceHeaders.CORRELATION_ID` (a *native* SMF field) |
| `KafkaHeaders.REPLY_PARTITION` | not needed — per-instance reply topic replaces it |
| `spring.kafka.*` | `solace.java.*` + `solace.request-reply.*` |

### Where the mental model breaks

| Concept | Kafka expectation | Solace reality |
|---|---|---|
| **Ordering** | guaranteed within a partition | a flat non-exclusive queue guarantees **none**. `concurrency` buys parallelism, not ordering. A partitioned queue is the analogue of a partition key. |
| Provisioning | you provision *topics* | inverted — topics are just strings; **queues** are objects with permissions |
| Replay | retained by time/size, seek to an offset | a queue **drains on ack**. Reprocessing needs the separate Message Replay feature. |
| Rebalancing | stop-the-world partition reassignment | per-message round robin. Adding a pod takes effect immediately, no rebalance storm, no partition-count ceiling on consumers. |
| Filtering | flat topic names, filter in-app | hierarchical topics filter **per message at the broker**. No Kafka equivalent. |
| Dead letters | client-side recoverer to `<topic>.DLT` | broker-side DMQ + max-redelivery, which keeps working when the consumer is what is broken |
| Queue browsing | n/a | **unavailable** on a partitioned queue (subcode 98) |

---

## Endpoints

| Endpoint | What |
|---|---|
| `POST /api/bookings` | one reservation, with latency breakdown |
| `POST /api/bookings?…simulate=` | `timeout`, `remote-error`, `slow-handler` — reproduce each failure mode |
| `GET /api/diagnostics/endpoints` | what was actually provisioned, not what was configured |
| `GET /api/diagnostics/reply-path` | is this instance's reply path really bound and subscribed |
| `POST /api/latency/start` · `POST /api/latency/report` | exact percentiles over ad-hoc traffic |
| `GET /actuator/health` | session and endpoint state |

---

## Tests

```bash
./mvnw test        # spins up a broker via Testcontainers
```

13 integration tests against a real broker. They exist because the properties that matter here are
properties of the *broker interaction*, and none of them can be caught by a test that mocks it.

| Test | Asserts |
|---|---|
| `RequestReplyIntegrationTest` | round trip; the send future resolving independently of the reply; **one request doing work exactly once despite three competing flows**; a replayed correlation id not repeating the work; 60 concurrent requests correlated independently; timeout eviction leaving nothing pending |
| `ReplyPathReconnectIntegrationTest` | replies still arrive after the connection is **severed from outside** via SEMP. The single most valuable test here: without re-subscription the queue would exist, the flow would be bound, nothing would log, and every request would time out for ever |
| `ProvisionDriftIntegrationTest` | identical re-provision is idempotent; drifted properties raise `PropertyMismatchException`; the ignore flag suppresses only "already exists". This is what makes `CREATE_IF_MISSING` safe as a default |
| `PartitionedQueueIntegrationTest` | SEMP creates a partitioned queue; a resize is refused unless explicitly allowed, because it deletes messages; a clear error when SEMP is unconfigured |
| `TracingToggleIntegrationTest` | tracing genuinely off by default, genuinely on when configured |

Note the Testcontainers setup uses `GenericContainer` rather than the Solace module: the module
rejects `default` as a username and does not set `container=docker` or a large enough shared-memory
size, without which this image fails platform detection and exits.

## Layout

```
solace-request-reply-core/     the reusable library
  api/         ReplyingSolaceTemplate · RequestReplyFuture · @SolaceListener · SolaceHeaders
  core/        template · correlation store · timeout reaper · codec
  endpoint/    reply endpoints (temporary/durable) · request-queue provisioner · SEMP client
  transport/   session · persistent publisher + ack correlation · flow consumer
  listener/    @SolaceListener discovery and container
  latency/     six-segment samples · exact percentiles · histogram renderer

booking-demo/                  the runnable sample
docker/                        local broker
spike/                         the provision-drift experiment and its result
```

## Licence

Apache-2.0.
