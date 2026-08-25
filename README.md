# Solace Spring Boot Request/Reply

Request/reply over **Solace PubSub+** using **Spring Boot and JCSMP**, with guaranteed messaging
and no Spring Cloud Stream layer. The worked example is train seat reservation.

The API follows Spring Kafka conventions: `ReplyingSolaceTemplate.sendAndReceive(...)` on the
requestor, `@SolaceListener` with `@SendTo` on the replier. If you have used
`ReplyingKafkaTemplate`, most of this will look familiar.

> This is sample code, not an officially supported Solace product. If you adopt it, copy it into
> your own package and take ownership of it.

---

## Quickstart

You need Docker and a JDK 17 or later.

```bash
# 1. Start a broker. It takes about a minute to become healthy.
docker compose -f docker/docker-compose.yml up -d

# 2. Build and run the demo.
./mvnw -q -DskipTests install
java -jar booking-demo/target/booking-demo-0.1.0-SNAPSHOT.jar

# 3. Book a berth.
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

On macOS, port 55555 is reserved by the operating system, so the broker publishes SMF on port
55565 instead.

For a guided, step-by-step tour of the demo — the two-stage future, the double-booking guard,
splitting the two sides across processes, and the latency harness — see
[booking-demo/README.md](booking-demo/README.md).

---

## How it works

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/architecture-dark.svg">
  <img alt="Request path through a shared non-exclusive queue to competing repliers, and reply path back through a per-instance reply queue" src="docs/architecture-light.svg">
</picture>

Requests go to a single shared queue that many repliers consume from. Any replier can handle any
booking, so the broker load-balances across them.

Replies go to a separate queue for each requestor instance. The `CompletableFuture` waiting for a
reply lives in the heap of one specific JVM, and no other instance can complete it, so replies have
to be addressed rather than load-balanced.

The request side deliberately uses a queue rather than a direct topic subscription. With a topic
subscription, every replier instance would receive every request, run the handler, and publish a
reply. In a booking system, one request would reserve a seat on each instance.

### The two-stage future

```java
RequestReplyFuture<SeatReservation> f =
        template.sendAndReceive(topic, key, req, SeatReservation.class);

f.getSendFuture().get(2, SECONDS);      // the broker has spooled the request
SeatReservation r = f.get(5, SECONDS);  // a replier answered
```

A request/reply call can fail in two independent ways. The request may never reach the broker, or it
may reach the broker and go unanswered. A single future cannot report both, so both arrive as the
same timeout and you cannot tell which half of the system to investigate.

`getSendFuture()` resolves when the broker acknowledges that the request is spooled. It fails
immediately if the publish is rejected, for example because the spool is full or a permission is
missing. You can ignore it and get single-outcome behaviour, but it is there when the difference
matters.

You can see the distinction directly:

```bash
# The request is spooled but nothing replies. HTTP 504, and publishConfirmed is still true.
curl -s -X POST localhost:8091/api/bookings -H 'Content-Type: application/json' \
  -d '{"zone":"nr","trainNo":"12951","journeyDate":"2026-09-15","seatClass":"AC3",
       "passengerName":"A Sharma","passengers":1,"simulate":"timeout"}' \
  | jq '{error, publishConfirmed}'
```

### Guaranteed delivery, and the idempotency that completes it

Guaranteed messaging is what makes a booking survive a replier crash, a network partition or a broker
failover. Every request is persisted to the broker's spool and stays there, redelivered as needed,
until a consumer acknowledges it. No request is silently dropped — which is exactly the property a
reservation system needs, and the reason this sample uses persistent delivery throughout.

That guarantee is at-least-once, and redelivery is how it is honoured. If a replier reserves a seat
and then dies before acknowledging, the broker hands the request to another consumer instead of
losing it. The work is never lost; it can simply be presented more than once.

Pairing that with an idempotent handler gives you exactly-once *effects* — no message lost, no seat
reserved twice. It is a small amount of code, and the same combination production booking and
payment systems are built on:

1. **Idempotent handling.** The correlation id is stored alongside the reservation, and a repeated
   request returns the original reply instead of doing the work again. See
   `SeatInventoryService.reserveOnce`. In a real service, the reservation and that record belong in
   one database transaction with a unique constraint.
2. **Acknowledge last.** The replier processes the request, publishes the reply, waits for the broker
   to confirm the reply is spooled, and only then acknowledges the request. That ordering is what
   keeps the broker's copy authoritative: until the reply is safely stored, the request is still on
   the queue and still redeliverable, so a crash anywhere in between costs nothing.

---

## Topic taxonomy and wildcards

The topics follow the Solace `Domain/Noun/Verb/Version/Properties` template, with properties
ordered from lowest to highest cardinality.

```
Request   cris/booking/seatReserve/request/v1/{zone}/{trainNo}
Reply     cris/booking/seatReserve/reply/v1/{zone}/{trainNo}/{instanceId}
```

There are two wildcard characters. `*` matches exactly one level, and also works as a prefix inside
a level, so `trn*` matches `trn123`. `>` matches one or more trailing levels, and only acts as a
wildcard when it is the final level.

| Subscription | What it gives you |
|---|---|
| `…/request/v1/>` | one pool of repliers handles every booking |
| `…/request/v1/nr/>` | shard by zone, so this pool only handles Northern Railway |
| `…/request/v1/*/12951` | tap a single train while investigating an incident |
| `…/reply/v1/nr/*/client-0` | one instance's replies, which replaces a hostname selector |
| `…/reply/v1/nr/12951/>` | every reply for one train, across all instances |

### Replacing a JMS selector

A common approach routes replies with a selector such as `hostname = '${HOSTNAME}'`. The broker
evaluates that for every message, and the Solace topic architecture guidance recommends putting the
discriminator in the topic instead.

```diff
- consumer:
-   selector: "hostname = '${HOSTNAME}'"
+ # The instance id is a topic level, so the broker only sends what this instance asked for.
+ cris/booking/seatReserve/reply/v1/nr/*/client-0
```

Moving off selectors is also a prerequisite for partitioned queues. JCSMP defines the error subcode
`SELECTORS_NOT_SUPPORTED_ON_PARTITIONED_QUEUE`, because the broker does not support both features on
the same queue.

### Why the reply topic includes the train number

The replier only echoes the `replyTo` value from the request, so it never needs to know how the
reply topic is structured. The requestor builds the concrete reply topic, because it already knows
the train number, and subscribes once using a wildcard in that position.

```
subscription   cris/booking/seatReserve/reply/v1/nr/*/client-0
reply-to       cris/booking/seatReserve/reply/v1/nr/12951/client-0
```

The train number is then visible in the topic, so you can analyse latency per train without parsing
payloads. Configure it with `reply.per-request-placeholders` and a matching
`per-request-placeholder-expressions` entry. If you list a placeholder without an expression, the
level renders as `unknown`. The subscription still matches, so nothing breaks, but the information
is lost.

---

## Configuration

Connection settings use the `solace.java.*` namespace, which the official
`solace-java-spring-boot-starter` already binds. This library adds nothing there. Its own settings
live under `solace.request-reply.*`.

There are three places to look, depending on what you need:

| File | What it is |
|---|---|
| [docs/configuration-reference.yml](docs/configuration-reference.yml) | Every property the library reads, with its default and an explanation. Start here. |
| [booking-demo/src/main/resources/application.yml](booking-demo/src/main/resources/application.yml) | A working configuration with real values, used by the demo. |
| `docs/config.json.example` | Template for broker credentials. Copy to `config.json`, which is gitignored. |

### A minimal configuration

This is enough to send and receive:

```yaml
solace:
  java:
    host: tcp://localhost:55565
    msg-vpn: default
    client-username: default
    client-password: default

  request-reply:
    request:
      timeout: 5s
    reply:
      topic-pattern: "my/service/reply/v1/{instanceId}"
      queue-name-pattern: "q.my.service.reply.{instanceId}"
    replier:
      queue: q.my.service.requests
      topics:
        - "my/service/request/v1/>"
```

Everything else has a default. The defaults are chosen so that a first run against a fresh broker
works without any provisioning: the reply endpoint is temporary, and the request queue is created
if it does not exist.

### The settings worth deciding deliberately

| Setting | Default | Why it matters |
|---|---|---|
| `reply.endpoint-type` | `TEMPORARY` | Temporary queues need no provisioning and leave nothing behind. Use `DURABLE` in production, because its subscription is a broker-side object and survives reconnects. |
| `replier.partitioning.partition-count` | `0` (flat) | A flat queue needs no SEMP access and keeps selectors, queue browsing and replay, but gives no ordering. Above zero serialises requests that share a partition key. |
| `request.ttl-matches-timeout` | `true` | Stops a replier acting on a request after the requestor has given up. Turning it off can produce work nobody is waiting for. |
| `replier.provision.max-redelivery` | `3` | Zero means redeliver forever, so one malformed message loops indefinitely. |
| `java.reconnect-retries` | — | Set this to at least 100 with a 3000 ms wait, which gives the 300 seconds needed to survive an HA failover. The commonly copied value of 20 only gives 60 seconds. |
| `tracing.enabled` | `false` | See [Distributed tracing](#distributed-tracing). |

### The temporary queue hazard

A temporary reply queue survives a disconnect for 60 seconds, or 180 seconds across a broker
failover. After that the broker destroys it. When the client reconnects, the broker recreates the
queue but does not restore its topic subscription.

In that state the session is connected, the flow is bound, the queue exists, and nothing is logged.
Every request times out until the process is restarted.

Two settings deal with this. `recreate-on-reconnect` redoes the whole sequence after a reconnect.
`canary-on-reconnect` then publishes a probe to this instance's own reply topic and requires it to
come back, which is the only check that proves a message can actually complete the round trip. The
result is reported through the reply-path health indicator, so a readiness probe can remove the
instance rather than leaving it to accept requests it cannot answer.

Using `DURABLE` avoids the problem entirely.

### Provisioning modes

`replier.provision.mode` accepts `CREATE_IF_MISSING` (the default), `VALIDATE` and `OFF`.

Creating on startup is safe to leave enabled because configuration drift is reported rather than
ignored. `FLAG_IGNORE_ALREADY_EXISTS` only suppresses the "already exists" error. If the queue
exists with different properties, JCSMP raises `PropertyMismatchException`, which names the property
that differs. This was verified against a live broker; see [spike/README.md](spike/README.md).

Partition counts are the exception. JCSMP cannot express one, so a partitioned queue is created or
verified over SEMP, using the management credentials under
`replier.partitioning.semp`. A count that does not match is a startup failure rather
than something the application changes on its own, because Solace requires the queue to be drained
first and reducing the count deletes the messages held in the removed partitions.

---

## Distributed tracing

Tracing is disabled by default. Three conditions must all be true for it to become active:

1. `solace.request-reply.tracing.enabled` is `true`,
2. the OpenTelemetry API is on the classpath, and
3. no `TracingContextBridge` bean is already defined.

If any of them is false, the library uses a no-op bridge that captures nothing and allocates no
wrappers. The OpenTelemetry dependencies are declared as optional, so a project that leaves tracing
disabled does not carry them at all.

```yaml
solace:
  request-reply:
    tracing:
      enabled: false           # set to true to activate
      propagate-context: true  # carry W3C trace context between processes
```

`GET /api/diagnostics/endpoints` reports `configuredEnabled` and `active` separately, because
tracing can be switched on and still be inactive if the libraries are missing.

To enable it, add the dependencies and set the flag:

```xml
<dependency>
  <groupId>io.opentelemetry</groupId>
  <artifactId>opentelemetry-api</artifactId>
</dependency>
<dependency>
  <!-- JCSMP has its own integration library, separate from the newer Java API's. -->
  <groupId>com.solace</groupId>
  <artifactId>solace-opentelemetry-jcsmp-integration</artifactId>
</dependency>
```

### What tracing adds

It does two separate things.

**Capturing and restoring context** keeps a trace correctly parented. `future.complete()` runs its
dependent stages on whichever thread completed it, and that thread holds the context of the reply.
The span that should be the parent is the one that was active when the request was sent. The library
stores the request's context in `PendingRequest` and restores it when completing the future.

**Injecting and extracting context** carries the trace across processes, so the requestor and replier
appear in one trace instead of two. This part needs the Solace integration library. If it is absent,
the in-process behaviour above still works and the library logs a warning explaining what is missing.

Broker-generated spans put both ends of a queue dwell measurement on the broker's own clock, which
removes clock skew from the result. Those require a telemetry profile on the broker and an
OpenTelemetry Collector configured with the Solace receiver. That setup is outside the scope of this
sample.

### Tracing and the handler executors

The handler and the future completion both run on bounded executors rather than on the JCSMP dispatch
thread. OpenTelemetry stores context in a thread local, and the Java agent propagates it across
executors using a list of known executor class names. `ThreadPoolExecutor` is on that list; a custom
`Executor` implementation is not.

Both executors here are plain JDK thread pools, so propagation works without extra configuration. If
you replace them with a custom `Executor`, context propagation will stop working silently.

---

## Coming from Spring Kafka

| Spring Kafka | This library |
|---|---|
| `ReplyingKafkaTemplate` | `ReplyingSolaceTemplate` |
| `sendAndReceive(...)` returning `RequestReplyFuture` | same names |
| `getSendFuture()` returning `SendResult` | `getSendFuture()` returning `PublishResult` |
| `ProducerRecord` key | `QUEUE_PARTITION_KEY` user property |
| `@KafkaListener(topics = …)` | `@SolaceListener(queue = …, topics = …)` |
| consumer `groupId` | `queue`, since a non-exclusive queue is the consumer group |
| `@SendTo` | `@SendTo`, Spring's own annotation |
| `KafkaHeaders.CORRELATION_ID` | `SolaceHeaders.CORRELATION_ID`, a native SMF field |
| `KafkaHeaders.REPLY_PARTITION` | not needed, because each instance has its own reply topic |
| `spring.kafka.*` | `solace.java.*` and `solace.request-reply.*` |

### Differences that matter

| Concept | Kafka behaviour | Solace behaviour |
|---|---|---|
| Ordering | guaranteed within a partition | a flat non-exclusive queue gives no ordering. `concurrency` adds parallelism, not ordering. The equivalent of a partition key is a partitioned queue. |
| Provisioning | you provision topics | the reverse. Topics are just strings and need no setup, while queues are objects with permissions. |
| Replay | messages are retained by time or size, and you can seek | a queue drains as messages are acknowledged. Reprocessing needs the separate Message Replay feature. |
| Rebalancing | partitions are reassigned in a stop-the-world rebalance | messages are distributed one at a time across bound flows. Adding an instance takes effect immediately. |
| Filtering | topic names are flat, so consumers filter in the application | topics are hierarchical and the broker filters per message. |
| Dead letters | a client-side recoverer republishes to a `.DLT` topic | the broker moves messages to a dead message queue after the redelivery limit. |
| Queue browsing | not applicable | not available on a partitioned queue. |

---

## Latency test

A single command runs a test, prints a report, and exits. It needs no metrics backend.

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

The percentiles are exact rather than estimated from histogram buckets. A test run is bounded, so
every sample can be kept. 100,000 measurements is 800 KB and sorts in about ten milliseconds.

The segment breakdown is the part that tells you what to do next. A p99 of 24.6 ms on its own does
not say much. Knowing that 13 ms of it was queue dwell suggests adding replier instances, whereas the
same figure under `handler` would point at the database.

Buckets are on a log scale, doubling each row, because latency distributions have long tails and
linear buckets put almost everything in one row.

### Closed loop and open loop

The default mode keeps a fixed number of requests in flight and waits for each reply. When the system
slows down, the generator sends fewer requests, so slow periods are under-sampled and the tail looks
better than it really is. This measures service time at a given concurrency, not latency at a given
arrival rate.

For the latter, use open loop mode, which sends at a fixed rate regardless of replies:

```bash
--loadtest.mode=OPEN_LOOP --loadtest.rate=500
```

The report always states which mode produced it.

---

## Endpoints

| Endpoint | Purpose |
|---|---|
| `POST /api/bookings` | one reservation, with a latency breakdown |
| `POST /api/bookings` with `"simulate"` | `timeout`, `remote-error` or `slow-handler`, to reproduce each failure mode |
| `POST /api/bookings/replay?correlationId=…` | resend a request under a chosen correlation id, to check idempotency |
| `GET /api/diagnostics/endpoints` | what was actually provisioned, rather than what was configured |
| `GET /api/diagnostics/reply-path` | whether this instance's reply path is bound and subscribed |
| `POST /api/latency/start` and `POST /api/latency/report` | exact percentiles over ad-hoc traffic |
| `GET /actuator/health` | session and endpoint state |

---

## Tests

```bash
./mvnw test        # starts a broker with Testcontainers
```

Sixteen integration tests run against a real broker. The behaviour that matters here belongs to the
interaction with the broker, so a test that mocked it would not catch the problems these are written
to catch.

| Test | What it checks |
|---|---|
| `RequestReplyIntegrationTest` | a round trip; the send future resolving independently of the reply; one request producing exactly one unit of work despite three competing flows; a replayed correlation id not repeating the work; 60 concurrent requests correlated correctly; a timed-out request being evicted |
| `ReplyPathReconnectIntegrationTest` | replies still arrive after the connection is cut from outside using SEMP. Without re-subscription the queue would exist, the flow would be bound, nothing would be logged, and every request would time out. |
| `ProvisionDriftIntegrationTest` | re-provisioning with identical properties is a no-op; differing properties raise `PropertyMismatchException`; the ignore flag only suppresses "already exists" |
| `PartitionedQueueIntegrationTest` | SEMP creates a partitioned queue; resizing is refused unless explicitly allowed, because a decrease deletes messages; a clear error when SEMP is not configured |
| `ReplyPathCanaryIntegrationTest` | the probe completes and health reports UP; when the subscription is removed behind the library's back, the probe fails and health reports DOWN; re-applying the subscription restores it |
| `MinimalConfigIntegrationTest` | the minimal configuration shown in this README actually round-trips, so the example cannot rot |
| `TracingToggleIntegrationTest` | tracing is off by default and active when configured |

The tests use `GenericContainer` rather than the Testcontainers Solace module. The module rejects
`default` as a client username, and does not set `container=docker` or a large enough shared memory
size. Without those two settings this broker image fails its platform check and exits. Readiness is
determined by a successful client login, because the management API starts answering well before the
message VPN accepts connections.

If your Docker host is already running other brokers, the suite may time out waiting for its own
container to become healthy.

---

## Layout

```
solace-request-reply-core/     the reusable library
  api/         ReplyingSolaceTemplate, RequestReplyFuture, @SolaceListener, SolaceHeaders
  core/        template, correlation store, timeout reaper, payload codec
  endpoint/    reply endpoints, request queue provisioner, SEMP client
  transport/   session, publisher with acknowledgement handling, flow consumer
  listener/    @SolaceListener discovery and container
  latency/     segment measurement, exact percentiles, histogram rendering
  tracing/     optional OpenTelemetry bridge

booking-demo/                  the runnable sample
docker/                        local broker
spike/                         the provisioning experiment and its result
```

## Licence

Apache 2.0.
