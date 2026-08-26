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
  "inventoryRow": "12951-2026-09-15-3a"
}
```

On macOS, port 55555 is reserved by the operating system, so the broker publishes SMF on port
55565 instead.

For a guided, step-by-step tour of the demo — the two-stage future, the double-booking guard,
splitting the two sides across processes, and the latency harness — see
[booking-demo/README.md](booking-demo/README.md).

---

## How it works

![Requestors publish to one shared request queue that competing repliers consume from; each requestor has its own durable reply queue subscribed to a topic carrying its instance id, so replies return to the instance that is waiting](docs/architecture.png)

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

### Acknowledgement modes: CLIENT and AUTO

That "acknowledge last" guarantee is specific to `@SolaceListener(ackMode = "CLIENT")`, the
default. A second mode, `AUTO`, exists because JCSMP exposes it — but it gives up more than it
looks like at first glance, so it is worth being explicit about what each one buys you.

`CLIENT`: the container acknowledges the request itself, only once the reply is published and the
broker has confirmed it, as above. A handler that throws `RetryableHandlerException`, or a reply
that fails to publish, gets an active redelivery — the container settles the request FAILED on
your behalf, subject to `replier.provision.max-redelivery`.

`AUTO`: JCSMP acknowledges for you, and — confirmed against a live broker, not just the docs — it
does so the instant the handler returns, regardless of what the handler actually did. That has two
consequences, not one:

- **It blocks the whole process, not just its own queue.** Every flow in a session shares one
  JCSMP dispatch thread, so an `AUTO` handler has to run directly on that thread rather than this
  listener's own pool, the way `CLIENT` handlers do. A slow `AUTO` handler stalls every other
  listener and every reply in the process for as long as it takes to return.
- **It has no failure path at all.** The settlement outcomes that make `CLIENT`'s active
  redelivery possible are a CLIENT-ack-only capability in JCSMP itself. A `RetryableHandlerException`
  or a broken reply-publish cannot force a redelivery under `AUTO` — JCSMP has already decided to
  acknowledge the message by the time either of those runs, and nothing the handler does can
  change that afterwards.

`AUTO` fits a fast handler that sends no reply and can tolerate an occasional failure disappearing
unnoticed. `CLIENT` is the right choice for everything else, including anything that replies.

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
works without provisioning anything by hand: the reply queue, the request queue and the dead
message queue are all created if they do not exist.

### The settings worth deciding deliberately

| Setting | Default | Why it matters |
|---|---|---|
| `reply.enabled` | `true` | Set `false` on a replier-only process. A replier is never addressed on a reply queue, so leaving it on provisions a durable queue that is subscribed, bound and never used — and under a Kubernetes Deployment, strands one per pod on every rollout. |
| `reply.instance-id` | hostname | Must be unique per instance and stable across restarts. The reply queue is durable and exclusive, so two instances sharing an id bind the same queue and the second silently receives nothing. |
| `request.ttl-matches-timeout` | `true` | Bounds how long an undelivered request can wait on the queue. It does **not** stop a replier that has already picked the request up: TTL is enforced by elapsed time alone, independent of delivery state — see [What actually reaches the DMQ](#what-actually-reaches-the-dmq). |
| `replier.provision.max-redelivery` | `3` | Zero means redeliver forever, so one malformed message loops indefinitely. |
| `java.reconnect-retries` | — | Set this to at least 100 with a 3000 ms wait, which gives the 300 seconds needed to survive an HA failover. The commonly copied value of 20 only gives 60 seconds. |
| `dmq.enabled` | `true` | On, because the alternative is deleting a message the system failed to process. Turning it off restores silent discard. |
| `replier.reply-ttl` | follows `request.timeout` | Bounds how long an undeliverable reply lingers. Set `0s` to keep replies forever, at the cost of orphaned queues growing. |

### The reply queue is durable, exclusive, and named after the instance

Each requestor instance owns one durable reply queue, `q.…reply.{instanceId}`, bound exclusively.
Exclusive because a reply is addressed rather than shared: the `CompletableFuture` waiting for it
lives in one JVM's heap and no other process can complete it.

That makes `instanceId` load-bearing, and it defaults to the hostname — the pod name on Kubernetes.
Two failure modes follow directly, and neither announces itself:

- **Two instances resolving the same id** bind the same exclusive queue. The second becomes a
  standby that receives nothing, and every one of its requests times out with no error logged. Set
  `reply.instance-id` explicitly when running more than one instance on a host.
- **An id that changes between runs** strands the previous queue on the broker, still spooling
  replies nobody will read. The hostname is stable across a restart, which is why the default no
  longer carries a random suffix.

The endpoint is logged at startup so the resolved value is never a guess:

```
Reply endpoint identity: instanceId=pod-0 queue=q.cris.booking.reply.pod-0
```

The queue is provisioned and subscribed before any flow binds, so a reply published in the gap
between startup and binding is spooled rather than lost.

**A replier-only process should not have one at all.** It consumes the shared request queue and
publishes each reply to the requestor's own `replyTo` topic, so nothing is ever addressed to a
reply queue of its own. Set `reply.enabled: false` there; it removes the reply endpoint, the
requestor-side template and the reply-path health indicator, and the process says so at startup.

### Provisioning modes

`replier.provision.mode` and `reply.provision-mode` both accept `CREATE_IF_MISSING` (the
default) and `OFF` — two modes, not three. A "validate but never create" mode looks appealing,
but JCSMP has no such call: `provision()` creates a missing endpoint unconditionally, flag or no
flag — verified against a live broker; see [spike/README.md](spike/README.md). Offering that
mode honestly would mean probing existence some other way before ever calling `provision()`,
which is machinery this library does not carry for a mode whose entire appeal was supposed to be
doing less, not more.

Creating on startup is safe to leave enabled because configuration drift is reported rather than
ignored, independently of that: `FLAG_IGNORE_ALREADY_EXISTS` only suppresses the "already exists"
error. If the queue exists with different properties, JCSMP raises `PropertyMismatchException`,
which names the property that differs — whether or not the queue had to be created.

`reply.provision-mode: OFF` is the requestor's equivalent of `replier.provision.mode: OFF`: on a
message VPN whose client profile forbids creating endpoints, the replier already had this escape
hatch and the requestor did not, simply failing to start. Because the reply queue is named per
instance rather than shared, using `OFF` here means provisioning one queue per instance identity
out of band in advance — practical when identities are known ahead of time, such as a
StatefulSet's stable ordinals.


### Dead message queues

A message that exhausts `replier.provision.max-redelivery`, or whose TTL expires, would otherwise be
**deleted** — a lost booking with no trace. Dead-lettering keeps it somewhere inspectable instead.
It is on by default, because the alternative is silent loss.

```yaml
solace:
  request-reply:
    dmq:
      enabled: true               # mark messages eligible and provision the queue
      name: "#DEAD_MSG_QUEUE"     # the Message VPN default, which every queue already points at
      provision: true             # create it at startup when missing
      quota-mb: 1000
    request:
      dmq-eligible: true          # flag on published requests
    replier:
      dmq-eligible: true          # flag on published replies
      reply-ttl:                  # unset: follow request.timeout. 0s disables expiry.
```

**One shared queue, deliberately.** Every queue's `deadMsgQueue` already points at
`#DEAD_MSG_QUEUE`, so using it needs no management credentials at all. Dead-lettered messages keep
their original topic, so `…/request/v1/…` and `…/reply/v1/…` remain easy to tell apart when you
inspect the queue.

**Using a different DMQ is a broker-side change, not a property.** `dmq.name` provisions and reports
the queue you name; it does not route to it. Which queue a message dead-letters into is
`deadMsgQueue` on the *source* queue, and JCSMP cannot set it — `EndpointProperties` has no such
property, so no client can. Set it yourself on the request queue and on every reply queue, then
point `dmq.name` at the same value so provisioning and diagnostics agree:

```bash
curl -u admin:admin -X PATCH \
  'http://localhost:8085/SEMP/v2/config/msgVpns/default/queues/q.cris.booking.seatReserve' \
  -H 'Content-Type: application/json' \
  -d '{"deadMsgQueue":"q.cris.booking.dmq"}'
```

A non-default `dmq.name` logs a warning at startup saying exactly this, because otherwise the gap is
invisible: the queue is created, diagnostics report `established: true`, and dead messages keep
going to the VPN default.

The DMQ is provisioned with `respectsMsgTTL=false`. These messages are here *because* they expired;
honouring their TTL again would expire them straight back out of the one place they are meant to
survive.

**Provisioning failure is a warning, not a crash.** Since this is on by default, a restricted client
profile or a DMQ owned by another team must not take the application down. You get one warning
naming the queue, and the behaviour that existed before the feature: dead messages are discarded.

### What actually reaches the DMQ

Less obvious than it looks, and worth knowing before you go hunting for a message that is not there.

| Situation | Dead-lettered? |
|---|---|
| Request expires on the queue with no replier consuming it | yes |
| Request already delivered to a replier, still waiting behind other work when its TTL elapses | yes — **the replier can go on to finish the work anyway** |
| Request redelivered past `max-redelivery` — a replier crashing before it acknowledges | yes |
| Reply published, requestor already gone, `reply-ttl` elapsed | yes |
| Handler throws `RetryableHandlerException` | after `max-redelivery` redeliveries — **yes**; before that, redelivered, not yet |
| Handler throws anything else | **no** — that becomes an error reply, and the request is acknowledged |
| Handler returns `null` (the demo's `simulate=timeout`) | **no** — it declines to reply but still acknowledges |

The second row is the one worth sitting with. Verified against a live broker: a message delivered
to a client and left unacknowledged — exactly what happens while it waits its turn in a busy
handler pool — is still dead-lettered by the broker at its TTL deadline. The broker does not check
delivery state before discarding its own copy, and it does not reach out to reclaim the client's;
it simply writes that client off and moves on. The client has no way to learn this happened —
acknowledging a message the broker has already dead-lettered raises no error at all — so a handler
can run to completion and produce a real side effect (a reserved seat, in the demo) for a request
that is simultaneously sitting in the DMQ looking exactly like a request that was never processed.

That matters most for recovery. **A DMQ entry is not proof the work never happened.** Anything that
replays one — an operator, a script — must reuse the *original* correlation id for
[the idempotency guard](#guaranteed-delivery-and-the-idempotency-that-completes-it) to recognize it
as a repeat; a replay that assigns a fresh id will not be caught, and produces a second, genuine
reservation for a booking that already succeeded.

The last two rows in the table are the other ones that surprise people. "The requestor timed out"
and "the request was dead-lettered" are different events: a handler that runs to completion and
simply says nothing has consumed the request quite legitimately, so there is nothing left to
dead-letter.

### Retrying a transient handler failure

Every exception a handler throws becomes an error reply by default — the requestor is told
immediately, and the request is acknowledged, because the work is considered done, badly. That is
right for a failure that will not change on a second attempt. It is wrong for one that might
succeed a moment later, such as a database connection blip: turning that into a permanent failure
is how a transient hiccup becomes a booking the customer is told never happened.

Throw `RetryableHandlerException` instead, and the request is settled `FAILED` rather than
acknowledged — an active request to the broker to redeliver, which is what actually triggers
another attempt; a merely-unacknowledged message only redelivers on the next reconnect. Exhausted
attempts dead-letter exactly like any other, per `replier.provision.max-redelivery`.

This does not make the original caller's call succeed — its own `request.timeout` still runs out
regardless of how many attempts follow. What it buys is that the work is not silently turned into
a permanent failure the moment a transient error is hit. It is also bounded by the request's own
TTL, which by default equals `request.timeout` via `ttl-matches-timeout`: a message can expire off
the queue mid-retry before `max-redelivery` is even reached. Raise `request.timeout` if a handler
needs real retry budget rather than one fast attempt.

### Broker version affects this

On brokers **10.25.10 and later**, *all* messages removed from a queue go to the DMQ, and a queue's
`respectDmqEligibleEnabled` restores the older behaviour. On **10.25.9 and earlier**, only messages
the publisher marked eligible are moved. Setting the flag is what makes behaviour the same on both,
which is why `dmq-eligible` exists rather than relying on the broker default.

One consequence worth stating: on a modern broker, `dmq.enabled=false` stops *this library* marking
messages and provisioning the queue, but if the DMQ already exists the broker may still move messages
into it. To make the publisher's flag authoritative there, set `respectDmqEligibleEnabled` on the
source queue over SEMP — this library does not, since it needs no management credentials otherwise.

### Reply TTL

Replies carry a TTL, defaulting to `request.timeout`. A reply is only useful to the one requestor
instance whose future is waiting; past that deadline no process can complete it, and without a TTL an
undeliverable reply would sit in the reply queue indefinitely — the orphaned-queue accumulation that
`DURABLE` reply endpoints are otherwise prone to.

It *derives* from `request.timeout` rather than defaulting to a fixed duration on purpose: a
hard-coded value would start expiring replies while requestors were still waiting the moment anyone
raised the timeout. Set `replier.reply-ttl` explicitly to override, or `0s` to disable expiry.

Expect a timed-out request to produce up to two DMQ entries — the expired request and its expired
reply. That is the intended troubleshooting signal, and the topics tell them apart.

### Inspecting it

```bash
curl -s -u admin:admin \
  'http://localhost:8085/SEMP/v2/monitor/msgVpns/default/queues/%23DEAD_MSG_QUEUE/msgs' | jq
```

`GET /api/diagnostics/endpoints` reports a `dmq` block with `configuredEnabled` and `established`
separately, because dead-lettering can be switched on and still be inert: a queue that could not be
created means the broker goes back to deleting.

---

## Coming from Spring Kafka

| Spring Kafka | This library |
|---|---|
| `ReplyingKafkaTemplate` | `ReplyingSolaceTemplate` |
| `sendAndReceive(...)` returning `RequestReplyFuture` | same names |
| `getSendFuture()` returning `SendResult` | `getSendFuture()` returning `PublishResult` |
| `@KafkaListener(topics = …)` | `@SolaceListener(queue = …, topics = …)` |
| consumer `groupId` | `queue`, since a non-exclusive queue is the consumer group |
| `@SendTo` | `@SendTo`, Spring's own annotation |
| `KafkaHeaders.CORRELATION_ID` | `SolaceHeaders.CORRELATION_ID`, a native SMF field |
| `spring.kafka.*` | `solace.java.*` and `solace.request-reply.*` |

### Differences that matter

| Concept | Kafka behaviour | Solace behaviour |
|---|---|---|
| Ordering | guaranteed within a partition | the queue preserves order; this library does not. `@SolaceListener(concurrency)` sizes this listener's own handler pool as well as its flow count, so replies can complete out of order the moment it is above 1 — even with a single flow. Set it to `1` for a listener that needs strict ordering; beyond that, give an ordered stream its own queue, since queues are cheap in Solace. |
| Provisioning | you provision topics | the reverse. Topics are just strings and need no setup, while queues are objects with permissions. |
| Replay | messages are retained by time or size, and you can seek | a queue drains as messages are acknowledged. Reprocessing needs the separate Message Replay feature. |
| Rebalancing | partitions are reassigned in a stop-the-world rebalance | messages are distributed one at a time across the bound flows. Adding an instance takes effect immediately, with no rebalance. |
| Filtering | topic names are flat, so consumers filter in the application | topics are hierarchical and the broker filters per message. |
| Dead letters | a client-side recoverer republishes to a `.DLT` topic | the broker moves messages to a dead message queue, with no client code involved. On by default here; see [Dead message queues](#dead-message-queues). |
| Queue browsing | not applicable | a queue can be browsed non-destructively, which is how you inspect the DMQ. |
| Acknowledgement | `AckMode.RECORD` and friends layer commit-after-processing over a raw `enable.auto.commit` that is really commit-on-a-timer | `@SolaceListener(ackMode = "CLIENT")` (the default) acks after processing, same idea as `AckMode.RECORD`; `AUTO` hands acking to JCSMP itself and gives up more than the name suggests — see [Acknowledgement modes: CLIENT and AUTO](#acknowledgement-modes-client-and-auto). |

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
| `ReplyPathReconnectIntegrationTest` | replies still arrive after the connection is cut from outside using SEMP, with no re-establish logic in play — which is what a durable, broker-side subscription buys. |
| `ProvisionDriftIntegrationTest` | re-provisioning with identical properties is a no-op; differing properties raise `PropertyMismatchException`; the ignore flag only suppresses "already exists" |
| `ReplierOnlyIntegrationTest` | with `reply.enabled=false` the context starts, has no reply endpoint or template, still binds the request queue, and provisions no reply queue on the broker |
| `MinimalConfigIntegrationTest` | the minimal configuration shown in this README actually round-trips, so the example cannot rot |
| `DmqIntegrationTest` | the DMQ is provisioned at startup; an expired request is kept there rather than deleted, carrying the published eligibility flag; reply TTL derives from `request.timeout` unless set |
| `ErrorHandlerRoutingIntegrationTest` | `@SolaceListener(errorHandler = "…")` selects a handler by bean name, and two handler beans in one context no longer break startup |

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
  endpoint/    durable reply endpoint, request queue and DMQ provisioners
  transport/   session, publisher with acknowledgement handling, flow consumer
  listener/    @SolaceListener discovery and container
  latency/     segment measurement, exact percentiles, histogram rendering

booking-demo/                  the runnable sample
docker/                        local broker
spike/                         the provisioning experiment and its result
```

## Licence

Apache 2.0.
