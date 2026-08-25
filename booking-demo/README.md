# Train Booking Demo — a guided walkthrough

A runnable train seat-reservation service built on the [`solace-request-reply-core`](../solace-request-reply-core)
library in this repository. It is the worked example: every claim the library makes is something
you can reproduce here with a `curl` and read back in a log line.

This document is a walkthrough. Follow it top to bottom and each step builds on the one before it.
Every command and every output below was captured from an actual run, not written from memory.

For the library's design and configuration reference, see the [root README](../README.md).

**Time required:** about 15 minutes for steps 1–8, another 10 for steps 9–12.

---

## What you are about to see

| Step | What it demonstrates |
|---|---|
| [1–3](#step-1-start-a-broker) | A guaranteed request/reply round trip end to end |
| [4](#step-4-read-the-reply-topic) | Per-request topic placeholders — the train number appears in the reply topic |
| [5](#step-5-ask-what-was-actually-provisioned) | What the library actually provisioned, versus what you configured |
| [6–7](#step-6-the-two-stage-future-publish-versus-reply) | The two-stage future: telling a publish failure apart from an unanswered request |
| [8](#step-8-the-double-booking-guard) | The double-booking guard under at-least-once delivery |
| [9](#step-9-nothing-is-lost-when-a-request-expires) | Dead-lettering: an expired request kept for inspection instead of deleted |
| [10–11](#step-10-split-the-two-sides-into-separate-processes) | Splitting requestor and replier, then scaling repliers out |
| [12](#step-12-the-reply-path-health-check) | The reply-path health check that catches a silent failure mode |
| [13](#step-13-measure-latency) | Exact latency percentiles with a segment breakdown |

---

## Prerequisites

- Docker
- JDK 17 or later
- `curl` and [`jq`](https://jqlang.github.io/jq/) (`jq` is only for readable output)

---

## Step 1: Start a broker

```bash
docker compose -f ../docker/docker-compose.yml up -d
```

Wait for it to report healthy. It takes about a minute:

```bash
docker inspect -f '{{.State.Health.Status}}' solace-rr
```

The Broker Manager UI is at http://localhost:8085 (`admin` / `admin`), which is worth keeping open
in a tab — you can watch the queues appear in step 2.

> **Port 55565, not 55555.** macOS reserves 55555, so the compose file publishes SMF on 55565.
> See [docker/README.md](../docker/README.md).

---

## Step 2: Build and run

From the repository root:

```bash
./mvnw -q -DskipTests install
java -jar booking-demo/target/booking-demo-0.1.0-SNAPSHOT.jar
```

The startup log is the most useful thing in this walkthrough, because it narrates everything the
library does before a single message is sent:

```
SolaceSession        : Solace session connected: host=tcp://localhost:55565 vpn=default
PersistentPublisher  : Publisher started, deliveryMode=PERSISTENT
AbstractReplyEndpoint: Reply endpoint prepared: kind=TEMPORARY
                       queue=#P2P/QTMP/v:rrdemo/q.cris.booking.reply.<host>-4c31ba46
                       subscription=cris/booking/seatReserve/reply/v1/nr/*/<host>-4c31ba46
FlowConsumer         : Flow 'reply' bound to queue '#P2P/QTMP/...' (ackMode=AUTO)
DefaultReplyingSolaceTemplate : Reply path verified: a probe published to
                       'cris/booking/seatReserve/reply/v1/nr/unknown/<host>-4c31ba46' came back
RequestQueueProvisioner : Request queue 'q.cris.booking.seatReserve' provisioned/verified:
                       accessType=NON_EXCLUSIVE quota=2000MB maxRedelivery=3 respectsTtl=true
RequestQueueProvisioner : Mapped topic 'cris/booking/seatReserve/request/v1/>' onto queue
FlowConsumer         : Flow 'seatReserve-0' bound to queue 'q.cris...' (ackMode=CLIENT)
FlowConsumer         : Flow 'seatReserve-1' ...  (four flows: concurrency=4)
SolaceMessageListenerContainer : Listener 'seatReserve' started: queue=q.cris.booking.seatReserve
                       concurrency=4 topics=[cris/booking/seatReserve/request/v1/>]
Started BookingDemoApplication in 2.145 seconds
```

Five things happened there, in order:

1. **The session connected.** Settings come from `solace.java.*`, the namespace the official
   `solace-java-spring-boot-starter` already binds. This library adds nothing there.
2. **A reply endpoint was created.** `TEMPORARY` by default, so nothing needs provisioning ahead of
   time and nothing is left behind. Its subscription wildcards the train-number level.
3. **The reply path was verified by a round-trip probe** — an actual message published to this
   instance's own reply topic that had to come back. This is `canary-on-reconnect`, and it is the
   only check that proves a message can complete the circuit.
4. **The request queue was provisioned** and the topic subscription mapped onto it.
5. **Four consumer flows bound** to that queue, from `concurrency: 4`.

Both sides of the conversation run in this one process, which is what lets a single `curl`
demonstrate a full round trip. Step 9 splits them.

---

## Step 3: Book a berth

```bash
curl -s -X POST http://localhost:8091/api/bookings \
  -H 'Content-Type: application/json' \
  -d '{"zone":"nr","trainNo":"12951","journeyDate":"2026-09-15",
       "seatClass":"AC3","passengerName":"A Sharma","passengers":2}' | jq
```

```json
{
  "reservation": {
    "pnr": "0841866636", "status": "CONFIRMED", "coach": "B3",
    "berths": "1,2", "trainNo": "12951", "replayed": false
  },
  "latency": { "totalMicros": 60194, "publishConfirmMicros": 13104 },
  "requestTopic": "cris/booking/seatReserve/request/v1/nr/12951",
  "partitionKey": "12951-2026-09-15-3a",
  "replyTopic": "cris/booking/seatReserve/reply/v1/nr/unknown/<host>-4c31ba46"
}
```

That single HTTP call went out to the broker as a persistent message, through a queue, into a
handler on another thread, back as a second persistent message, and completed a
`CompletableFuture` in this JVM — in 60 ms, most of which is first-call warmup. Repeat the call and
`totalMicros` drops to around 8,000.

Three fields are worth reading:

- **`publishConfirmMicros`** is how long the broker took to acknowledge the request as *spooled*.
  It is measured separately from the round trip because it answers a different question. See step 6.
- **`partitionKey`** is `train-date-class` — the contended inventory row. Not the request id.
  Too coarse creates hot partitions; too fine hashes randomly and silently reintroduces the race
  that partitioning exists to remove.
- **`replyTopic`** shows `unknown` in the train-number position because this is the *template*.
  The concrete per-request value is in step 4.

---

## Step 4: Read the reply topic

Look at the application log for the request you just sent:

```
SeatReservationListener : A Sharma train=12951 class=3a -> PNR 0841866636 CONFIRMED |
    replyTo=cris/booking/seatReserve/reply/v1/nr/12951/<host>-4c31ba46
```

Compare the two topics:

```
subscription   cris/booking/seatReserve/reply/v1/nr/*/<host>-4c31ba46
reply-to       cris/booking/seatReserve/reply/v1/nr/12951/<host>-4c31ba46
```

The requestor subscribes **once**, with a wildcard in the train-number position, and builds a
**concrete** reply-to per request. The replier never has to know how reply topics are structured —
it only echoes what it was given.

The payoff is that the train number is visible in the topic, so you can measure latency per train,
or tap one train during an incident, without parsing payloads.

#### How that is configured

The reply topic pattern holds three kinds of placeholder, and they resolve at different times:

```yaml
reply:
  topic-pattern: "cris/booking/seatReserve/reply/v1/{zone}/{trainNo}/{instanceId}"
  placeholders:
    zone: nr                       # static
  per-request-placeholders:
    - trainNo                      # wildcarded in the subscription...
  per-request-placeholder-expressions:
    trainNo: "trainNo()"           # ...and filled per publish from the payload
```

| Placeholder | Resolved | In the subscription | In each reply-to |
|---|---|---|---|
| `{zone}` | once at startup, from `placeholders` | `nr` | `nr` |
| `{instanceId}` | once at startup: hostname plus a random suffix | `<host>-4c31ba46` | `<host>-4c31ba46` |
| `{trainNo}` | on every publish, from the expression | `*` | `12951` |

The two `per-request-*` properties do different jobs, which is why you need both:

- **`per-request-placeholders`** is the list that makes a level a `*` in the subscription. Naming
  `trainNo` here is what lets one subscription cover every train.
- **`per-request-placeholder-expressions`** supplies the value for one publish. The expression is
  SpEL, parsed once at startup and evaluated with the **request payload as the root object** — so
  `trainNo()` invokes `BookingRequest.trainNo()`. Anything valid against the payload works:
  a getter, a field, `seatClass().code()`, a literal. The result is carried to the publish on an
  internal `rr_rt_<name>` header. (With the raw `sendAndReceive(topic, RequestReplyMessage, timeout)`
  API you can skip expressions and set a header named `<name>` yourself.)

Two failure modes are worth knowing, and they are deliberately different in severity:

- **A name listed with no expression**, or an expression that throws or returns null, renders that
  level as `unknown` and logs a warning. The subscription still matches, because the level is
  wildcarded — so you lose the observability, not the reply. Non-fatal on purpose.
- **A `{placeholder}` in neither map** fails at startup, naming the placeholder and telling you to
  either give it a static value or declare it per-request. Fatal on purpose: it would otherwise build
  a subscription containing a literal `{name}` that matches nothing, and every request would time out.

See [application.yml](src/main/resources/application.yml) for the values this demo uses.

This is also the replacement for a JMS selector such as `hostname = '${HOSTNAME}'`. The
discriminator lives in the topic, so the broker sends only what this instance asked for instead of
evaluating a selector against every message.

---

## Step 5: Ask what was actually provisioned

With `provision.mode=CREATE_IF_MISSING`, "did my configuration take effect?" is a real question.

```bash
curl -s http://localhost:8091/api/diagnostics/endpoints | jq
```

```json
{
  "session":  { "connected": true, "lastEvent": "CONNECTED", "reconnects": 0 },
  "replyEndpoint": {
    "type": "TEMPORARY", "established": true,
    "subscription": "cris/booking/seatReserve/reply/v1/nr/*/<host>-4c31ba46",
    "perRequestPlaceholders": ["trainNo"], "recreateOnReconnect": true
  },
  "requestQueue": {
    "queue": "q.cris.booking.seatReserve", "accessType": "NON_EXCLUSIVE",
    "concurrency": 4, "provisionMode": "CREATE_IF_MISSING",
    "maxRedelivery": 3, "respectsTtl": true,
    "partitionCount": 0, "partitioned": false
  },
  "inFlight":  { "pendingRequests": 0, "distinctReservations": 2 },
  "tracing":   { "configuredEnabled": false, "active": false }
}
```

`tracing` reports `configuredEnabled` and `active` separately on purpose: tracing can be switched on
and still be inert because the OpenTelemetry libraries are not on the classpath. A single boolean
would hide that.

---

## Step 6: The two-stage future — publish versus reply

A request/reply call fails in two independent ways. The request may never reach the broker, or it may
reach the broker and go unanswered. One future cannot express both, so both surface as the same
timeout and point you at the wrong half of the system.

```java
RequestReplyFuture<SeatReservation> f =
        template.sendAndReceive(topic, key, req, SeatReservation.class);

f.getSendFuture().get(5, SECONDS);      // stage 1: the broker has spooled the request
SeatReservation r = f.get(5, SECONDS);  // stage 2: a replier answered
```

Force an unanswered request. The `simulate` field is a test hook on the request body, so you can
reproduce each failure mode without breaking the broker:

```bash
curl -s -X POST http://localhost:8091/api/bookings \
  -H 'Content-Type: application/json' \
  -d '{"zone":"nr","trainNo":"12951","journeyDate":"2026-09-15","seatClass":"AC3",
       "passengerName":"A Sharma","passengers":1,"simulate":"timeout"}' | jq
```

```json
{
  "error": "reply-timeout",
  "detail": "The request was spooled but no replier answered in time",
  "requestTopic": "cris/booking/seatReserve/request/v1/nr/12951",
  "publishConfirmed": true,
  "publishConfirmMicros": 9981
}
```

HTTP 504, and — the point of the exercise — **`publishConfirmed` is `true`**. The message reached the
broker in 10 ms and is sitting on the queue. Nobody answered. That tells you to look at repliers, not
at connectivity. Had the publish itself been rejected (full spool, missing permission), you would get
`"error": "publish-failed"` with `publishConfirmed: false` instead, and you would look somewhere else
entirely.

You can ignore `getSendFuture()` and get ordinary single-outcome behaviour. It is there for when the
distinction matters.

---

## Step 7: A remote error

A handler that throws produces an error reply, not a timeout:

```bash
curl -s -X POST http://localhost:8091/api/bookings \
  -H 'Content-Type: application/json' \
  -d '{"zone":"nr","trainNo":"12951","journeyDate":"2026-09-15","seatClass":"AC3",
       "passengerName":"A Sharma","passengers":1,"simulate":"remote-error"}' | jq
```

```json
{
  "error": "remote-error",
  "detail": "No berths available on train 12951 (simulated)",
  "publishConfirmed": true,
  "publishConfirmMicros": 5032
}
```

HTTP 422, and it comes back immediately rather than after the 5-second timeout. The replier's
exception message crossed the wire as a `RemoteErrorException`. Three failure modes, three distinct
HTTP statuses, three distinct causes.

---

## Step 8: The double-booking guard

Guaranteed messaging is **at-least-once, not exactly-once**. On a non-exclusive queue, an
unacknowledged message is redelivered to another consumer. A replier that reserves a seat and then
dies before acknowledging will see the same request again — and a naive handler reserves a second
seat.

`POST /api/bookings/replay` sends a request under a correlation id you choose, which is how you
reproduce a redelivery on demand.

```bash
CID="demo-$(date +%s)"
BODY='{"zone":"nr","trainNo":"12951","journeyDate":"2026-09-15",
       "seatClass":"AC3","passengerName":"R Iyer","passengers":1}'

curl -s -X POST "http://localhost:8091/api/bookings/replay?correlationId=$CID" \
  -H 'Content-Type: application/json' -d "$BODY" | jq '.reservation'

curl -s -X POST "http://localhost:8091/api/bookings/replay?correlationId=$CID" \
  -H 'Content-Type: application/json' -d "$BODY" | jq '.reservation'
```

```json
{ "pnr": "0841866638", "status": "CONFIRMED", "coach": "B3", "berths": "3", "replayed": false }
{ "pnr": "0841866638", "status": "CONFIRMED", "coach": "B3", "berths": "3", "replayed": true }
```

**Same PNR, same berth, `replayed: true`.** One seat was reserved, and the second delivery returned
the original answer instead of doing the work again.

Two mechanisms produce that, both in [SeatInventoryService.java](src/main/java/com/solace/samples/booking/replier/SeatInventoryService.java):

1. **Idempotent handling.** The correlation id is recorded *with* the reservation, and a repeat
   returns the stored reply. Here that is a `ConcurrentHashMap`; in a real service the reservation and
   that record are one database transaction under a unique constraint.
2. **Acknowledge last.** The replier processes the request, publishes the reply, waits for the broker
   to confirm the reply is spooled, and only then acknowledges the request (`ackMode = "CLIENT"`).
   Acknowledging first would mean a crash in between loses the request after the work was already
   done — the seat taken, the customer told it failed, and nothing left to redeliver.

---

## Step 9: Nothing is lost when a request expires

Guaranteed delivery keeps a request safe right up to the point the broker gives up on it. When a
request exhausts its redeliveries, or its TTL expires, the broker's choice is to **delete** it or to
move it to a dead message queue. Deleting means a booking vanishes with nothing anywhere recording
that it existed, so the demo enables dead-lettering.

You need a request that nobody consumes — so start the demo with the replier switched off. (Step 10
covers the profiles properly; here it is just a way to leave the request queue unattended.)

```bash
# Stop the running demo first.
java -jar booking-demo/target/booking-demo-0.1.0-SNAPSHOT.jar \
  --spring.profiles.active=requestor &
```

At startup the DMQ is provisioned:

```
DmqProvisioner : Dead message queue '#DEAD_MSG_QUEUE' provisioned/verified: quota=1000MB respectsTtl=false
```

`respectsTtl=false` on that queue is deliberate. These messages arrive *because* they expired;
honouring their TTL a second time would expire them straight back out of the one place they are
meant to survive.

Now send a booking nothing will handle. It returns 504 after the 5s timeout, exactly as in step 6:

```bash
curl -s -X POST http://localhost:8091/api/bookings -H 'Content-Type: application/json' \
  -d '{"zone":"nr","trainNo":"19999","journeyDate":"2026-12-25","seatClass":"AC1",
       "passengerName":"Dead Letter Test","passengers":1}' | jq '{error, publishConfirmed}'
```

The difference is what is left behind. Wait a few seconds, then look in the DMQ:

```bash
curl -s -u admin:admin \
  'http://localhost:8085/SEMP/v2/monitor/msgVpns/default/queues/%23DEAD_MSG_QUEUE/msgs' \
  | jq -r '.data[] | "dmqEligibleAsPublished=\(.dmqEligibleAsPublished) ttl=\(.ttl) redeliveryCount=\(.redeliveryCount)"'
```

```
dmqEligibleAsPublished=true ttl=5000 redeliveryCount=0
```

Three things to read there. `dmqEligibleAsPublished=true` is the flag the requestor set, which is
what makes this work on brokers before 10.25.10. `ttl=5000` is the request timeout, applied by
`ttl-matches-timeout`. `redeliveryCount=0` says it expired rather than being retried to exhaustion —
nothing ever picked it up.

The request queue is now empty: the message was **moved**, not copied.

```bash
curl -s -u admin:admin \
  'http://localhost:8085/SEMP/v2/monitor/msgVpns/default/queues/q.cris.booking.seatReserve/msgs' \
  | jq '.data | length'      # 0
```

### What does not go to the DMQ

This is the part worth internalising, because a message you expect to find and cannot is confusing.

| Situation | Dead-lettered? |
|---|---|
| Request expires with nothing consuming it — what you just did | yes |
| Replier crashes before acknowledging, past `max-redelivery` | yes |
| Reply published after the requestor has gone, once `reply-ttl` elapses | yes |
| `"simulate":"remote-error"` — the handler throws | **no** — it becomes an error reply, and the request is acknowledged |
| `"simulate":"timeout"` — the handler returns `null` | **no** — it declines to reply but still acknowledges |
| The reply-path canary from step 12 | **no** — deliberately ineligible, or every reconnect would leave one |

"The requestor timed out" and "the request was dead-lettered" are different events. `simulate=timeout`
produces the first without the second: a handler that runs to completion and simply says nothing has
consumed the request perfectly legitimately, so there is nothing left to dead-letter.

Replies carry a TTL too, defaulting to `request.timeout` — visible as `replyTtlMillis` in
`/api/diagnostics/endpoints`, alongside whether the DMQ was actually established:

```bash
curl -s http://localhost:8091/api/diagnostics/endpoints | jq '.dmq'
```

```json
{
  "configuredEnabled": true, "established": true,
  "queue": "#DEAD_MSG_QUEUE", "detail": "provisioned/verified",
  "requestsEligible": true, "repliesEligible": true, "replyTtlMillis": 5000
}
```

`configuredEnabled` and `established` are separate for the same reason they are under `tracing`:
dead-lettering can be switched on and still be inert, because a DMQ that could not be created means
the broker goes back to deleting.

---

## Step 10: Split the two sides into separate processes

Nothing about the demo requires both sides in one JVM. They are independent beans over separate
queues, so splitting them needs no code change — only profiles.

> **On client names.** A Message VPN permits one client per name, so instances that share a name
> evict each other and reconnect in a loop. [application.yml](src/main/resources/application.yml)
> therefore gives every process a unique one by default —
> `${SOLACE_CLIENT_NAME:${HOSTNAME:booking-demo}-${random.int[1000,9999]}}` — so the commands below
> just work. Override it with `--solace.java.client-name=` or `SOLACE_CLIENT_NAME` when you want a
> name you chose in the broker's client list; the examples do so purely for readability.

```bash
JAR=booking-demo/target/booking-demo-0.1.0-SNAPSHOT.jar

# Replier only: the listener runs, no bookings are sent from here.
# --solace.java.client-name is optional; it just makes this process easy to spot on the broker.
java -jar $JAR --spring.profiles.active=replier \
  --server.port=8092 --solace.java.client-name=replier-1 &

# Requestor only: the listener is disabled, so this process cannot answer its own requests.
java -jar $JAR --spring.profiles.active=requestor \
  --server.port=8091 --solace.java.client-name=requestor-1 &
```

Confirm the requestor really has no listener — this prints nothing:

```bash
# In the requestor's log: no "Listener 'seatReserve' started" line appears.
```

Now book through the requestor:

```bash
curl -s -X POST http://localhost:8091/api/bookings -H 'Content-Type: application/json' \
  -d '{"zone":"nr","trainNo":"12621","journeyDate":"2026-10-02","seatClass":"AC2",
       "passengerName":"S Nair","passengers":1}' | jq '.reservation'
```

```json
{ "pnr": "0571124365", "status": "CONFIRMED", "coach": "B1", "berths": "1", "trainNo": "12621" }
```

And in the **replier's** log, on port 8092:

```
SeatReservationListener : S Nair train=12621 class=2a -> PNR 0571124365 CONFIRMED |
    replyTo=cris/booking/seatReserve/reply/v1/nr/12621/<host>-2c86810f
```

Two processes, one round trip. The reply found its way back to the specific requestor JVM whose heap
holds the waiting future, because the reply topic names that instance.

---

## Step 11: Scale the repliers out

Add a second replier against the same queue:

```bash
java -jar $JAR --spring.profiles.active=replier \
  --server.port=8093 --solace.java.client-name=replier-2 &

for i in $(seq 1 12); do
  curl -s -o /dev/null -X POST http://localhost:8091/api/bookings \
    -H 'Content-Type: application/json' \
    -d "{\"zone\":\"nr\",\"trainNo\":\"1230$((i%3))\",\"journeyDate\":\"2026-11-0$((i%9+1))\",
         \"seatClass\":\"SLEEPER\",\"passengerName\":\"P$i\",\"passengers\":1}"
done
```

Count the handled requests in each replier's log. In the captured run, 13 requests split **9 / 4**
across the two processes.

They competed for messages on one shared non-exclusive queue — that queue *is* the consumer group, the
Solace equivalent of a Kafka `groupId`. No rebalance, no partition reassignment, no restart: the new
instance began taking work the moment its flow bound.

---

## Step 12: The reply-path health check

There is a failure mode worth knowing about. A temporary reply queue survives a disconnect for 60
seconds, or 180 across a failover. Past that the broker destroys it — and when the client reconnects,
the broker recreates the queue **without its topic subscription**.

In that state the session is connected, the flow is bound, the queue exists, nothing is logged, and
every request times out until the process is restarted.

```bash
curl -s http://localhost:8091/api/diagnostics/reply-path | jq
```

```json
{
  "established": true, "ready": true,
  "subscription": "cris/booking/seatReserve/reply/v1/nr/*/<host>-4c31ba46",
  "verdict": "reply path is bound and subscribed"
}
```

The same verdict is wired into Spring Boot's health endpoint, so a readiness probe can remove an
instance that cannot answer:

```bash
curl -s http://localhost:8091/actuator/health | jq '.components.solaceReplyPath'
```

```json
{
  "status": "UP",
  "details": {
    "sessionConnected": true, "reconnects": 0,
    "replyEndpointEstablished": true, "replyPathVerified": true,
    "replyPathDetail": "verified by a round-trip probe"
  }
}
```

`recreate-on-reconnect` redoes the sequence after a reconnect, and `canary-on-reconnect` then proves
it with a real message. Between them the failure is handled and, more importantly, *visible* — which
is what makes `TEMPORARY` a perfectly sound production choice, and a low-maintenance one: nothing to
provision, nothing left behind, no queue lifecycle to own.

`DURABLE` is the alternative, not the upgrade. It trades that zero maintenance for a broker-side
subscription that survives on its own, and for a queue that keeps spooling replies while the
requestor is disconnected instead of discarding them. Pick it when you want replies to survive a
requestor outage, or when your operational model prefers endpoints declared up front; stay on
`TEMPORARY` when you would rather own no endpoint at all.

---

## Step 13: Measure latency

One command runs a test, prints a report, and exits. No metrics backend required.

```bash
java -jar booking-demo/target/booking-demo-0.1.0-SNAPSHOT.jar \
  --spring.profiles.active=loadtest --solace.java.client-name=loadtest-1 \
  --loadtest.count=2000 --loadtest.concurrency=32 --loadtest.warmup=200
```

```
Seat reservation latency
2,000 requests · concurrency 32 · CLOSED_LOOP · 200 warmup discarded
completed in 4.8s · 414 req/s

OUTCOMES
  success                 2,000  100.00%
  timeout                     0    0.00%
  remote_error                0    0.00%
  publish_failure             0    0.00%

TOTAL ROUND TRIP
       p50       7.2 ms
       p99      10.7 ms
     p99.9      37.2 ms
       max      43.2 ms

DISTRIBUTION
      4 -     8 ms     1,703  ██████████████████████████████████
      8 -    16 ms       291  █████▊
     16 -    32 ms         3  ▏
     32 -    64 ms         3  ▏

SEGMENTS                      p50        p99
  publish confirm          3.5 ms     5.6 ms
  queue dwell              3.5 ms     5.8 ms
  handler                  0.1 ms     0.2 ms
  dispatch delay           0.0 ms     0.0 ms

ORDERING
  sequence gaps                   0   no message loss
  out-of-order                    0   in order
```

The **segment breakdown** is the part that tells you what to do next. A p99 of 10.7 ms says little on
its own; knowing that 5.8 ms of it was queue dwell points at adding repliers, whereas the same figure
under `handler` would point at the database.

Percentiles are exact, not interpolated from buckets — a bounded test run means every sample can be
kept. Buckets double each row because latency distributions have long tails.

The report names its mode, because it changes the meaning. `CLOSED_LOOP` keeps a fixed number of
requests in flight, so a slowdown makes the generator send *less* and the tail flatters reality. For
latency at a fixed arrival rate:

```bash
--loadtest.mode=OPEN_LOOP --loadtest.rate=500
```

You can also measure ad-hoc traffic against a running instance, using the same report code:

```bash
curl -s -X POST http://localhost:8091/api/latency/start
# ... drive whatever traffic you like ...
curl -s -X POST "http://localhost:8091/api/latency/report?concurrency=1" | jq
```

---

## Cleanup

```bash
# Stop any demo JVMs you started, then:
docker compose -f ../docker/docker-compose.yml down -v
```

`-v` removes the message spool as well, so the next run starts from a clean broker.

---

## How the demo uses the core library

The demo depends only on `solace-request-reply-core`. There is **no** `@EnableSolaceRequestReply` and
no manual bean wiring: the library ships a Spring Boot auto-configuration, so adding the dependency
and setting `solace.request-reply.*` properties is the whole integration.

The entire surface used by this demo is four things:

| Library API | Used by | Role |
|---|---|---|
| `ReplyingSolaceTemplate.sendAndReceive(...)` | [BookingController](src/main/java/com/solace/samples/booking/web/BookingController.java), [LoadTestRunner](src/main/java/com/solace/samples/booking/loadtest/LoadTestRunner.java) | Requestor side. Returns a `RequestReplyFuture`. |
| `RequestReplyFuture.getSendFuture()` | [BookingController](src/main/java/com/solace/samples/booking/web/BookingController.java) | Separates "the broker took it" from "somebody answered". |
| `@SolaceListener` + `@SendTo` | [SeatReservationListener](src/main/java/com/solace/samples/booking/replier/SeatReservationListener.java) | Replier side. Bare `@SendTo` replies to the request's reply-to. |
| `SolaceHeaders.CORRELATION_ID` | [SeatReservationListener](src/main/java/com/solace/samples/booking/replier/SeatReservationListener.java) | The idempotency key, a native SMF field. |

Everything else — provisioning the request queue, creating and re-establishing the reply endpoint,
correlating replies, timing out and evicting abandoned requests, measuring segments, acknowledging
after the reply is spooled — is the library's job and needs no application code.

If you have used Spring Kafka, the shapes are deliberately familiar:

```java
// Requestor
RequestReplyFuture<SeatReservation> f = template.sendAndReceive(
        topic, request.partitionKey(), request, SeatReservation.class);

// Replier
@SolaceListener(queue = "q.cris.booking.seatReserve",
                topics = "cris/booking/seatReserve/request/v1/>",
                concurrency = "4", ackMode = "CLIENT")
@SendTo
public SeatReservation reserve(@Payload BookingRequest req,
                               @Header(SolaceHeaders.CORRELATION_ID) String correlationId) {
    return inventory.reserveOnce(correlationId, req);
}
```

The `partitionKey` argument plays exactly the role a Kafka record key plays. A flat queue ignores it,
so it is safe — and recommended — to always supply one: turning on partitioning later then needs no
application change.

### Source map

```
src/main/java/com/solace/samples/booking/
  BookingDemoApplication.java     plain @SpringBootApplication; the library auto-configures
  domain/
    BookingRequest.java           request; partitionKey() is the contended inventory row
    SeatReservation.java          reply; `replayed` is the visible redelivery signal
    SeatClass.java                AC1/AC2/AC3/SLEEPER/... with topic-level codes
  web/
    BookingController.java        REST facade; the two-stage future and the failure taxonomy
    DiagnosticsController.java    what was provisioned; reply-path probe
    LoadTestController.java       ad-hoc latency start/report
  replier/
    SeatReservationListener.java  @SolaceListener + @SendTo
    SeatInventoryService.java     idempotency by correlation id; per-row locking
  loadtest/
    LoadTestRunner.java           standalone harness; closed and open loop
src/main/resources/application.yml   the worked configuration, commented throughout
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Endless `RECONNECTING` / `Channel is closed by peer`, no bookings complete | Two instances pinned to the same `client-name`; a VPN permits one client per name. The default name is unique, so this only happens when you override it | Give each process a distinct `--solace.java.client-name=`, or drop the override and let the default apply |
| Connection refused on startup | Broker not healthy yet | `docker inspect -f '{{.State.Health.Status}}' solace-rr` until `healthy` |
| Every request returns `reply-timeout`, `publishConfirmed: true` | No replier is bound to the request queue | Check for `Listener 'seatReserve' started` in a replier log; the `requestor` profile disables it |
| Port 55555 fails to bind | macOS reserves it | Already handled — the compose file uses 55565 |
| `Address already in use` on 8091 | An earlier demo JVM is still running | Stop it, or pass `--server.port=` |
| Startup fails with `PropertyMismatchException` | The queue exists with different properties than configured | Delete the queue in Broker Manager, or align the config. The exception names the property. |

---

## Where to go next

- [Root README](../README.md) — the library's design, topic taxonomy, and the Spring Kafka comparison
- [docs/configuration-reference.yml](../docs/configuration-reference.yml) — every property, with defaults
- [application.yml](src/main/resources/application.yml) — this demo's working configuration
- [spike/README.md](../spike/README.md) — the provisioning-drift experiment and its result

> Sample code, not an officially supported Solace product. If you adopt it, copy it into your own
> package and take ownership of it.
