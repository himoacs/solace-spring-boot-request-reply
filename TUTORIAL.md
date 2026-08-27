# Build your own request/reply service

A step-by-step guide to using [`solace-request-reply-core`](solace-request-reply-core) in your
own Spring Boot application. By the end you'll have a requestor that sends a message and gets a
typed reply back, and a replier that answers it — the same round trip the
[booking demo](booking-demo/README.md) shows running, built up piece by piece so you can see
exactly where each piece comes from.

This guide writes the code; it doesn't explain *why* the library is shaped the way it is. For that,
see the [root README](README.md) — it's linked from every step below where it's relevant.

**Time required:** about 15 minutes.

---

## Prerequisites

- Java 17+ and Maven
- A Solace broker to connect to. The quickest way is the one this repo already provides:
  ```bash
  docker compose -f docker/docker-compose.yml up -d
  ```
  See [booking-demo/README.md, Step 1](booking-demo/README.md#step-1-start-a-broker) for details
  and troubleshooting.
- The library itself, built locally. It isn't published to Maven Central yet (see
  [root README §7](README.md#7-known-gaps-and-future-work)), so from the repository root:
  ```bash
  ./mvnw -q -DskipTests install
  ```
  This installs `solace-request-reply-core` into your local `~/.m2` repository, where your own
  project's build can find it.

---

## Step 1: Add the dependency

```xml
<dependency>
    <groupId>com.solace.samples</groupId>
    <artifactId>solace-request-reply-core</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

That's the only dependency the library needs you to add. It pulls in the official
`solace-java-spring-boot-starter` itself, so you don't declare that separately. If your app exposes
a REST endpoint the way this guide's example does, also add `spring-boot-starter-web` — that's an
application choice, not something the library requires.

The library ships a Spring Boot auto-configuration, so there's no `@Enable…` annotation to add
anywhere. Adding the dependency and setting the properties in Step 6 is the whole integration.

---

## Step 2: Connect to a broker

Connection settings belong to the official starter, under `solace.java.*` — this library doesn't
add its own connection layer or reinvent this configuration:

```yaml
solace:
  java:
    host: tcp://localhost:55565
    msg-vpn: default
    client-username: default
    client-password: default
```

These are the defaults for the broker you started in the prerequisites. Leave this step here for
now — it joins the rest of the configuration in Step 6.

---

## Step 3: Define your request and reply types

The library serializes and deserializes plain Java objects as JSON — there's no message-building
API to learn. A request and a reply, as records:

```java
public record BookingRequest(
        String trainNo,
        String journeyDate,
        String seatClass,
        String passengerName,
        int passengers) {
}
```

```java
public record SeatReservation(
        String pnr,
        String status,
        String coach,
        String berths) {
}
```

(The booking demo's actual
[`BookingRequest`](booking-demo/src/main/java/com/solace/samples/booking/domain/BookingRequest.java)
and
[`SeatReservation`](booking-demo/src/main/java/com/solace/samples/booking/domain/SeatReservation.java)
add a few more fields — a `zone` used for topic routing, a `replayed` flag, validation annotations
— none of which change the shape of what follows. Start with the plain version above and grow it
later.)

---

## Step 4: Write the requestor

Inject `ReplyingSolaceTemplate` and call `sendAndReceive(topic, payload, replyType)`. It publishes
the request and gives you back a `RequestReplyFuture` — a `CompletableFuture` that completes once
the reply arrives:

```java
@RestController
@RequestMapping("/api/bookings")
public class BookingController {

    private final ReplyingSolaceTemplate template;

    public BookingController(ReplyingSolaceTemplate template) {
        this.template = template;
    }

    @PostMapping
    public SeatReservation book(@RequestBody BookingRequest request) throws Exception {
        RequestReplyFuture<SeatReservation> future = template.sendAndReceive(
                "rail/booking/seatReserve/request/v1", request, SeatReservation.class);
        return future.get(10, TimeUnit.SECONDS);
    }
}
```

`future.get(...)` blocks the HTTP thread until either the reply arrives or the timeout you pass
elapses. If it fails, the exception tells you what kind of failure it was:

- **`RequestTimeoutException`** (wrapped in `ExecutionException`) — nobody answered in time.
- **`RemoteErrorException`** — a replier received the request and its handler threw.
- **`TransportException`** — the broker didn't accept the publish in the first place.

Three different problems, three different exceptions — worth catching separately rather than
treating every failure the same way. The demo's real
[`BookingController`](booking-demo/src/main/java/com/solace/samples/booking/web/BookingController.java)
goes one step further and awaits `future.getSendFuture()` on its own, so it can tell "the broker
never even took the request" apart from "the broker took it and nobody answered" — see
[booking-demo/README.md, Step 6](booking-demo/README.md#step-6-the-two-stage-future-publish-versus-reply)
for why that distinction is worth the extra code.

---

## Step 5: Write the replier

A replier is a method annotated `@SolaceListener`, bound to a queue and one or more topics, with
`@SendTo` telling the library to publish the return value back to whoever asked:

```java
@Component
public class SeatReservationListener {

    private final SeatInventoryService inventory;

    public SeatReservationListener(SeatInventoryService inventory) {
        this.inventory = inventory;
    }

    @SolaceListener(
            queue = "q.rail.booking.seatReserve",
            topics = "rail/booking/seatReserve/request/v1")
    @SendTo
    public SeatReservation reserve(@Payload BookingRequest request,
                                   @Header(SolaceHeaders.CORRELATION_ID) String correlationId) {
        return inventory.reserveOnce(correlationId, request);
    }
}
```

A few things worth knowing right away:

- **The queue is what makes scaling safe.** Run three copies of this process and all three bind the
  same queue; the broker hands each request to exactly one of them. See
  [root README §3.3](README.md#33-request-queues-vs-reply-queues).
- **`@SendTo` with no value replies to the request's own reply-to destination.** You never hard-code
  a reply topic — the library derives it per request. See
  [root README §3.2](README.md#32-topics-wildcards-and-topic-to-queue-mapping).
- **Throwing an exception here becomes an error reply**, and the caller's `future.get()` fails with
  `RemoteErrorException` right away instead of waiting out its timeout. See
  [root README §3.4](README.md#34-ttls-redeliveries-and-timeouts) for the default failure
  behavior, and when to reach for `RetryableHandlerException` instead.

---

## Step 6: Make the replier idempotent

Guaranteed delivery is *at-least-once*: if this process reserves a seat and crashes before
acknowledging the request, the broker redelivers the same request to another instance. Without
guarding against it, that's a seat reserved twice for one booking.

The fix is to key on the correlation id — the same id the library already handed you in
Step 5 — and only do the work once per id:

```java
@Service
public class SeatInventoryService {

    private final Map<String, SeatReservation> byCorrelationId = new ConcurrentHashMap<>();

    public SeatReservation reserveOnce(String correlationId, BookingRequest request) {
        return byCorrelationId.computeIfAbsent(correlationId, id -> reserve(request));
    }

    private SeatReservation reserve(BookingRequest request) {
        // your actual seat-allotment logic goes here
    }
}
```

`computeIfAbsent` is what makes this safe even if two threads see the same redelivered request at
once — only one of them actually runs `reserve(...)`. A real service would back this with a
database row and a unique constraint on the correlation id rather than an in-memory map, so the
guard survives a restart. See
[root README §3.1](README.md#31-correlation-id) for why the correlation id is the right key to
use, and the demo's own
[`SeatInventoryService`](booking-demo/src/main/java/com/solace/samples/booking/replier/SeatInventoryService.java)
for a version that also locks per train/date/class so two concurrent bookings for the same train
don't race each other.

---

## Step 7: Put the configuration together

Everything the library itself needs, alongside the connection settings from Step 2:

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
      topic-pattern: "rail/booking/seatReserve/reply/v1/{instanceId}"
      queue-name-pattern: "q.rail.booking.reply.{instanceId}"
    replier:
      queue: q.rail.booking.seatReserve
      topics:
        - "rail/booking/seatReserve/request/v1"
```

Everything else — queue provisioning mode, dead-lettering, redelivery limits — has a default chosen
to work against a fresh broker with nothing set up by hand. `docs/configuration-reference.yml` in
the repository root lists every property if you want to see what's available beyond this minimal
set.

---

## Step 8: Run it and confirm the round trip

Start your application, then send a request:

```bash
curl -s -X POST http://localhost:8080/api/bookings \
  -H 'Content-Type: application/json' \
  -d '{"trainNo":"12951","journeyDate":"2026-09-15","seatClass":"AC3",
       "passengerName":"A Sharma","passengers":2}'
```

You should get the reply back as the HTTP response body, something like:

```json
{"pnr":"0841866636","status":"CONFIRMED","coach":"B3","berths":"1,2"}
```

If instead the call hangs and returns a timeout, check that a replier is actually running and bound
to `q.rail.booking.seatReserve` — the requestor and replier can be the same process or two separate
ones; nothing about the code above cares which.

---

## Where to go next

You now have a working round trip. Everything past this point is about making it production-shaped
rather than making it work at all:

| Topic | Where to read about it |
|---|---|
| Telling a rejected publish apart from an unanswered request | [root README §2](README.md#2-how-the-library-is-built), [booking-demo/README.md Step 6](booking-demo/README.md#step-6-the-two-stage-future-publish-versus-reply) |
| Retrying a transient failure instead of failing permanently | [root README §3.4](README.md#34-ttls-redeliveries-and-timeouts) |
| `CLIENT` vs. `AUTO` acknowledgement | [root README §3.5](README.md#35-acknowledgements) |
| Dead message queues — what gets kept, what doesn't | [root README §3.6](README.md#36-dead-message-queues) |
| Running requestor and replier as separate, independently-scaled processes | [booking-demo/README.md Steps 9–10](booking-demo/README.md#step-9-split-the-two-sides-into-separate-processes) |
| Routing replies per request (e.g. one topic level per train) instead of one flat reply topic | [root README §3.2](README.md#32-topics-wildcards-and-topic-to-queue-mapping) |
| Every configuration property, with defaults | [docs/configuration-reference.yml](docs/configuration-reference.yml) |

> Sample code, not an officially supported Solace product. If you adopt it, copy it into your own
> package and take ownership of it.
