# Running the demo on Kubernetes (minikube)

The application runs in the cluster; the Solace broker stays on the laptop. That split is the
point of this setup — it exercises the parts that behave differently under Kubernetes (pod
identity, rolling updates, scaling) without putting a broker in the way.

Verified against minikube v1.33.1 with the docker driver, 4 CPUs / 8 GB.

---

## Reaching the laptop broker from a pod

minikube writes `host.minikube.internal` into the **node's** `/etc/hosts`, not into pod DNS, so a
pod cannot resolve that name. Find the address and put it in the ConfigMap:

```bash
minikube ssh -- grep host.minikube.internal /etc/hosts
# 192.168.65.254  host.minikube.internal
```

That value lives in [00-config.yaml](00-config.yaml) as `SOLACE_HOST`. It is stable for a given
minikube installation but not across machines, so it is the one thing to check first if pods start
and then fail to connect.

## Deploy

```bash
# 1. Broker on the laptop, as usual.
docker compose -f ../docker/docker-compose.yml up -d

# 2. Build the jar, then bake it into an image inside minikube's own daemon.
cd .. && ./mvnw -q -DskipTests install
minikube image build -t booking-demo:local -f k8s/Dockerfile .

# 3. Deploy.
kubectl apply -f k8s/00-config.yaml -f k8s/10-requestor.yaml -f k8s/20-replier.yaml
kubectl rollout status statefulset/booking-requestor
kubectl rollout status deployment/booking-replier
```

`imagePullPolicy: Never` is what stops Kubernetes trying to pull `booking-demo:local` from a
registry that does not have it.

## Book a berth

```bash
kubectl run curl-test --rm -i --restart=Never --image=curlimages/curl:8.10.1 -- \
  -s -X POST http://booking-requestor:8091/api/bookings \
  -H 'Content-Type: application/json' \
  -d '{"zone":"nr","trainNo":"12951","journeyDate":"2026-09-15",
       "seatClass":"AC3","passengerName":"K8s Test","passengers":2}'
```

```json
{"reservation":{"pnr":"0841866636","status":"CONFIRMED","coach":"B3","berths":"1,2"},
 "replyTopic":"cris/booking/seatReserve/reply/v1/nr/unknown/booking-requestor-0"}
```

The reply topic names the pod, which is the whole point of the next section.

Expect the first call to take a few hundred milliseconds rather than the ~25 ms it takes on the
laptop: it crosses minikube's NAT on top of the usual JIT and connection warmup. Subsequent calls
settle down, but this is not the setup to measure latency on.

---

## Why the requestor is a StatefulSet

Each requestor owns a **durable, exclusive** reply queue named from `reply.instance-id`, which
defaults to the hostname. Under a StatefulSet the hostname is a stable ordinal, so a restarted pod
comes back with the same name and rebinds the queue it already had.

Verified by deleting the pod:

```
reply queues before:  q.cris.booking.reply.booking-requestor-0
reply queues after:   q.cris.booking.reply.booking-requestor-0
```

Same queue, no orphan. Under a Deployment the pod would come back as
`booking-requestor-7dbd6cb676-x4bd6`, leaving the previous queue behind on the broker — durable,
subscribed, and spooling replies nobody will ever read.

## Scaling the repliers

Repliers are interchangeable, so they are a Deployment. They compete for messages on one shared
non-exclusive queue — that queue *is* the consumer group — so scaling needs no coordination:

```bash
kubectl scale deployment/booking-replier --replicas=3
```

Twenty-one bookings across three pods landed **5 / 8 / 8**, with the requestor pod handling none —
it runs the `requestor` profile, so its listener is disabled. No rebalance and no restart; a new
pod takes work as soon as its flow binds, which is why the newest pod's share is the smallest.

---

## Repliers provision no reply queue

Only a requestor needs a reply queue. A replier consumes the shared request queue and publishes each
reply to the requestor's own `replyTo` topic, so it is never addressed on a queue of its own. The
demo's `replier` profile sets `solace.request-reply.reply.enabled=false`, and the pod says so:

```
SolaceRequestReplyAutoConfiguration : Reply endpoint disabled
  (solace.request-reply.reply.enabled=false): this process provisions no reply queue and cannot
  send requests. Replies to requests it handles still go to each request's own replyTo topic.
```

This is what makes a Deployment safe for repliers. Their pod names change on every rollout, so a
per-pod durable queue would be stranded on the broker each time. With the reply endpoint off, two
rollouts of a two-replica Deployment leave the queue list unchanged:

```
#DEAD_MSG_QUEUE
q.cris.booking.reply.booking-requestor-0     <- the only reply queue, and it belongs to the StatefulSet
q.cris.booking.seatReserve
```

A replier still serves `/actuator/health` and `/api/diagnostics/endpoints`, so it stays probeable and
inspectable. `POST /api/bookings` answers `503`:

```json
{"error":"not-a-requestor",
 "detail":"This process runs replier-only (solace.request-reply.reply.enabled=false), so it has
           no reply queue and cannot send a request. Send bookings to a requestor instance instead."}
```

### Cleaning up queues from earlier runs

Durable queues outlive the pods that made them, so a cluster that ran an older build may still hold
orphans. A queue's monitor record has no "is anything bound" field; the bound consumers are a
sub-collection, `txFlows`. An orphan is a reply queue with none:

```bash
SEMP=http://localhost:8085/SEMP/v2
for q in $(curl -s -u admin:admin "$SEMP/monitor/msgVpns/default/queues?select=queueName&count=100" \
             | jq -r '.data[].queueName' | grep '^q.cris.booking.reply.'); do
  flows=$(curl -s -u admin:admin "$SEMP/monitor/msgVpns/default/queues/$q/txFlows?count=10" | jq '.data | length')
  [ "$flows" = "0" ] && echo "orphan: $q"
done
```

Delete one with:

```bash
curl -s -u admin:admin -X DELETE "$SEMP/config/msgVpns/default/queues/<name>"
```

Run it while the pods are up — a queue belonging to a *running* pod also has zero flows for the
moment it is restarting, so a sweep during a rollout can delete a queue that is about to be rebound.

---

## A trap worth knowing: `sh -c` swallows Kubernetes `args`

The image's entrypoint runs the jar through a shell so `$JAVA_OPTS` is expanded. With
`ENTRYPOINT ["sh", "-c", "<script>"]`, anything Kubernetes supplies in `args` becomes the script's
**positional parameters starting at `$0`** rather than being appended to the command. A naive

```dockerfile
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
```

therefore discards `--spring.profiles.active=replier` in silence. The pods start, pass their probes,
and serve traffic — while every one of them runs the default profile, so a "replier" pod is quietly
running both sides. The only visible symptom is a log line early in startup:

```
No active profile set, falling back to 1 default profile: "default"
```

The fix is the `"$@"` and the trailing `"--"` in [Dockerfile](Dockerfile): `--` fills `$0` so real
arguments start at `$1`, and `"$@"` passes them through. Worth checking that log line first whenever
a profile or a `--property` seems not to apply in a pod.

## Teardown

```bash
kubectl delete -f k8s/20-replier.yaml -f k8s/10-requestor.yaml -f k8s/00-config.yaml
minikube stop            # or: minikube delete
```

Deleting the workloads does **not** remove the durable queues they created on the broker; that is
what durable means. Clear them with `docker compose -f ../docker/docker-compose.yml down -v`, which
takes the message spool with it.
