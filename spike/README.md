# Phase 0 spike — provision drift

**Question.** Does `JCSMPSession.provision(..., FLAG_IGNORE_ALREADY_EXISTS)` silently
accept an existing queue whose properties differ from the ones requested? If it does,
`provision.mode=CREATE_IF_MISSING` would let configuration drift away from reality with
no signal, and would be unsafe to ship enabled.

**Answer: no. Drift is loud.** Verified against `solace/solace-pubsub-standard` 10.26,
`sol-jcsmp` 10.30.1.

| Step | Call | Result |
|---|---|---|
| 1 | provision new, `quota=100`, `maxRedelivery=3`, with ignore flag | OK |
| 2 | re-provision, **identical** properties, with ignore flag | OK — idempotent |
| 3 | re-provision, `quota=200`, with ignore flag | **`PropertyMismatchException`** — `Quota mismatch. (QUOTA mismatch, expected=100)` |
| 4 | re-provision, `maxRedelivery=9`, with ignore flag | **`PropertyMismatchException`** — `Max Redelivery mismatch. (MAXREDELIVERY mismatch, expected=3)` |
| 5 | re-provision existing, **without** the ignore flag | `JCSMPErrorResponseException` subcode `33` / 400 `Already Exists` |
| 6 | provision **missing**, **without** the ignore flag | **created it, no exception** |

Step 6 was added after a later attempt to add a third `VALIDATE` mode ("check for drift, never
create") assumed the ignore flag also gated creation, and shipped a dead code path that could
never fire. It does not: `FLAG_IGNORE_ALREADY_EXISTS` only changes how an *existing* queue is
treated. Whether a *missing* one gets created is not conditional on any flag at all — `provision()`
creates it regardless.

## Consequences for the design

1. **`CREATE_IF_MISSING` is safe to ship as the default.** A drifted queue fails fast
   rather than being silently accepted, whether or not the queue had to be created.
2. **`FLAG_IGNORE_ALREADY_EXISTS` suppresses only subcode 33**, not property mismatch —
   the two conditions are genuinely separate, as the distinct subcodes suggested.
3. **The mismatch is a dedicated exception type, not a subcode to switch on.**
   `PropertyMismatchException extends JCSMPException` and exposes `getProperty()` and
   `getPropertyValue()`, so `CREATE_IF_MISSING` can report *which* property drifted and
   what the broker actually has — with no string parsing and **no SEMP call**.
4. **There is no honest third mode.** Step 6 rules out a `VALIDATE` that checks drift
   without ever creating: no flag combination produces that behaviour, so it would need
   an existence probe by some other means before ever calling `provision()` — machinery
   this library does not carry for a mode whose whole appeal was doing less, not more.
   `ProvisionMode` is `OFF` and `CREATE_IF_MISSING` only.

Reproduce with `ProvisionDriftSpike.java` against a broker from `docker/docker-compose.yml`.
