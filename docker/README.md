# Local broker

```bash
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml ps      # wait for "healthy"
```

| What | URL / port |
|---|---|
| SMF (messaging) | `tcp://localhost:55565` |
| Broker Manager UI | http://localhost:8085 — `admin` / `admin` |
| SEMP v2 | http://localhost:8085/SEMP/v2 |

**Why 55565 and not 55555?** macOS reserves 55555, so Docker cannot bind it. The
compose file publishes SMF on 55565 by default; set `SOLACE_SMF_PORT` to change it.

Tear down, including the message spool:

```bash
docker compose -f docker/docker-compose.yml down -v
```
