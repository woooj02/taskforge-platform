# TaskForge Platform

A distributed task orchestration platform built with **Event Sourcing**, **CQRS**, and the **Saga pattern** on a backbone of gRPC microservices. Designed as a reference implementation showing how these patterns fit together in Python — every service has a single responsibility, every state change is an event, and every multi-step workflow is a compensable saga.

## Architecture

```
                         ┌─────────────────┐
                         │   API Gateway   │  (gRPC)
                         └────────┬────────┘
                                  │
                ┌─────────────────┴─────────────────┐
                │                                   │
        ┌───────▼────────┐                ┌─────────▼────────┐
        │  Task Service  │                │ Workflow Service │
        │  (write side)  │                │   (saga driver)  │
        └───────┬────────┘                └─────────┬────────┘
                │                                   │
                ▼                                   ▼
        ┌──────────────────────────────────────────────────┐
        │   PostgreSQL (event store)  +  Kafka (event bus) │
        └──────────────────────────────────────────────────┘
                │                                   │
                ▼                                   ▼
        ┌──────────────┐                  ┌──────────────────┐
        │    Redis     │                  │  Jaeger + Prom + │
        │   (cache)    │                  │     Grafana      │
        └──────────────┘                  └──────────────────┘
```

### Patterns

- **Event Sourcing** — Every state mutation on a `Task` aggregate is appended as an immutable event to PostgreSQL. The current state is a fold over the event history; nothing is updated in place.
- **CQRS** — Writes go through the task service's command handlers and event store. Reads (when added) project events into denormalized read models.
- **Saga** — Multi-step workflows (create task → assign → notify → schedule) live in the workflow service as compensable sagas. Each step has a forward action and a compensating action; partial failures unwind cleanly.
- **gRPC** — All inter-service communication is typed gRPC over protobuf. Schemas live in `protos/`.

## Components

| Service | Role |
|---------|------|
| `task-service` | Command side. Holds the Task aggregate, validates commands, writes events. |
| `workflow-service` | Saga orchestrator. Drives multi-step workflows; emits compensating events on failure. |
| `api-gateway` | Public gRPC entry point; fans out to internal services. |
| `postgres` | Event store. |
| `kafka` + `zookeeper` | Event bus between services. |
| `redis` | Projection / read cache. |
| `jaeger` | Distributed tracing via OpenTelemetry. |
| `prometheus` + `grafana` | Metrics + dashboards. |

## Tech stack

- Python 3.10+, asyncio
- gRPC (`grpcio`, `grpcio-tools`, protobuf 4)
- Kafka (`aiokafka`)
- PostgreSQL (`asyncpg`, SQLAlchemy 2.x async)
- Redis
- OpenTelemetry (API + SDK + OTLP exporter)
- JWT auth (`pyjwt`)
- structlog + loguru, tenacity for retries
- Docker Compose for local dev; Kubernetes manifests in `kubernetes/`

## Quick start

```bash
# 1. Generate protobuf stubs
make proto

# 2. Build images
make build

# 3. Start the whole stack (postgres, kafka, redis, services, observability)
make up

# 4. Run database migrations
make db-migrate

# 5. Run the workflow demo (creates a task, drives a saga, emits events)
make demo

# 6. Tail logs
make logs
```

Tear down with `make down`. Full reset (volumes + caches + generated stubs): `make clean`.

## Observability

After `make up`:

- **Grafana** — `http://localhost:3000` (default `admin`/`admin`)
- **Jaeger** — `http://localhost:16686`
- **Prometheus** — `http://localhost:9090`

Every gRPC call is traced; every service emits structured logs with a correlation ID propagated through the event headers.

## Development

```bash
make install     # editable install with dev extras
make test        # pytest with coverage
make lint        # black + isort + mypy --strict
make format      # auto-format
```

## Layout

```
taskforge-platform/
├── protos/                          # protobuf schemas
│   ├── common.proto
│   ├── task_service.proto
│   └── workflow_service.proto
├── src/
│   ├── services/
│   │   ├── task_service/            # command side
│   │   │   ├── domain/              # aggregates, commands, events, value objects
│   │   │   └── infrastructure/      # event store, gRPC server, repository
│   │   └── workflow_service/        # saga orchestrator
│   │       └── saga/                # workflow definitions
│   └── libs/
│       ├── common/                  # event_bus, observability, unit_of_work
│       └── saga_framework/          # saga + step base classes
├── kubernetes/                      # k8s manifests for prod
├── tests/
├── docker-compose.yml               # local dev stack
└── Makefile                         # entry point for every operation
```

## License

MIT
