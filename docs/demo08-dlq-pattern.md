# Demo 8 — Kafka Connect Dead-Letter Queue Pattern

## What it demonstrates

In production, streams always contain some invalid records — wrong types, null required fields, corrupt payloads. A pipeline that fails on the first bad message is not production-ready.

This demo shows Kafka Connect's built-in **dead-letter queue (DLQ)** mechanism: bad records are routed to a separate Kafka topic rather than failing the connector. Valid records continue flowing to Teradata without interruption.

| Demo | Method | Error handling |
|------|--------|----------------|
| 7 | Kafka Connect JDBC Sink | None — any error fails the task |
| **8** | **Kafka Connect JDBC Sink + DLQ** | **errors.tolerance=all → bad rows → DLQ topic** |

## Architecture

```
adsb_producer.py                 Kafka                        Kafka Connect           Teradata
  (--format connect-json   →  adsb-positions-dlq-demo  →  JDBC Sink connector  →  adsb_positions_08
   --inject-errors 5)                                           │
                                                               └─ [bad rows] → adsb-positions-dlq-demo.dlq
```

Every 5th message has `latitude: null` in the payload, while the Connect JSON schema still declares that field `"optional": false`. Kafka Connect's `JsonConverter` rejects the null during JSON→Connect-record conversion — a `DataException` at the `VALUE_CONVERTER` stage, before the record ever reaches the JDBC Sink task or Teradata — and because `errors.tolerance=all`, routes the record to the DLQ topic and continues processing. (Teradata's own `NOT NULL` constraint on `adsb_positions_08.latitude` is never actually exercised by this demo — the failure happens one stage earlier, at schema conversion.)

## What activates the DLQ

Three properties added to the connector config (everything else is identical to Demo 7):

```json
"errors.tolerance": "all",
"errors.deadletterqueue.topic.name": "adsb-positions-dlq-demo.dlq",
"errors.deadletterqueue.context.headers.enable": "true"
```

- `errors.tolerance=all` — swallow any per-record error; log it and continue
- `errors.deadletterqueue.topic.name` — the DLQ is a normal Kafka topic; it is auto-created by the connector if it does not exist (or pre-created by `run.sh` with explicit partition/replication config)
- `errors.deadletterqueue.context.headers.enable=true` — attach error context to each DLQ record as Kafka headers (see below)

Additionally:

```json
"errors.log.enable": "true",
"errors.log.include.messages": "true"
```

These write the error and the original payload to the Kafka Connect worker log for observability without consuming the DLQ topic.

## Error context headers on DLQ records

Each record in the DLQ topic carries headers that identify exactly what went wrong:

| Header | Example value |
|--------|---------------|
| `__connect.errors.topic` | `adsb-positions-dlq-demo` |
| `__connect.errors.partition` | `0` |
| `__connect.errors.offset` | `4` |
| `__connect.errors.connector.name` | `demo08-td-jdbc-sink-dlq` |
| `__connect.errors.task.id` | `0` |
| `__connect.errors.stage` | `VALUE_CONVERTER` |
| `__connect.errors.exception.class.name` | `org.apache.kafka.connect.errors.DataException` |
| `__connect.errors.exception.message` | Converter error: null value for a required (non-optional) field |

The original message payload is preserved in the DLQ record value — enabling replay after the issue is fixed.

## Error injection

`adsb_producer.py` accepts `--inject-errors N`: it injects on message index `0, N, 2N, ...` (0-based, so with `N=5` the 1st, 6th, 11th, ... messages sent are bad, not literally "every 5th" by 1-based count) — each has `latitude: null` in the payload while the Connect JSON schema keeps `"optional": false` for that field. This forces a converter-level `DataException` on every injected record, regardless of the Teradata DDL.

```python
# In encode_connect_json():
"latitude": None if inject_error else record["latitude"],
```

## Running the demo

```bash
# Bounded: 100 messages, ~20 errors expected
bash demos/08-dlq-pattern/run.sh

# Continuous: produce until Ctrl+C
bash demos/08-dlq-pattern/run.sh --continuous
```

The script:
1. Writes `kafka/connect/td-credentials.properties` from `.env`
2. Deletes any existing `demo08-td-jdbc-sink-dlq` connector
3. Drops and recreates `adsb-positions-dlq-demo` and `adsb-positions-dlq-demo.dlq` topics
4. Creates `adsb_positions_08` (if absent) and clears it
5. Registers the connector and waits for `RUNNING`
6. Runs `adsb_producer.py --format connect-json --inject-errors 5`
7. Polls Teradata until good-record count stabilises
8. Samples 3 DLQ records and displays their error headers
9. Prints final row count and per-aircraft breakdown

## Expected output (bounded, 100 messages)

```
──────────────────────────────────────────────────────
  Demo 8: Kafka Connect JDBC Sink — DLQ Pattern
  Broker (host):     localhost:29092
  Kafka Connect:     http://localhost:8083
  Topic:             adsb-positions-dlq-demo
  DLQ topic:         adsb-positions-dlq-demo.dlq
  Connector:         demo08-td-jdbc-sink-dlq
  Error injection:   every 5th message (latitude=null)
  Mode:              BOUNDED (100 messages)
──────────────────────────────────────────────────────

── 3/5  Registering JDBC Sink connector with DLQ config ──
      errors.tolerance=all → bad rows go to adsb-positions-dlq-demo.dlq, connector stays RUNNING
      Waiting for connector+task to start......... RUNNING

── 4/5  Publishing ADS-B positions (with injected errors) ─
      Publishing 100 messages (10 aircraft × 10 cycles, 1s interval)...
      Every 5th message has latitude=null → routed to DLQ.
      Expected: ~80 good rows in Teradata, ~20 records in DLQ.

[2026-06-26T...] 50 ADS-B messages sent to adsb-positions-dlq-demo
[2026-06-26T...] 100 ADS-B messages sent to adsb-positions-dlq-demo

      Producer done. Waiting for JDBC Sink to flush to Teradata...
      [3s] Rows in adsb_positions_08: 42 / ~80
      [6s] Rows in adsb_positions_08: 80 / ~80

── 5/5  Inspecting DLQ and verifying Teradata ─────────────

── DLQ sample (up to 3 records with error headers) ────────
  CreateTime:1750941...
      __connect.errors.topic: adsb-positions-dlq-demo
      __connect.errors.offset: 4
      __connect.errors.exception.class.name: org.apache.kafka.connect.errors.DataException
      __connect.errors.exception.message: Error converting value: null ...

── Teradata row count ──────────────────────────────────────

  positions_received  earliest_ts  latest_ts  aircraft_seen
  ------------------  -----------  ---------  -------------
                  80  2026-06-26…  2026-06-26…           10

──────────────────────────────────────────────────────
  Demo 8 complete!

  Results:
    100 messages produced
    80 good rows → Teradata demo_db.adsb_positions_08
    20 error records → Kafka adsb-positions-dlq-demo.dlq
    Connector status: RUNNING (errors.tolerance=all kept it alive)
──────────────────────────────────────────────────────
```

## Manually inspecting the DLQ topic

```bash
# Show all DLQ records with headers
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic adsb-positions-dlq-demo.dlq \
  --from-beginning \
  --property print.headers=true \
  --property print.timestamp=true \
  --timeout-ms 5000

# Connector status (should show RUNNING, not FAILED)
curl -s http://localhost:8083/connectors/demo08-td-jdbc-sink-dlq/status \
  | python3 -m json.tool

# Live connector log (shows per-record error messages)
docker compose logs -f kafka-connect
```

## Replay from the DLQ

The DLQ is a normal Kafka topic. After fixing the underlying issue (e.g., the producer now sends valid latitude values), you can replay:

```bash
# Copy DLQ records back to the main topic after fixing them
# (in practice: consume DLQ, fix payload, re-produce to main topic)
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic adsb-positions-dlq-demo.dlq \
  --from-beginning --timeout-ms 3000 \
  | docker compose exec -T kafka kafka-console-producer \
      --bootstrap-server localhost:9092 \
      --topic adsb-positions-dlq-demo
```

Real-world replay typically involves a small script that reads DLQ records, corrects the payload, and re-publishes to the source topic.

## Troubleshooting

| Error | Cause | Fix |
|-------|-------|-----|
| `No suitable driver found for jdbc:teradata://` | Teradata JAR on wrong classloader | See Demo 7 troubleshooting |
| `Authentication failed` | Wrong credentials | Check `.env`; re-run `run.sh` |
| `Table 'adsb_positions_08' not found` | `prepare.bteq` did not run | Check step 2 completed without error |
| DLQ topic has 0 records | `errors.deadletterqueue.topic.name` typo | Verify connector config matches the topic name |
| Connector goes `FAILED` despite `errors.tolerance=all` | Worker-level error (not per-record) | Check `docker compose logs kafka-connect` for startup errors |

## Key design decisions

| Setting | Value | Reason |
|---------|-------|--------|
| `errors.tolerance` | `all` | Route any per-record failure to DLQ; connector stays `RUNNING` |
| `errors.deadletterqueue.topic.name` | `adsb-positions-dlq-demo.dlq` | DLQ is a normal Kafka topic — can be monitored, consumed, and replayed |
| `errors.deadletterqueue.context.headers.enable` | `true` | Attach full error context (offset, exception class, stack trace) to each DLQ record |
| `errors.log.enable` + `errors.log.include.messages` | `true` | Mirror errors to the Connect worker log for real-time observability |
| Error type | null latitude (NOT NULL column) | Guaranteed JDBC exception regardless of Teradata version or session mode |

## Why not a Flink DLQ?

Flink SQL has no per-record DLQ mechanism. An Avro deserialization failure (wrong magic byte, unknown schema ID, corrupt payload) terminates the Flink job — there is no way to catch and route the bad record within SQL. A Flink DataStream API job (Java/Scala) could use side outputs to implement DLQ, but that is beyond the scope of this demo stack.

TPT STREAM's error table (`adsb_positions_06_ET`) is the equivalent pattern for bulk-load pipelines: rows that fail type casting or constraint validation are written to the error table rather than aborting the load.

## Known limitations

- `errors.tolerance=all` catches per-record failures only. Worker-level failures (e.g., cannot connect to Teradata) still fail the connector task.
- The DLQ record preserves the original serialized bytes, not a human-readable form. Viewing the payload requires a consumer that can deserialize the Connect JSON envelope.
- No automatic replay — replay requires a manual step or a separate pipeline reading from the DLQ topic.
