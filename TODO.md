# To-Do

## Backlog

### New demos

- [ ] **Demo 06 — Flink Avro pre-processor → TPT STREAM → Teradata**

  **Architecture:**
  ```
  adsb_producer.py --format sr-avro --registry http://localhost:8082
      → Kafka: adsb-avro  [Confluent wire Avro: \x00 + 4-byte schema ID + schemaless Avro]
          → Flink SQL (avro-confluent source → csv/pipe-delimited sink)
              → Kafka: adsb-positions-flink  [pipe-delimited text, same format as Demo 02]
                  → TPT STREAM (kafka_stream_06.tbuild)
                      → Teradata: demo_db.adsb_positions_06
  ```

  Shows where Flink adds value *before* Teradata — typed Avro deserialization,
  boolean→INT cast, epoch-millis→TIMESTAMP, pos_date derivation — before handing off
  to TPT STREAM for low-latency ingest. Complements Demo 02 (plain text → TPT) and
  Demo 01 (Avro BLOBs from files).

  **Schema evolution note:** Schema Registry makes schema changes *safe* (old and new
  message versions coexist in the topic; Flink fetches the writer schema per message ID).
  But it does not make them *automatic* — adding a field to the Avro schema also requires
  updating the Flink SQL table definition, the pipe-delimited sink, the TPT STREAM schema,
  and the Teradata DDL. Good demo talking point.

  **Files to create / modify:**

  | Action | File | Notes |
  |--------|------|-------|
  | Modify | `flink/Dockerfile` | Add `flink-sql-avro-confluent-registry-1.20.1.jar` from Maven Central |
  | Modify | `kafka/producers/adsb_producer.py` | Add `sr-avro` format, `--registry` arg, `encode_sr_avro()`, schema registration via urllib |
  | Create | `flink/jobs/demo06/stream.sql` | `avro-confluent` source → `csv`/pipe-delimited Kafka sink; no checkpointing needed |
  | Create | `flink/jobs/demo06/batch.sql` | Same as stream.sql with `earliest-offset` for `--bounded` mode |
  | Create | `tpt/scripts/demo06/prepare.bteq` | Full DDL for `adsb_positions_06`; drop LT/ET; DELETE ALL |
  | Create | `tpt/scripts/demo06/verify.bteq` | COUNT(*) + TOP 5 sample from `adsb_positions_06` |
  | Create | `tpt/scripts/demo06/status.bteq` | Single-line STATUS: N rows (mid-run heartbeat) |
  | Create | `tpt/tbuild/kafka_stream_06.tbuild` | Copy of `kafka_stream.tbuild`; target `adsb_positions_06`, LT/ET `adsb_positions_06_LT/ET` |
  | Create | `demos/06-flink-avro-tpt/run.sh` | Orchestration: reset → Flink job → TPT + producer → monitor → graceful drain |
  | Create | `docs/demo06-flink-avro-tpt.md` | Documentation |
  | Modify | `README.md` | Add Demo 06 row to demos table |

  **Key design decisions:**
  - Target table `adsb_positions_06` (not `adsb_positions`) — keeps demos fully independent
  - Producer SR encoding: `\x00` + `struct.pack('>I', schema_id)` + schemaless Avro bytes — no new pip dependencies, uses stdlib `struct` + `urllib.request`
  - Flink type mapping from Avro: `boolean` → `BOOLEAN` (cast to INT in INSERT); `long/timestamp-millis` → `TIMESTAMP_LTZ(3)` (formatted via `DATE_FORMAT`)
  - Intermediate topic `adsb-positions-flink`: pipe-delimited text, identical schema to Demo 02 so TPT STREAM reuse is straightforward
  - TPT STREAM consumes `adsb-positions-flink` with `KAFKA_IDLE_TIMEOUT=30`; exits naturally after producer stops and Flink goes quiet
  - Requires `docker compose build flink-jobmanager flink-taskmanager` after Dockerfile change

  **Risks:**
  - `flink-sql-avro-confluent-registry` must be on Flink's classpath (lib/), not in plugins/ — verified `1.20.1` exists on Maven Central (22 MB)
  - `DATE_FORMAT(TIMESTAMP_LTZ, ...)` is valid in Flink SQL — confirmed by Flink docs
  - Schema registration POST to SR must happen before first produce; error surfaced immediately via `urllib.request.URLError`

- [x] **Demo 07 — Kafka Teradata Sink Connector → Teradata**
  Completed. See `demos/07-kafka-connect-td/` and `docs/demo07-kafka-connect-td.md`.

### Infrastructure

- [x] **Add Confluent Schema Registry to the stack**
  Demo 06 (Avro over Kafka) requires a Schema Registry so Flink can fetch schemas
  dynamically and schema evolution can be demonstrated properly. Added
  `confluentinc/cp-schema-registry:7.6.1` service to `docker-compose.yml` (host port 8082),
  `SCHEMA_REGISTRY_PORT` to `.env.example`, and updated `docs/setup.md` ports table.

- [x] **Add Teradata JDBC Sink connector to the `kafka-connect` image (Demo 07 infrastructure)**

  **Decision made: Option B — Confluent JDBC Sink + Teradata JDBC driver from Maven Central.**
  - Option A (`confluentinc/kafka-connect-teradata`) ruled out: uses the Confluent Software
    Evaluation License (time-limited, non-production). Hard blocker.
  - Option B (`confluentinc/kafka-connect-jdbc`) uses the Confluent Community License — free,
    no registration required.
  - Teradata JDBC driver (`terajdbc 20.00.00.58`) is on Maven Central and already pulled by
    `flink/Dockerfile` — no manual download or Teradata portal registration needed.

  **Key constraint:** `kafka-connect-jdbc` has no `TeradataDialect`. The connector uses
  `GenericDatabaseDialect`, which generates standard ANSI `INSERT INTO (cols) VALUES (?)`.
  Fine for `insert` mode. If `upsert` is needed later, a custom dialect would be required.

  **`kafka/connect/td-jdbc-sink.json` key design decisions:**
  - Topic: `adsb-positions-json` (separate from Demo 02's `adsb-positions`; created/destroyed
    by the demo run script to avoid coupling)
  - `"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector"`
  - JDBC URL: `jdbc:teradata://<host>/DATABASE=<db>,TMODE=ANSI,CHARSET=UTF8`
    (`TMODE=ANSI` is required — Teradata defaults to TERA mode; JDBC Sink expects ANSI
    per-statement error semantics)
  - Credentials via `${file:/etc/kafka/connect-configs/td-credentials.properties:td_host}`
    (Kafka Connect external secrets mechanism; file is bind-mounted via existing volume)
  - `"insert.mode": "insert"`, `"auto.create": "false"`, `"auto.evolve": "false"`
    (table already created by `run_setup.sh`; `auto.create` would fail on PPI DDL)
  - `"value.converter": "org.apache.kafka.connect.json.JsonConverter"` with
    `"value.converter.schemas.enable": "false"` (overrides worker-level ByteArrayConverter
    at connector scope only — Demo 03 S3 Sink is unaffected)
  - `"fields.whitelist"` listing all columns except `ingest_ts` (the `DEFAULT
    CURRENT_TIMESTAMP(0)` server-side column absent from Kafka records)

  **Risks:**
  - If "No suitable driver found for jdbc:teradata://" error occurs, the JAR landed on the
    wrong classloader. Fix: move it to its own plugin dir
    (`/usr/share/confluent-hub-components/teradata-jdbc/lib/terajdbc.jar`) and add that
    path to `CONNECT_PLUGIN_PATH`.
  - `upsert` mode unsupported without a custom Teradata dialect — document this in demo docs.

### Housekeeping

- [x] **Fix residual "Vantage" branding in docs and README**
  Fixed `docs/setup.md` Prerequisites table ("Vantage 20.x" → "Teradata 20.x").

### Presentation

- [ ] **Update HTML presentation for demos 06 and 07**
  Once those demos exist: add flow-diagram slides, "what it demonstrates" sections,
  and update the Compare & Choose summary table. The "three ways in" section expands
  to five distinct ingestion paths.

### Future / lower priority

- [x] **Audit demo target table naming for consistency**
  Completed. Findings and fixes:
  - Demo 7 was sharing `adsb_positions` with Demo 2 → renamed to `adsb_positions_07`
    (connector config, prepare/status/verify BTEQ, run.sh, doc updated)
  - `aircraft_registry` in `setup_demo_tables.bteq` was dead code (Demo 5 uses `aircraft_ref`
    created by its own `demo05/setup.bteq`) → removed from infra script
  - `flink/jobs/demo06_batch.sql` and `demo06_stream.sql` were exact duplicates of the
    `flink/jobs/demo06/` subdir versions → deleted
  - README repo layout was missing `tpt/scripts/demo07/` → added
  - Final table ownership: `adsb_positions` (Demo 2), `adsb_positions_06` (Demo 6),
    `adsb_positions_07` (Demo 7); all demos fully independent

- [x] **Dead-letter queue / parse-error handling pattern**
  Completed as Demo 08. See `demos/08-dlq-pattern/` and `docs/demo08-dlq-pattern.md`.
  Uses Kafka Connect `errors.tolerance=all` + `errors.deadletterqueue.topic.name` to
  route bad records (null latitude → NOT NULL violation) to `adsb-positions-dlq-demo.dlq`
  while valid records flow to Teradata `adsb_positions_08`. `adsb_producer.py` gains
  `--inject-errors N` to inject malformed records deterministically.

- [ ] **Second synthetic dataset**
  All five demos share the ADS-B aircraft dataset. A second schema (e.g. IoT sensor
  readings or weather observations) would show the framework is domain-agnostic and
  give Avro's nested-record and array types a more natural showcase.

---

## Suggested priority order

| # | Task | Rationale |
|---|------|-----------|
| 1 | Demo 06 — Flink Avro pre-processor → TPT STREAM | All infrastructure dependencies (Schema Registry) already live; this is the last remaining core demo |
| 2 | Update HTML presentation for demos 06 and 07 | Demo 07 is done; one pass after Demo 06 completes covers both |
| 3 | Audit demo target table naming for consistency | Quick housekeeping; best done before the presentation is finalised |
| 4 | Dead-letter queue / error handling pattern | High real-world value; best built as extension of Demo 06 while that code is fresh |
| 5 | Second synthetic dataset | Nice to have; no blockers, low urgency while other items outstanding |
