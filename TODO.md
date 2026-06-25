# To-Do

## Backlog

### New demos

- [ ] **Demo 06 — Flink Avro pre-processor → TPT STREAM → Teradata**
  Kafka messages arrive as Avro (requires Schema Registry — see infrastructure task below).
  Flink deserializes the Avro schema, flattens/transforms the record, and emits it as a
  plain row that TPT STREAM consumes. Shows where Flink adds value *before* Teradata rather
  than replacing it. Complements Demo 01 (Avro BLOBs from files) and Demo 02 (plain-text
  Kafka → TPT STREAM).

- [ ] **Demo 07 — Kafka Teradata Sink Connector → Teradata**
  Land Kafka messages directly in Teradata using a Kafka Connect Teradata Sink connector —
  no TPT involved. The `kafka-connect` service is already in the stack; this needs a
  connector JAR added to the image and a connector config (see infrastructure task below).
  Completes the "three ways to get data into Teradata" story alongside TPT STREAM (Demo 02)
  and Flink (Demos 04/05).

### Infrastructure

- [x] **Add Confluent Schema Registry to the stack**
  Demo 06 (Avro over Kafka) requires a Schema Registry so Flink can fetch schemas
  dynamically and schema evolution can be demonstrated properly. Added
  `confluentinc/cp-schema-registry:7.6.1` service to `docker-compose.yml` (host port 8082),
  `SCHEMA_REGISTRY_PORT` to `.env.example`, and updated `docs/setup.md` ports table.

- [ ] **Add Teradata JDBC Sink connector to the `kafka-connect` image (Demo 07 infrastructure)**

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

  **Files to create / modify:**

  | Action | File |
  |--------|------|
  | Modify | `kafka-connect/Dockerfile` |
  | Create | `kafka/connect/td-jdbc-sink.json` |
  | Create | `kafka/connect/td-credentials.properties` *(gitignored; written at demo runtime)* |
  | Create | `demos/07-kafka-connect-td/run.sh` |
  | Create | `tpt/scripts/demo07/prepare.bteq` |
  | Create | `tpt/scripts/demo07/verify.bteq` |
  | Create | `tpt/scripts/demo07/status.bteq` |
  | Create | `docs/demo07-kafka-connect-td.md` |
  | Modify | `README.md` *(add Demo 07 row to demos table)* |
  | Modify | `.gitignore` *(exclude `kafka/connect/td-credentials.properties`)* |
  | Modify | `docker-compose.yml` *(add `schema-registry` to `kafka-connect` depends_on)* |

  **`kafka-connect/Dockerfile` change:**
  ```dockerfile
  FROM confluentinc/cp-kafka-connect:7.6.1
  # S3 Sink — Demo 03
  RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.14
  # JDBC Sink — Demo 07
  RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.9.5
  # Teradata JDBC driver — Maven Central, placed inside the JDBC connector's own lib/
  RUN curl -fsSL \
      "https://repo1.maven.org/maven2/com/teradata/jdbc/terajdbc/20.00.00.58/terajdbc-20.00.00.58.jar" \
      -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/terajdbc.jar
  ```

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

- [ ] **Dead-letter queue / parse-error handling pattern**
  None of the demos show what happens when a bad message arrives — schema mismatch,
  null required field, corrupt payload. Add a DLQ variant (route failed records to a
  separate Kafka topic or a Teradata error table) as an extension of Demo 06 or a
  standalone demo. Important for real-world credibility.

- [ ] **Second synthetic dataset**
  All five demos share the ADS-B aircraft dataset. A second schema (e.g. IoT sensor
  readings or weather observations) would show the framework is domain-agnostic and
  give Avro's nested-record and array types a more natural showcase.

---

## Suggested priority order

| # | Task | Rationale |
|---|------|-----------|
| 1 | Fix "Vantage" branding in docs/README | Trivial effort; keeps docs consistent with the presentation right now |
| 2 | Add Schema Registry to the stack | Infrastructure dependency that must land before Demo 06 can be built |
| 3 | Add Teradata JDBC Sink connector to kafka-connect image | **Decision made**: Confluent JDBC Sink + `terajdbc` from Maven Central. See detailed plan above. |
| 4 | Demo 07 — Kafka Teradata Sink Connector | No Schema Registry dependency; builds on existing kafka-connect service; simpler than Demo 06 |
| 5 | Demo 06 — Flink Avro pre-processor → TPT STREAM | Depends on Schema Registry (#2); higher complexity but high demo value |
| 6 | Update HTML presentation for demos 06 and 07 | Follows naturally once both demos are working end-to-end |
| 7 | Dead-letter queue / error handling pattern | High real-world value but not blocking any other work; good extension of Demo 06 |
| 8 | Second synthetic dataset | Nice to have; low urgency while all demos are still being built |
