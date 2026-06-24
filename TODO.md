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

- [ ] **Add Teradata Sink connector JAR to the `kafka-connect` image**
  Decide between: (a) the official Teradata Kafka Connector (Confluent Hub —
  **licensing TBC**: likely requires a Confluent license; verify before use), or
  (b) the standard Confluent JDBC Sink connector with the Teradata JDBC driver (open
  source, but requires the JDBC driver JAR to be sourced from Teradata). Update
  `kafka-connect/Dockerfile` accordingly. The connector config (bootstrap servers, TD
  JDBC URL, table name mapping) will live alongside the existing S3 Sink connector
  config in `kafka/connect/`.

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
| 3 | Add Teradata Sink connector JAR to kafka-connect image | Infrastructure dependency for Demo 07; decision on which connector to use first |
| 4 | Demo 07 — Kafka Teradata Sink Connector | No Schema Registry dependency; builds on existing kafka-connect service; simpler than Demo 06 |
| 5 | Demo 06 — Flink Avro pre-processor → TPT STREAM | Depends on Schema Registry (#2); higher complexity but high demo value |
| 6 | Update HTML presentation for demos 06 and 07 | Follows naturally once both demos are working end-to-end |
| 7 | Dead-letter queue / error handling pattern | High real-world value but not blocking any other work; good extension of Demo 06 |
| 8 | Second synthetic dataset | Nice to have; low urgency while all demos are still being built |
