FROM flink:1.20-scala_2.12

USER root

# Enable the bundled S3 plugin (for MinIO / S3-compatible checkpointing)
RUN mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/flink-s3-fs-hadoop/

# Download connector JARs into lib/ so they are available for all jobs
ARG FLINK_VERSION=1.20
ARG MAVEN=https://repo1.maven.org/maven2

RUN curl -fsSL "${MAVEN}/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar" \
        -o /opt/flink/lib/flink-sql-connector-kafka.jar && \
    curl -fsSL "${MAVEN}/org/apache/iceberg/iceberg-flink-runtime-1.20/1.8.1/iceberg-flink-runtime-1.20-1.8.1.jar" \
        -o /opt/flink/lib/iceberg-flink-runtime.jar && \
    curl -fsSL "${MAVEN}/org/apache/flink/flink-connector-jdbc/3.3.0-1.20/flink-connector-jdbc-3.3.0-1.20.jar" \
        -o /opt/flink/lib/flink-connector-jdbc.jar && \
    curl -fsSL "${MAVEN}/com/teradata/jdbc/terajdbc/20.00.00.58/terajdbc-20.00.00.58.jar" \
        -o /opt/flink/lib/terajdbc.jar

USER flink
