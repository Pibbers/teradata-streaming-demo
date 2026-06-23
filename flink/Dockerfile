FROM flink:1.20-scala_2.12

USER root

# Enable the bundled S3 plugin for Flink's own FileSystem (checkpointing, state).
# Note: do NOT copy this JAR to lib/ — it would register DelegationTokenReceiver twice
# and crash the TaskManager with "multiple implementations [s3-hadoop]".
RUN mkdir -p /opt/flink/plugins/flink-s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/flink-s3-fs-hadoop/

# Download connector JARs into lib/ so they are available for all jobs
ARG MAVEN=https://repo1.maven.org/maven2

# ── Flink connectors ────────────────────────────────────────────────────────
RUN curl -fsSL "${MAVEN}/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar" \
        -o /opt/flink/lib/flink-sql-connector-kafka.jar && \
    curl -fsSL "${MAVEN}/org/apache/iceberg/iceberg-flink-runtime-1.20/1.8.1/iceberg-flink-runtime-1.20-1.8.1.jar" \
        -o /opt/flink/lib/iceberg-flink-runtime.jar && \
    curl -fsSL "${MAVEN}/org/apache/flink/flink-connector-jdbc/3.3.0-1.20/flink-connector-jdbc-3.3.0-1.20.jar" \
        -o /opt/flink/lib/flink-connector-jdbc.jar && \
    curl -fsSL "${MAVEN}/com/teradata/jdbc/terajdbc/20.00.00.58/terajdbc-20.00.00.58.jar" \
        -o /opt/flink/lib/terajdbc.jar

# ── S3A FileSystem for Iceberg (separate from Flink's plugin) ───────────────
# Iceberg HiveCatalog uses Hadoop's FileSystem API directly (not Flink's plugin
# classloader). hadoop-aws registers S3AFileSystem for s3a:// with Hadoop's
# ServiceLoader. The minimal AWS SDK v1 modules provide the S3 client.
# core-site.xml (copied below) supplies endpoint/credentials for MinIO.
RUN curl -fsSL "${MAVEN}/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar" \
        -o /opt/flink/lib/hadoop-aws.jar && \
    curl -fsSL "${MAVEN}/com/amazonaws/aws-java-sdk-s3/1.12.262/aws-java-sdk-s3-1.12.262.jar" \
        -o /opt/flink/lib/aws-java-sdk-s3.jar && \
    curl -fsSL "${MAVEN}/com/amazonaws/aws-java-sdk-core/1.12.262/aws-java-sdk-core-1.12.262.jar" \
        -o /opt/flink/lib/aws-java-sdk-core.jar && \
    curl -fsSL "${MAVEN}/com/amazonaws/jmespath-java/1.12.262/jmespath-java-1.12.262.jar" \
        -o /opt/flink/lib/jmespath-java.jar && \
    curl -fsSL "${MAVEN}/org/apache/httpcomponents/httpclient/4.5.14/httpclient-4.5.14.jar" \
        -o /opt/flink/lib/httpclient.jar && \
    curl -fsSL "${MAVEN}/org/apache/httpcomponents/httpcore/4.4.16/httpcore-4.4.16.jar" \
        -o /opt/flink/lib/httpcore.jar && \
    curl -fsSL "${MAVEN}/commons-codec/commons-codec/1.15/commons-codec-1.15.jar" \
        -o /opt/flink/lib/commons-codec.jar && \
    curl -fsSL "${MAVEN}/com/fasterxml/jackson/core/jackson-databind/2.15.2/jackson-databind-2.15.2.jar" \
        -o /opt/flink/lib/jackson-databind.jar && \
    curl -fsSL "${MAVEN}/com/fasterxml/jackson/core/jackson-core/2.15.2/jackson-core-2.15.2.jar" \
        -o /opt/flink/lib/jackson-core.jar && \
    curl -fsSL "${MAVEN}/com/fasterxml/jackson/core/jackson-annotations/2.15.2/jackson-annotations-2.15.2.jar" \
        -o /opt/flink/lib/jackson-annotations.jar

# ── Hadoop client libraries (required by iceberg-flink-runtime HiveCatalog) ─
# iceberg-flink-runtime marks hadoop-common as "provided" — we supply it here.
# hadoop-client-runtime is NOT used: it only shades third-party deps, not the
# Hadoop API classes themselves.
RUN curl -fsSL "${MAVEN}/org/apache/hadoop/hadoop-common/3.3.6/hadoop-common-3.3.6.jar" \
        -o /opt/flink/lib/hadoop-common.jar && \
    curl -fsSL "${MAVEN}/org/apache/hadoop/hadoop-hdfs-client/3.3.6/hadoop-hdfs-client-3.3.6.jar" \
        -o /opt/flink/lib/hadoop-hdfs-client.jar && \
    curl -fsSL "${MAVEN}/org/apache/hadoop/hadoop-auth/3.3.6/hadoop-auth-3.3.6.jar" \
        -o /opt/flink/lib/hadoop-auth.jar && \
    curl -fsSL "${MAVEN}/org/apache/hadoop/hadoop-mapreduce-client-core/3.3.6/hadoop-mapreduce-client-core-3.3.6.jar" \
        -o /opt/flink/lib/hadoop-mapreduce-client-core.jar && \
    curl -fsSL "${MAVEN}/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.1.1/hadoop-shaded-guava-1.1.1.jar" \
        -o /opt/flink/lib/hadoop-shaded-guava.jar

# ── Hadoop XML/config dependencies ──────────────────────────────────────────
RUN curl -fsSL "${MAVEN}/com/fasterxml/woodstox/woodstox-core/5.4.0/woodstox-core-5.4.0.jar" \
        -o /opt/flink/lib/woodstox-core.jar && \
    curl -fsSL "${MAVEN}/org/codehaus/woodstox/stax2-api/4.2.1/stax2-api-4.2.1.jar" \
        -o /opt/flink/lib/stax2-api.jar && \
    curl -fsSL "${MAVEN}/org/apache/commons/commons-configuration2/2.1.1/commons-configuration2-2.1.1.jar" \
        -o /opt/flink/lib/commons-configuration2.jar && \
    curl -fsSL "${MAVEN}/commons-collections/commons-collections/3.2.2/commons-collections-3.2.2.jar" \
        -o /opt/flink/lib/commons-collections.jar

# ── Hive Metastore client libraries (required by HiveCatalog Thrift client) ─
RUN curl -fsSL "${MAVEN}/org/apache/hive/hive-standalone-metastore/3.1.3/hive-standalone-metastore-3.1.3.jar" \
        -o /opt/flink/lib/hive-standalone-metastore.jar && \
    curl -fsSL "${MAVEN}/org/apache/hive/hive-common/3.1.3/hive-common-3.1.3.jar" \
        -o /opt/flink/lib/hive-common.jar && \
    curl -fsSL "${MAVEN}/org/apache/hive/hive-exec/3.1.3/hive-exec-3.1.3-core.jar" \
        -o /opt/flink/lib/hive-exec-core.jar && \
    curl -fsSL "${MAVEN}/org/apache/hive/hive-storage-api/2.7.0/hive-storage-api-2.7.0.jar" \
        -o /opt/flink/lib/hive-storage-api.jar

# ── Thrift and fb303 (Hive Metastore RPC protocol) ──────────────────────────
RUN curl -fsSL "${MAVEN}/org/apache/thrift/libthrift/0.9.3/libthrift-0.9.3.jar" \
        -o /opt/flink/lib/libthrift.jar && \
    curl -fsSL "${MAVEN}/org/apache/thrift/libfb303/0.9.3/libfb303-0.9.3.jar" \
        -o /opt/flink/lib/libfb303.jar

# ── Common utility libraries ─────────────────────────────────────────────────
RUN curl -fsSL "${MAVEN}/com/google/guava/guava/27.0-jre/guava-27.0-jre.jar" \
        -o /opt/flink/lib/guava.jar && \
    curl -fsSL "${MAVEN}/commons-logging/commons-logging/1.2/commons-logging-1.2.jar" \
        -o /opt/flink/lib/commons-logging.jar && \
    curl -fsSL "${MAVEN}/commons-lang/commons-lang/2.6/commons-lang-2.6.jar" \
        -o /opt/flink/lib/commons-lang.jar && \
    curl -fsSL "${MAVEN}/joda-time/joda-time/2.8.1/joda-time-2.8.1.jar" \
        -o /opt/flink/lib/joda-time.jar

# ── Teradata dialect for flink-connector-jdbc ────────────────────────────────
# Registers TeradataFactory via SPI so 'connector' = 'jdbc' accepts jdbc:teradata://
# URLs. Compiled here from source against flink-connector-jdbc 3.3.0-1.20.
# Uses ECJ 3.29.0 (runs on JRE 11, unlike ECJ 3.35+ which requires JRE 17).
COPY jobs/TeradataDialectConverter.java /tmp/td-src/TeradataDialectConverter.java
COPY jobs/TeradataDialect.java           /tmp/td-src/TeradataDialect.java
COPY jobs/TeradataFactory.java           /tmp/td-src/TeradataFactory.java
RUN apt-get update && apt-get install -y --no-install-recommends zip && rm -rf /var/lib/apt/lists/* && \
    curl -fsSL "https://repo1.maven.org/maven2/org/eclipse/jdt/ecj/3.29.0/ecj-3.29.0.jar" \
        -o /tmp/ecj.jar && \
    CP="$(ls /opt/flink/lib/*.jar | tr '\n' ':')" && \
    java -jar /tmp/ecj.jar -1.8 -nowarn -d /tmp/td-classes -cp "$CP" \
        /tmp/td-src/TeradataDialectConverter.java \
        /tmp/td-src/TeradataDialect.java \
        /tmp/td-src/TeradataFactory.java && \
    mkdir -p /tmp/td-jar/META-INF/services && \
    cp -r /tmp/td-classes/com /tmp/td-jar/ && \
    echo "com.teradata.flink.TeradataFactory" \
        > "/tmp/td-jar/META-INF/services/org.apache.flink.connector.jdbc.core.database.JdbcFactory" && \
    (cd /tmp/td-jar && zip -r /opt/flink/lib/flink-connector-jdbc-teradata.jar .) && \
    rm -rf /tmp/td-src /tmp/td-classes /tmp/td-jar /tmp/ecj.jar

# ── Hadoop config: registers S3A FileSystem so Iceberg can access MinIO ────────
# Flink picks up core-site.xml from its own conf dir when HADOOP_CONF_DIR is unset.
COPY core-site.xml /opt/flink/conf/core-site.xml

# ── Entrypoint wrapper: fix HMS hostname reverse-DNS (underscore in Docker     ─
#    network FQDN breaks Hive Thrift URI parsing — see hive-hosts-fix.sh)     ─
COPY hive-hosts-fix.sh /hive-hosts-fix.sh
RUN chmod +x /hive-hosts-fix.sh

ENTRYPOINT ["/hive-hosts-fix.sh"]
# No USER flink here — the entrypoint runs as root to modify /etc/hosts,
# then delegates to /docker-entrypoint.sh which starts Flink normally.
