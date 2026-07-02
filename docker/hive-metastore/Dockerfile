FROM apache/hive:4.0.0

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends wget ca-certificates gettext \
    && rm -rf /var/lib/apt/lists/*

# MySQL JDBC driver is required for the metastore to connect to the MySQL backend.
RUN wget -q -P /opt/hive/lib/ \
    https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar

# Hadoop AWS connector and AWS SDK add support for s3a:// and S3AFileSystem.
# Hive needs these jars when accessing MinIO via fs.s3a.impl.
RUN wget -q -P /opt/hive/lib/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar

# Iceberg Hive runtime provides HiveIcebergStorageHandler and HiveIcebergSerDe,
# which HMS needs to process Iceberg metadata commits from Teradata OTF.
# Without this JAR the snapshot commit phase silently fails even though the
# Parquet data files are written to object storage successfully.
RUN wget -q -P /opt/hive/lib/ \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-runtime/1.6.1/iceberg-hive-runtime-1.6.1.jar

# Manage hive-site.xml in this repo so MinIO credentials are driven by env vars.
COPY hive-site.xml /opt/hive/conf/hive-site.xml.template
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENV SERVICE_NAME=metastore \
    DB_DRIVER=mysql

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
