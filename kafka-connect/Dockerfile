FROM confluentinc/cp-kafka-connect:7.6.1

# S3 Sink — Demo 03
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.5.14

# JDBC Sink — Demo 07
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:10.9.5

# Teradata JDBC driver — Maven Central, placed inside the JDBC connector's own lib/
RUN curl -fsSL \
    "https://repo1.maven.org/maven2/com/teradata/jdbc/terajdbc/20.00.00.58/terajdbc-20.00.00.58.jar" \
    -o /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/terajdbc.jar

# oracle.i18n.util.LocaleMapper stub — required by Oracle xmlparserv2-19.7 (bundled by
# kafka-connect-jdbc for Oracle support) which is used by Teradata's TdgssConfigApi to parse
# the TDGSS config XML.  Oracle removed LocaleMapper from the Maven Central orai18n distribution
# but the XML parser binary still references it.  The stub passes the charset name through
# unchanged, which is correct for ASCII/UTF-8 config files.
RUN mkdir -p /tmp/stub/oracle/i18n/util && \
    printf 'package oracle.i18n.util;\npublic class LocaleMapper {\n  public static String getJavaCharacterSet(int t, String name) { return name; }\n}\n' \
        > /tmp/stub/oracle/i18n/util/LocaleMapper.java && \
    javac /tmp/stub/oracle/i18n/util/LocaleMapper.java && \
    jar cf /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/locale-mapper-stub.jar \
        -C /tmp/stub . && \
    rm -rf /tmp/stub
