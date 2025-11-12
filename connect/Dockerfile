FROM quay.io/debezium/connect:3.3.1

USER root

# Tools we need to fetch and unpack plugins
RUN microdnf -y install unzip curl && microdnf clean all || true

# Create a directory for extra jars
RUN mkdir -p /etc/kafka-connect/jars /kafka/connect/kafka-connect-jdbc

# Install JDBC Sink connector from Confluent Hub (includes dependencies)
RUN curl -fsSL https://hub-downloads.confluent.io/api/plugins/confluentinc/kafka-connect-jdbc/versions/10.7.4/confluentinc-kafka-connect-jdbc-10.7.4.zip -o /tmp/kafka-connect-jdbc.zip \
    && unzip -q /tmp/kafka-connect-jdbc.zip -d /tmp/jdbc \
    && cp -r /tmp/jdbc/confluentinc-kafka-connect-jdbc*/lib/* /kafka/connect/kafka-connect-jdbc/ \
    && rm -rf /tmp/jdbc /tmp/kafka-connect-jdbc.zip

# Add ClickHouse JDBC driver with all dependencies so JDBC sink can write to ClickHouse
RUN set -eux; \
    curl -fsSL https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.9.4/clickhouse-jdbc-0.9.4-all-dependencies.jar \
      -o /tmp/clickhouse-jdbc-all-deps.jar; \
    cp /tmp/clickhouse-jdbc-all-deps.jar /kafka/connect/kafka-connect-jdbc/; \
    mv /tmp/clickhouse-jdbc-all-deps.jar /etc/kafka-connect/jars/clickhouse-jdbc-all-deps.jar

# Legacy Yandex ClickHouse JDBC is not installed to avoid classpath conflicts

# Add Jolokia JVM agent for Debezium UI metrics
RUN mkdir -p /kafka/libs \
    && curl -fsSL https://repo1.maven.org/maven2/org/jolokia/jolokia-jvm/1.7.1/jolokia-jvm-1.7.1.jar \
       -o /kafka/libs/jolokia-jvm-1.7.1.jar

# Ensure permissions for the default user in the base image
RUN chown -R kafka:kafka /kafka/connect /etc/kafka-connect /kafka/libs

USER kafka
