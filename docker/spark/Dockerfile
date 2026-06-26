FROM apache/spark:3.5.1-python3

USER root

RUN pip install --no-cache-dir kafka-python psycopg2-binary

ENV PATH="${PATH}:/opt/spark/bin"
ARG MAVEN_REPO="https://repo1.maven.org/maven2"

RUN ln -sf /opt/spark/bin/spark-submit /usr/local/bin/spark-submit

RUN set -eux; \
    curl -fL "${MAVEN_REPO}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar" -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar; \
    curl -fL "${MAVEN_REPO}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar" -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar; \
    curl -fL "${MAVEN_REPO}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" -o /opt/spark/jars/kafka-clients-3.4.1.jar; \
    curl -fL "${MAVEN_REPO}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" -o /opt/spark/jars/commons-pool2-2.11.1.jar; \
    curl -fL "${MAVEN_REPO}/org/apache/hadoop/hadoop-client-api/3.3.4/hadoop-client-api-3.3.4.jar" -o /opt/spark/jars/hadoop-client-api-3.3.4.jar; \
    curl -fL "${MAVEN_REPO}/org/apache/hadoop/hadoop-client-runtime/3.3.4/hadoop-client-runtime-3.3.4.jar" -o /opt/spark/jars/hadoop-client-runtime-3.3.4.jar; \
    curl -fL "${MAVEN_REPO}/com/google/code/findbugs/jsr305/3.0.0/jsr305-3.0.0.jar" -o /opt/spark/jars/jsr305-3.0.0.jar; \
    curl -fL "${MAVEN_REPO}/commons-logging/commons-logging/1.1.3/commons-logging-1.1.3.jar" -o /opt/spark/jars/commons-logging-1.1.3.jar; \
    curl -fL "${MAVEN_REPO}/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar" -o /opt/spark/jars/lz4-java-1.8.0.jar; \
    curl -fL "${MAVEN_REPO}/org/slf4j/slf4j-api/2.0.7/slf4j-api-2.0.7.jar" -o /opt/spark/jars/slf4j-api-2.0.7.jar; \
    curl -fL "${MAVEN_REPO}/org/xerial/snappy/snappy-java/1.1.10.3/snappy-java-1.1.10.3.jar" -o /opt/spark/jars/snappy-java-1.1.10.3.jar

RUN mkdir -p /workspace /workspace/.spark-checkpoints

WORKDIR /workspace

USER spark
