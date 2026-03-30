# Custom Airflow image with Java + PySpark so spark-submit is available
# inside the Airflow container and SparkSubmitOperator can connect to the
# Spark cluster at spark://spark-master:7077.
#
# Build is triggered automatically by docker-compose via `build: ./airflow`.

FROM apache/airflow:2.9.3

USER root

# Install OpenJDK — required by PySpark / spark-submit
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

USER airflow

# PySpark ships spark-submit as part of the package — no separate Spark
# installation needed for the client/driver side.
# apache-airflow-providers-apache-spark provides SparkSubmitOperator.
RUN pip install --no-cache-dir \
    pyspark==3.5.2 \
    apache-airflow-providers-apache-spark==4.9.0
