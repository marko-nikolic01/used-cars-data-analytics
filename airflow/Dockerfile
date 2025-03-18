FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64

USER airflow

RUN pip uninstall -y apache-airflow-providers-openlineage && \
    pip install --no-cache-dir apache-airflow-providers-openlineage>=1.8.0

RUN pip install --no-cache-dir apache-airflow apache-airflow-providers-apache-spark pyspark==3.2.2

COPY ./scripts/init_spark_connector.py /opt/airflow/init_ssh_connection.py
