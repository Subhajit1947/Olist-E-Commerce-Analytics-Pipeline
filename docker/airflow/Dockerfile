# docker/airflow/Dockerfile
FROM apache/airflow:2.7.0

USER root

# Install system dependencies including Java
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        curl \
        openjdk-11-jdk \
        && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH



RUN mkdir -p /opt/app/data/output && \
chown -R 50000:0 /opt/app/data/output && \
chmod -R 777 /opt/app/data/output

# Copy requirements and change ownership using UID/GID (50000:0)
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN chown 50000:0 /tmp/requirements-airflow.txt

USER airflow

# Install Python dependencies as airflow user
RUN pip install --no-cache-dir --user -r /tmp/requirements-airflow.txt && \
    rm /tmp/requirements-airflow.txt

WORKDIR /opt/airflow