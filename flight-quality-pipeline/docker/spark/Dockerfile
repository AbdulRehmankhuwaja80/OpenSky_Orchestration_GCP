# syntax=docker/dockerfile:1
FROM bitnamilegacy/spark:3.5

ENV PYTHONUNBUFFERED=1

USER root
WORKDIR /app

COPY docker/spark/requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --default-timeout=180 --retries 10 -r requirements.txt

COPY transformation/spark_jobs ./transformation/spark_jobs

# Pulling gcs-connector via spark-submit --packages resolves the unshaded Maven artifact,
# whose transitive Guava version conflicts with the one already on Spark/Hadoop's classpath
# (NoSuchMethodError on Preconditions.checkState). Google's own pre-shaded jar avoids this.
RUN mkdir -p /app/jars && \
    curl -fsSL -o /app/jars/gcs-connector-hadoop3-2.2.19.jar \
      https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-2.2.19.jar && \
    chmod a+r /app/jars/gcs-connector-hadoop3-2.2.19.jar

# Pre-create + chown the Ivy cache dir so the named volume mounted here at runtime inherits
# UID 1001 ownership (Docker copies the image's existing content/perms into a fresh named
# volume on first use) - otherwise the volume is root-owned and this non-root user can't write.
RUN mkdir -p /opt/bitnami/spark/.ivy2 && chown -R 1001:1001 /opt/bitnami/spark/.ivy2

USER 1001
