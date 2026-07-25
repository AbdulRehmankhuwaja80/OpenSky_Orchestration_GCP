# syntax=docker/dockerfile:1
FROM python:3.11-slim

WORKDIR /usr/app/dbt

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --default-timeout=180 --retries 10 dbt-bigquery==1.8.2

COPY dbt/air_quality_dbt ./air_quality_dbt

WORKDIR /usr/app/dbt/air_quality_dbt

# dbt looks for profiles.yml in ~/.dbt/ by default, not the project directory - ours lives
# alongside dbt_project.yml, so point DBT_PROFILES_DIR at it explicitly.
ENV DBT_PROFILES_DIR=/usr/app/dbt/air_quality_dbt

ENTRYPOINT ["dbt"]
CMD ["run"]
