FROM apache/airflow:3.3.2-python3.12
USER root
RUN python -m venv /opt/pipeline-venv && chown -R airflow:0 /opt/pipeline-venv
COPY requirements-dbt.txt requirements-postgres.txt requirements-aws.txt /tmp/pipeline-req/
USER airflow
RUN /opt/pipeline-venv/bin/pip install --no-cache-dir \
    -r /tmp/pipeline-req/requirements-dbt.txt \
    -r /tmp/pipeline-req/requirements-postgres.txt \
    -r /tmp/pipeline-req/requirements-aws.txt \
    && /opt/pipeline-venv/bin/pip check
