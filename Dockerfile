FROM apache/airflow:3.1.8-python3.12

# Pipeline libraries, installed against Airflow's own pinned constraints.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# dbt lives in its own virtualenv so its dependencies never clash with Airflow's.
# /opt is owned by root, so create the venv as root, then switch back to the airflow user.
USER root
RUN python -m venv /opt/dbt_venv \
 && /opt/dbt_venv/bin/pip install --no-cache-dir "dbt-core~=1.11.0" "dbt-postgres~=1.11.0" \
 && chown -R airflow:0 /opt/dbt_venv
USER airflow