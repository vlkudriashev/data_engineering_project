FROM apache/airflow:2.7.1

COPY ./app_airflow/requirements.txt /

# Install dependencies
RUN pip install -r /requirements.txt
RUN pip install --no-dependencies airflow-clickhouse-plugin==1.1.0

# Copy files needed for further configuration
COPY ./app_airflow/.airflowignore /opt/airflow/dags
