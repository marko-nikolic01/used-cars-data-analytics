import os
from airflow import settings
from airflow.models import Connection
from airflow.utils.db import provide_session

@provide_session
def create_ssh_connection(conn_id, host, login, password, port, session=None):
    conn_type = "SSH"

    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

    if existing_conn:
        print(f"Connection '{conn_id}' already exists, skipping.")
        return

    new_conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        login=login,
        password=password,
        port=port
    )

    session.merge(new_conn)
    session.commit()
    print(f"Connection '{conn_id}' created successfully.")

if __name__ == "__main__":
    create_ssh_connection("data_extractor_connector", "data-extractor", os.environ['DATA_EXTRACTOR_SSH_USERNAME'], os.environ['DATA_EXTRACTOR_SSH_PASSWORD'], 22)
    create_ssh_connection("spark_connector", "spark-master", os.environ['SPARK_SSH_USERNAME'], os.environ['SPARK_SSH_PASSWORD'], 22)
    create_ssh_connection("kafka_streams_connector", "kafka-streams", os.environ['KAFKA_STREAMS_SSH_USERNAME'], os.environ['KAFKA_STREAMS_SSH_PASSWORD'], 22)
