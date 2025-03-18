from airflow import settings
from airflow.models import Connection
from airflow.utils.db import provide_session

@provide_session
def create_ssh_connection(session=None):
    conn_id = "spark_connector"
    conn_type = "SSH"
    host = "spark-master"
    schema = ""
    login = "root"
    password = "rootpassword"
    port = 22

    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

    if not existing_conn:
        new_conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            login=login,
            password=password,
            port=port
        )
        session.add(new_conn)
        session.commit()

if __name__ == "__main__":
    create_ssh_connection()
