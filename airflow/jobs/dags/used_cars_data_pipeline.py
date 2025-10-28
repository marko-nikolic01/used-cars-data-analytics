import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

default_args = {
    'owner': 'airflow',
    'retries': 0,
}

data_extractor_env_vars = {
    "KAFKA_BROKER": "kafka:9092",
    "DOWNLOAD_STREAM_DATA": os.getenv("DOWNLOAD_STREAM_DATA", "N"),
    "DOWNLOAD_STREAM_DATA_START_PAGE": os.getenv("DOWNLOAD_STREAM_DATA_START_PAGE", "1"),
    "DOWNLOAD_STREAM_DATA_END_PAGE": os.getenv("DOWNLOAD_STREAM_DATA_END_PAGE", "1000"),
    "DOWNLOAD_STREAM_DATA_PAGE_SIZE": os.getenv("DOWNLOAD_STREAM_DATA_PAGE_SIZE", "100"),
    "AUTO_DEV_API_KEY": os.getenv("AUTO_DEV_API_KEY", ""),
}
data_extractor_env_exports = " && ".join([f"export {k}={v}" for k, v in data_extractor_env_vars.items()])

spark_env_vars = {
    "JAVA_HOME": "/opt/java/openjdk",
    "CORE_CONF_fs_defaultFS": "hdfs://namenode:9000",
    "CORE_CONF_hadoop_http_staticuser_user": "root",
    "CORE_CONF_hadoop_proxyuser_hue_hosts": "*",
    "CORE_CONF_hadoop_proxyuser_hue_groups": "*",
    "HDFS_CONF_dfs_webhdfs_enabled": "true",
    "HDFS_CONF_dfs_permissions_enabled": "false",
    "YARN_CONF_yarn_log___aggregation___enable": "true",
    "YARN_CONF_yarn_resourcemanager_recovery_enabled": "true",
    "YARN_CONF_yarn_resourcemanager_store_class": "org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore",
    "YARN_CONF_yarn_resourcemanager_fs_state___store_uri": "/rmstate",
    "YARN_CONF_yarn_nodemanager_remote___app___log___dir": "/app-logs",
    "YARN_CONF_yarn_log_server_url": "http://historyserver:8188/applicationhistory/logs/",
    "YARN_CONF_yarn_timeline___service_enabled": "true",
    "YARN_CONF_yarn_timeline___service_generic___application___history_enabled": "true",
    "YARN_CONF_yarn_resourcemanager_system___metrics___publisher_enabled": "true",
    "YARN_CONF_yarn_resourcemanager_hostname": "resourcemanager",
    "YARN_CONF_yarn_timeline___service_hostname": "historyserver",
    "YARN_CONF_yarn_resourcemanager_address": "resourcemanager:8032",
    "YARN_CONF_yarn_resourcemanager_scheduler_address": "resourcemanager:8030",
    "YARN_CONF_yarn_resourcemanager_resource__tracker_address": "resourcemanager:8031",
    "PYSPARK_PYTHON": "python3",
    "MONGO_URI": "mongodb://mongodb:27017"
}
spark_env_exports = " && ".join([f"export {k}={v}" for k, v in spark_env_vars.items()])

kafka_streams_env_vars = {
    "JAVA_HOME": "/usr/local/openjdk-17",
    "PATH": "$JAVA_HOME/bin:$PATH"
}
kafka_streams_env_exports = " && ".join([f"export {k}={v}" for k, v in kafka_streams_env_vars.items()])

def build_data_extractor_command(file: str, detached: bool):
    base_cmd = f"{data_extractor_env_exports} && cd /app && python /app/{file}.py"
    if detached:
        return f"nohup bash -c '{base_cmd}' > /dev/null 2>&1 < /dev/null & disown"
    return base_cmd

def build_spark_command(file: str, language: str):
    base_cmd = f"{spark_env_exports} && /opt/spark/bin/spark-submit " \
               f"--master spark://spark-master:7077 " \
               f"--deploy-mode client " \
               f"--packages org.mongodb.spark:mongo-spark-connector_2.12:10.5.0 "
    if language.lower() == "python":
        return base_cmd + f"/opt/spark/jobs/{file}.py"
    elif language.lower() == "scala":
        jar_path = f"/opt/spark/jobs/{file}/target/scala-2.12/{file}_2.12-0.1.0-SNAPSHOT.jar"
        main_class = "Main"
        return base_cmd + f"--class {main_class} {jar_path}"
    else:
        raise ValueError(f"Unsupported language: {language}")
    
def build_kafka_streams_command(file: str, detached: bool):
    base_cmd = f"{kafka_streams_env_exports} && java -jar /app/{file}-1.0-SNAPSHOT.jar"
    if detached:
        return f"nohup bash -c '{base_cmd}' > /dev/null 2>&1 < /dev/null & disown"
    return base_cmd

def create_root_job():
    return EmptyOperator(task_id='run-used-cars-data-pipeline')

def create_data_extractor_job(file: str, job_name: str, detached: bool):
    return SSHOperator(
        task_id=f'run-{job_name}',
        ssh_hook=SSHHook(ssh_conn_id='data_extractor_connector', cmd_timeout=3600),
        command=build_data_extractor_command(file, detached),
        execution_timeout=timedelta(hours=1)
    )

def create_spark_job(file: str, job_name: str, language: str):
    return SSHOperator(
        task_id=f'run-{job_name}',
        ssh_hook=SSHHook(ssh_conn_id='spark_connector', cmd_timeout=3600),
        command=build_spark_command(file, language),
        execution_timeout=timedelta(hours=1)
    )

def create_kafka_streams_job(file: str, job_name: str, detached: bool):
    return SSHOperator(
        task_id=f'run-{job_name}',
        ssh_hook=SSHHook(ssh_conn_id='kafka_streams_connector', cmd_timeout=3600),
        command=build_kafka_streams_command(file, detached),
        execution_timeout=timedelta(hours=1)
    )

with DAG(
    dag_id='used_cars_data_pipeline',
    default_args=default_args,
    description='Run used cars data pipeline',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    root_job = create_root_job()
    
    extract_batch_data_job_config = ('extract_batch_used_cars_data', 'extract-batch-used-cars-data', False)
    extract_batch_data_job = create_data_extractor_job(extract_batch_data_job_config[0], extract_batch_data_job_config[1], extract_batch_data_job_config[2])

    extract_stream_data_job_config = ('extract_stream_used_cars_data', 'extract-stream-used-cars-data', False)
    extract_stream_data_job = create_data_extractor_job(extract_stream_data_job_config[0], extract_stream_data_job_config[1], extract_stream_data_job_config[2])

    create_kafka_topics_job_config = ('create_used_cars_data_stream_kafka_topics', 'create-used-cars-data-stream-kafka-topics', False)
    create_kafka_topics_job = create_data_extractor_job(create_kafka_topics_job_config[0], create_kafka_topics_job_config[1], create_kafka_topics_job_config[2])

    create_data_stream_job_config = ('create_used_cars_data_stream', 'create-used-cars-data-stream', True)
    create_data_stream_job = create_data_extractor_job(create_data_stream_job_config[0], create_data_stream_job_config[1], create_data_stream_job_config[2])

    clean_batch_data_job_config = ('clean_used_cars_data', 'clean-batch-used-cars-data', 'python')
    clean_batch_data_job = create_spark_job(clean_batch_data_job_config[0], clean_batch_data_job_config[1], clean_batch_data_job_config[2])

    clean_stream_data_job_config = ('clean-used-cars-data', 'clean-stream-used-cars-data', True)
    clean_stream_data_job = create_kafka_streams_job(clean_stream_data_job_config[0], clean_stream_data_job_config[1], clean_stream_data_job_config[2])

    transform_batch_data_jobs_config = [
        ('analzye_most_popular_vehicle_by_city_and_body_type', 'analyze-most-popular-vehicle-by-city-and-body-type', 'python'),
        ('analzye_fuel_consumption_by_horsepower', 'analyze-fuel-consumption-by-horsepower', 'python'),
        ('analyze_vehicle_prices_by_model', 'analyze-vehicle-prices-by-model', 'python'),
        ('analyze_vehicle_offer_by_fuel_type_and_month', 'analyze-vehicle-offer-by-fuel-type-and-month', 'python'),
        ('analyze_vehicle_damage_by_size', 'analyze-vehicle-damage-by-size', 'python'),
        ('analyzevehiclepricedistribution', 'analyze-vehicle-price-distribution', 'scala'),
        ('analyzevehiclecolorimpactonpriceanddaysonmarket', 'analyze-vehicle-color-impact-on-price-and-days-on-market', 'scala'),
        ('analyzevehicleage', 'analyze-vehicle-age', 'scala'),
        ('analyzeownercountimpactondaysonmarket', 'analyze-owner-count-impact-on-days-on-market', 'scala'),
        ('analyzebodytypepercity', 'analyze-body-type-per-city', 'scala'),
    ]
    transform_batch_data_jobs = [create_spark_job(file, job_name, language) for file, job_name, language in transform_batch_data_jobs_config]

    transform_stream_data_jobs_config = [
        ('calculate-average-price-trends-pet-state', 'calculate-average-price-trends-pet-state', False)
    ]
    transform_stream_data_jobs = [create_kafka_streams_job(file, job_name, detached) for file, job_name, detached in transform_stream_data_jobs_config]

    root_job >> extract_batch_data_job >> clean_batch_data_job >> transform_batch_data_jobs
    root_job >> extract_stream_data_job >> create_kafka_topics_job >> create_data_stream_job >> clean_stream_data_job >> transform_stream_data_jobs
