# Used Car Listings - Big Data Analysis

This project focuses on analyzing large datasets of used car listings. Using Big Data technologies and a structured data pipeline based on the Medallion Architecture (Bronze, Silver, Gold layers), the project processes raw data, improves its quality, and prepares it for analysis.

## Architecture
### Overview

The system processes both batch and streaming data sources. Data is extracted using an extraction tool and follows two main flows:
- Batch Data: Extracted data is placed into the Raw Data Zone, then batch processed through the Transformation Zone and into the Curated Zone.
- Streaming Data: Real-time data is ingested into message queue topics, transformed in real-time, and then stored in the Curated Zone.

From the Curated Zone, the cleaned and structured data is used to build analysis dashboards.

The entire system is containerized and managed by an orchestration tool.

<p align="center">
  <img alt="Untitled Diagram drawio (8)" src="https://github.com/user-attachments/assets/bf5649bc-fe05-46ee-835b-05509d708f6a" />
</p>

### Tools and Components

**Batch Data Source**: 
- [Kaggle - US Used Cars Dataset](https://www.kaggle.com/datasets/ananaymital/us-used-cars-dataset)



**Real-time Data Source**: 
- Not yet implemented  


**Extraction Tool**: 
- Apache NiFi  


**Data Zones**:
- **Raw Zone (Bronze)**: 
  - HDFS (Hadoop Distributed File System)

- **Transformation Zone (Silver)**: 
  - HDFS (Hadoop Distributed File System)

- **Curated Zone (Gold)**: 
  - MongoDB  

 
**Batch Processing**: 
- Apache Spark  


**Stream Processing**: 
- Apache Kafka
- Apache Kafka Streams


**Dashboards**: 
- Metabase


**Containerization**:
- Docker
- Docker Compose


**Orchestration**:
- Apache Airflow

<p align="center">
  <img alt="Untitled Diagram drawio (7)" src="https://github.com/user-attachments/assets/b82b7d08-5487-4691-8b42-5e6ad44baeae" />
</p>

## Analytical Questions
### Batch Processing Questions
  1. What are the most popular cars by city, body type, and year?
  2. How does horsepower impact fuel consumption (city, highway, and combined)?
  3. How have vehicle prices evolved over time by model?
  4. How has the percentage distribution of vehicle offers by fuel type evolved over the months?
  5. How does vehicle size influence the likelihood of vehicle damage?

### Stream Processing Questions
[Comming Soon]

## Batch Processing
### Data Source
Source: [Kaggle - US Used Cars Dataset](https://www.kaggle.com/datasets/ananaymital/us-used-cars-dataset).


The dataset contains details of **3 million real-world used cars**.

The data was obtained by running a self-made **crawler** on Cargurus' inventory in **September 2020**. This dataset includes a wide range of features, such as car make, model, year, mileage, price, and more.

CSV format with 66 different columns.

<p align="center">
  <img alt="Kaggle_Logo" src="https://github.com/user-attachments/assets/d6136cb5-b270-4a0d-bd77-19aaf5a8fa81" />
</p>

### Extraction
Apache NiFi was used for the extraction process, which consists of the following phases:
1. Load batch data from a folder where new files arrive.
2. Split the data into smaller-sized files.
3. Rename the split files by adding handles for data and UUID.
4. Place the renamed files into the **Raw Data Zone** in HDFS.

<p align="center">
  <img alt="Kaggle_Logo" src="https://github.com/user-attachments/assets/1b8dcb5f-2b06-43c7-a61b-aea6fee4b0cc" />
</p>

<p align="center">
  <img alt="Screenshot_20250428_114800" src="https://github.com/user-attachments/assets/6938d212-0e64-4c1d-bd24-9a29bee2083c" />
</p>

> **Note**: There is logic for converting the files to **Avro** and **Parquet** formats, but it is not used due to the project specification.
<p align="center">
  <img alt="Screenshot_20250428_115237" src="https://github.com/user-attachments/assets/cbb07fa8-2bc0-4d47-8047-7d645f8d76ee" />
</p>

### Data Zones

The data pipeline follows the **Medallion Architecture**, which organizes the data into three distinct zones:
1. **Raw Data Zone (Bronze)**
2. **Transformation Zone (Silver)**
3. **Curated Zone (Gold)**

<p align="center">
  <img width="410" alt="Screenshot-2024-06-30-at-18 57 11" src="https://github.com/user-attachments/assets/c3a0c95f-4051-4d7d-b8d2-fda5294c9387" />
</p>

#### Raw Zone (Bronze)
The **Raw Zone** is located in HDFS and stores the raw data without any transformations. It serves as the initial repository for all incoming batch data, preserving it in its original format for future processing.

<p align="center">
  <img src="https://github.com/user-attachments/assets/2652d4ff-2c10-465d-b1a6-ebd06782e841" alt="Hadoop_logo_new" />
</p>

#### Transformation Zone (Silver)
The **Transformation Zone** is located in HDFS and contains data that has been cleaned and transformed into a format suitable for further processing and analysis. This zone ensures that the data is structured and ready for more advanced operations.

<p align="center">
  <img src="https://github.com/user-attachments/assets/2652d4ff-2c10-465d-b1a6-ebd06782e841" alt="Hadoop_logo_new" />
</p>

#### Curated Zone (Gold)
The **Curated Zone (Gold)** is located in MongoDB and it is where the final, high-quality data is stored. This zone is typically optimized for reporting, dashboarding, and machine learning tasks. The data in the **Curated Zone** is fully cleaned, aggregated, and enriched, ensuring that it's in the most usable form for advanced analysis. This data is ready for decision-making processes, business intelligence tools, and further data science operations.

<p align="center">
  <img src="https://github.com/user-attachments/assets/6c1c3335-8cb5-456f-939a-2d8b65e825ad" alt="MongoDB_Fores-Green" />
</p>

### Data Processing
Data is processed using **Apache Spark** and **Python (PySpark)** in two main phases for batch processing. Each phase corresponds to moving data from one zone to another within the Medallion Architecture, namely **Bronze**, **Silver**, and **Gold**.

<p align="center">
  <img src="https://github.com/user-attachments/assets/90ba6383-4e94-44f3-afb3-c358cdbc35c5" alt="Apache_Spark_logo" />
</p>

#### Phase 1: Data Cleaning (Raw to Transformation) – **Bronze to Silver**
The first phase involves cleaning the raw data in the **Raw Data Zone (Bronze)** and moving it into the **Transformation Zone (Silver)**. This is done with a single Spark job called **`clean_used_cars_data.py`**, which performs data cleansing, such as removing missing or inconsistent values, correcting data types, and preparing it for further transformation.

#### Phase 2: Data Transformation (Transformation to Curated) – **Silver to Gold**
After the data is cleaned and structured in the **Transformation Zone (Silver)**, it is further processed and enriched to move to the **Curated Zone (Gold)**. This phase consists of several Spark jobs, each designed to analyze and aggregate different aspects of the used car data. These jobs ensure that the data in the **Curated Zone (Gold)** is ready for advanced analysis and reporting.

The following five Spark jobs are used in this phase:
1. **`analyze_most_popular_vehicle_by_city_and_body_type.py`**: Analyzes the most popular vehicles by city, body type and year.
2. **`analyze_fuel_consumption_by_horsepower.py`**: Examines the relationship between fuel consumption and horsepower.
3. **`analyze_vehicle_prices_by_model.py`**: Analyzes vehicle prices based on the model.
4. **`analyze_vehicle_offer_by_fuel_type_and_month.py`**: Studies vehicle offers by fuel type and the month they were listed.
5. **`analyze_vehicle_damage_by_size.py`**: Analyzes vehicle damage based on the size of the vehicle.

These jobs help transform and enrich the data, making it ready for visualization, reporting, and advanced analysis in the **Curated Zone (Gold)**.

### Dashboards

The transformed and enriched data in the **Curated Zone (Gold)** is presented and visualized using **Metabase**, an open-source business intelligence tool. For each of the five Spark jobs, a dedicated dashboard is created to visualize the key insights and metrics derived from the processed data.

<p align="center">
  <img src="https://github.com/user-attachments/assets/62024e89-6f60-4ec7-b595-99dc22d3adad" width="200" alt="metabase-logo" />
</p>

Each dashboard focuses on specific aspects of the used car dataset, providing interactive visualizations that allow users to analyze and explore the data in depth. Below are the dashboards associated with each Spark job:

1. **Dashboard for `analyze_most_popular_vehicle_by_city_and_body_type.py`**:  
   - Visualizes the most popular vehicles by city and body type, providing insights into regional preferences and trends in car types.

<p align="center">
  <img src="https://github.com/user-attachments/assets/d11cac60-7d2f-42a2-bab4-8c319560f738" alt="Screenshot_20250428_124256" />
</p>

2. **Dashboard for `analyze_fuel_consumption_by_horsepower.py`**:  
   - Shows the relationship between fuel consumption and horsepower, helping users understand how vehicle performance impacts fuel efficiency.

<p align="center">
  <img src="https://github.com/user-attachments/assets/781599d5-b5f1-4615-8e51-4f7644b42fb5" alt="Screenshot_20250428_124517" />
</p>

3. **Dashboard for `analyze_vehicle_prices_by_model.py`**:  
   - Displays the pricing trends for different car models, allowing users to analyze price variations across models.

<p align="center">
  <img src="https://github.com/user-attachments/assets/88d66312-701b-426d-b785-7f87742dd971" alt="Screenshot_20250428_124442" />
</p>

4. **Dashboard for `analyze_vehicle_offer_by_fuel_type_and_month.py`**:  
   - Provides insights into vehicle offers based on fuel type and listing month, helping users track seasonal trends and fuel type preferences.

<p align="center">
  <img src="https://github.com/user-attachments/assets/3d734266-c1a1-4c25-90f8-56a638d86074" alt="Screenshot_20250428_124538" />
</p>

5. **Dashboard for `analyze_vehicle_damage_by_size.py`**:  
   - Visualizes vehicle damage patterns by vehicle size, giving users a better understanding of how vehicle size affects the likelihood of damage.

<p align="center">
  <img src="https://github.com/user-attachments/assets/07ec16bf-dc9b-480e-97dc-6a70b7a6ab0c" alt="Screenshot_20250428_124605" />
</p>

### Orchestration
Orchestration for the data processing pipeline is managed using **Apache Airflow**.

<p align="center">
  <img src="https://github.com/user-attachments/assets/259c84ba-a660-4afc-9d47-add28346d7ad" alt="AirflowLogo" />
</p>

A Directed Acyclic Graph (DAG) is created to orchestrate the flow of tasks. The DAG is structured in a way that ensures the following sequence:
1. **Data Cleaning**: The first task in the DAG triggers the execution of the **`clean_used_cars_data.py`** Spark job, which cleans and prepares the raw data in the **Raw Data Zone (Bronze)**.

2. **Parallel Data Processing**: Once the cleaning task is completed, the remaining tasks are executed in parallel. These tasks involve running five separate Spark jobs that analyze and transform the data, moving it from the **Transformation Zone (Silver)** to the **Curated Zone (Gold)**. The tasks are:
   - **`analyze_most_popular_vehicle_by_city_and_body_type.py`**
   - **`analyze_fuel_consumption_by_horsepower.py`**
   - **`analyze_vehicle_prices_by_model.py`**
   - **`analyze_vehicle_offer_by_fuel_type_and_month.py`**
   - **`analyze_vehicle_damage_by_size.py`**

<p align="center">
  <img src="https://github.com/user-attachments/assets/34d5daa7-6a2c-4a90-b6ca-1013af1b9ee8"alt="Screenshot_20250428_125531" />
</p>

## Stream Processing
[Comming soon]

## Containerization
Containerization of the application was achieved using Docker and Docker Compose.
<p align="center">
  <img src="https://github.com/user-attachments/assets/5b1fd6a3-22b3-464c-b01a-2fd035f74bbd" alt="Docker_logo" />
</p>

### Apache NiFi
<p align="center">
  <img alt="Kaggle_Logo" src="https://github.com/user-attachments/assets/1b8dcb5f-2b06-43c7-a61b-aea6fee4b0cc" />
</p>

The NiFi setup consists of the following containers:

- **niFi** (`apache/nifi:1.15.3`):
  - Manages data flows, data ingestion, and processing. Provides the main interface for creating and managing data pipelines.

- **nifi-registry** (`apache/nifi-registry:1.15.3`):
  - Stores and manages versioned NiFi data flows for tracking and version control of flow configurations.

### Hadoop
<p align="center">
  <img src="https://github.com/user-attachments/assets/2652d4ff-2c10-465d-b1a6-ebd06782e841" alt="Hadoop_logo_new" />
</p>
<p align="center">
  <img src="https://github.com/user-attachments/assets/b0481116-f441-4461-89be-eda0f12e72b3" alt="Hue_official_logo" />
</p>

The Hadoop setup consists of the following containers:

- **namenode** (`bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`):
  - Manages metadata and the HDFS directory structure. Only one **Namenode** is used.

- **datanode1 & datanode2** (`bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8`):
  - Store and manage the actual data. Two **Datanodes** are used for redundancy.

- **hue** (`gethue/hue:20201111-135001`):
  - Provides a web-based UI for interacting with the Hadoop ecosystem.

### MongoDB
<p align="center">
  <img src="https://github.com/user-attachments/assets/6c1c3335-8cb5-456f-939a-2d8b65e825ad" alt="MongoDB_Fores-Green" />
</p>

The MongoDB setup consists of the following containers:

- **mongodb** (`mongo:8.0`):
  - A NoSQL database container that stores data for the Curated Zone (Gold).

- **mongo-express** (`mongo-express:1.0.2-20-alpine3.19`):
  - A web-based UI for interacting with MongoDB, allowing for easy database management and viewing data.

### Apache Spark
<p align="center">
  <img src="https://github.com/user-attachments/assets/90ba6383-4e94-44f3-afb3-c358cdbc35c5" alt="Apache_Spark_logo" />
</p>

The Spark setup consists of the following containers:

- **spark-master** (`bitnami/spark:3.2.2`):
  - Manages the overall cluster, coordinates the job distribution, and acts as the main entry point for Spark applications. The **Spark Master** container exposes the necessary ports for both the web UI and Spark cluster communication.

- **spark-worker1, spark-worker2, and spark-worker3** (`bitnami/spark:3.2.2`):
  - These containers perform the actual computation and run the tasks assigned by the **Spark Master**. Each worker container is allocated specific resources (cores and memory) for parallel task execution.

### Metabase
<p align="center">
  <img src="https://github.com/user-attachments/assets/62024e89-6f60-4ec7-b595-99dc22d3adad" width="200" alt="metabase-logo" />
</p>

The Metabase setup consists of the following containers:

- **metabase** (`metabase/metabase:v0.53.x`):
  - Provides an easy-to-use interface for data visualization and analytics.
 
### Apache Airflow
<p align="center">
  <img src="https://github.com/user-attachments/assets/259c84ba-a660-4afc-9d47-add28346d7ad" alt="AirflowLogo" />
</p>
<p align="center">
  <img src="https://github.com/user-attachments/assets/aecff8b3-e950-44c8-a1b1-dec24a171143" width="200" alt="Postgresql_elephant" />
</p>

The Airflow setup consists of the following containers:

- **airflow-webserver** (`apache/airflow:2.7.1-python3.11`):
  - Provides the interface for users to manage and monitor workflows.

- **airflow-scheduler** (`apache/airflow:2.7.1-python3.11`):
  - Manages and schedules the execution of tasks in workflows.

- **postgres** (`postgres:14.0`):
  - A database used to store Airflow data.


