# Amazon-Sales-Product-Dataset-2023

## Introduction
This project is to build a batching ETL pipeline to extract sales results every week from the Kaggle Amazon product sales dataset 2023 by using Python, Spark, MySQL, Cassandra, Kafka, Airflow, and Docker.
<br>
![image](https://github.com/dlduy33/Amazon-Products-Sales-Dataset-2023/assets/131146326/1354806f-34fb-4b46-9ea0-7b65aebbf71d)

## Dataset
Data is an Amazon sales product dataset 2023 found from Kaggle that includes fields such as main_category,  sub_category, name, actual_price, and discount_price. Then, I assumed and added more fields to increase the difficulty of the problem.

## Technologies
- Apache Spark: Key framework to build ETL process for project
- MySQL: Used as Data Warehouse 
- Cassandra: Used as Data Lake
- Apache Kafka: is applied to the assumption of distributed processing in the ingestion data and load data in the ETL process (the main purpose of learning Kafka)
- Apache Airflow: Used as a workflow manager, DAG is scheduled to run ETL to Data Warehouse and ETL to Data Mart at 00:00 on Monday (as Kafka, Airflow is used for learning purposes).
- Docker:
  - [Dockerfile](https://github.com/dlduy33/Amazon-Products-Sales-Dataset-2023/blob/main/Dockerfile): Create an image and copy all data for Apache Airflow with the "apache/airflow:2.6.2-python3.10" image.
  - [Docker compose](https://github.com/dlduy33/Amazon-Products-Sales-Dataset-2023/blob/main/docker-compose.yml): Images are used for each tool as follows:
    - Airflow: apache/airflow:2.6.2-python3.10
    - Spark: docker.io/bitnami/spark:3.3
    - Cassandra: cassandra:4.1.1
    - MySQL: mysql:8.0.34
    - Kafka: confluentinc/cp-kafka:7.3.2
  
## Process description
- Data Ingestion:
  - This phase consists of two parts:
    - First, this step assumes the act of retrieving data from a Database having [schema](https://github.com/dlduy33/Amazon-Products-Sales-Dataset-2023/blob/main/Source%20System%20Schema.png) created based on available as well as fields were added more while the ingestion data process.
    - Second, This is the stage of moving the newly created data from the Database and saving them following partition keys to the Data Lake (Cassandra) through Kafka topics.
- ETL to Data Warehouse:
  - This is a process that extracts data from Data Lake (Cassandra) following partitions and transforms them based on [Star schema](https://github.com/dlduy33/Amazon-Products-Sales-Dataset-2023/blob/main/Star%20Schema%20Amazon.png), and load data into Data Warehouse (MySQL)
- ETL to Data Mart:
  - As process above, this process still takes data from Data Lake (Cassandra) following partitions and transforms to take data frame to serve the analysis of Revenue, Order, and Customer.
