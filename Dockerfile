# My image inherits from python image
FROM apache/airflow:2.6.2-python3.10

# Setup container working directory
WORKDIR /app

# Switch to root user for installation
USER root

# Install necessary packages
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-11-jdk \
        default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip3 install --upgrade pip

# Copy the MySQL Connector/J JAR file to the Spark JARs directory
COPY mysql-connector-j-8.0.33.jar /app/spark/jars/

# Switch back to the airflow user
USER airflow

# Install required Python packages
COPY . ./amazon_sale_2023
RUN pip3 install -r ./amazon_sale_2023/requirements.txt --user