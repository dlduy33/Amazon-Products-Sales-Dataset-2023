from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import date

# Create Spark session
spark = SparkSession.builder.appName("Transform and Load data into DataWarehouse") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.kafka:kafka-clients:3.3.1") \
    .config("spark.jars", "mysql-connector-java-8.0.33.jar").getOrCreate()

# Set up Spark Log
spark.sparkContext.setLogLevel("ERROR")

# Take dataframe from Cassandra
def read_from_cassandra(table, keyspace, arg_month):
    df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table, keyspace=keyspace) \
        .option("spark.sql.query", f"SELECT * FROM {table} WHERE month = {arg_month}") \
        .load().drop("year", "month", "day")
    print(f"-----------{table}-----------")
    df.printSchema()
    df.show(10)
    return df

# Processing dataframes before create new ones
def processing_data(inventoryDf, productDf, orderDf, customerDf, addressDf):
    # Join dataframes into a dataframe
    print("----------------------------")
    print("| Start joinning dataframe |")
    df = inventoryDf.join(productDf.withColumnRenamed("id", "product_id"), inventoryDf.id == productDf.inventory_id, how="inner") \
        .join(orderDf, productDf.order_id == orderDf.id, how="inner") \
        .join(customerDf, orderDf.customer_id == customerDf.id, how="inner") \
        .join(addressDf, orderDf.city_id == addressDf.id.alias("city_id"), how="inner").drop("id")
    df = df.select("product_id", "product_name", "category", "type", "actual_price", "discount_price", "ratings", "no_of_ratings",
        "inventory_id", "inv_quantity", "quantity_sold", "link", "image", "customer_id", "customer_name", "gender", "email",
        "delivery_status", "segment", "city_id", "city", "state", "country", "order_id", "order_date", "ship_date", "timestamp",
        "timeuuid", "quarter")
    # Filter and fill Null data with some requirements
    print("------------------------------")
    print("| Start filter and fill Null |")
    print("------------------------------")
    df = df.filter(col("image").isNotNull())
    df = df.filter(col("link").isNotNull())
    df = df.filter(col("image").rlike("^https://.")).filter(col("link").rlike("^https://."))
    df = df.filter(col("actual_price").isNotNull()).filter(col("actual_price") != 0)
    df = df.fillna(value=0, subset=["ratings", "no_of_ratings", "discount_price"])
    df = df.filter(col("discount_price") < col("actual_price"))
    df.printSchema()
    df.show(10)
    return df

# Put processed data into Order table in data warehouse
def fact_Order(df):
    fact_Order = df.select("product_id", "customer_id", "city_id", "timeuuid")
    windowSpec_id = Window.orderBy("product_id")
    fact_Order = fact_Order.withColumn("id", row_number().over(windowSpec_id))
    fact_Order = fact_Order.select("id", "product_id", "customer_id", "city_id", "timeuuid").distinct()
    fact_Order.printSchema()
    fact_Order.show(10)
    return fact_Order

# Put processed data into Product table in data warehouse
def dim_Product(df):
    dim_Product = df.select(col("product_id").alias("id"), "product_name", "category", "type", "actual_price", "discount_price",
        "ratings", "no_of_ratings", "link", "image", "quantity_sold", "inv_quantity", "delivery_status").distinct()
    dim_Product.printSchema()
    dim_Product.show(10)
    return dim_Product

# Put processed data into Customer table in data warehouse
def dim_Customer(df):
    dim_Customer = df.select(col("customer_id").alias("id"), "customer_name", "gender", "email", "segment").distinct()
    dim_Customer.printSchema()
    dim_Customer.show(10)
    return dim_Customer

# Put processed data into Dilivery Location table in data warehouse
def dim_DiliveryLocation(df):
    dim_DiliveryLocation = df.select(col("city_id").alias("id"), "city", "state", "country").distinct()
    dim_DiliveryLocation.printSchema()
    dim_DiliveryLocation.show(10)
    return dim_DiliveryLocation

# Put processed data into Dilivery Location table in data warehouse
def dim_Date(df):
    quarter_name = concat(lit("Q"), "quarter").alias("quarter_name")
    month_name = date_format(to_timestamp("ship_date"), "MMMM").alias("month_name")
    weekday_name = date_format(to_timestamp("ship_date"), "EEEE").alias("weekday_name")
    dim_Date = df.select("timeuuid", year("ship_date").alias("year"), "quarter", quarter_name, month("ship_date").alias("month"),
                        month_name, dayofmonth("ship_date").alias("day"), weekday_name).distinct().sort("month", "day")
    dim_Date.printSchema()
    dim_Date.show(10)
    return dim_Date

def kafkaTopic_to_mysql(df, topic, schema, dbtable, arg_year, arg_month, arg_day):
    print("---------------------------------")
    print(f"| Process {dbtable} with Kafka |")
    print("---------------------------------")
    # Write data to Kafka topic
    KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
    df.selectExpr(f"CAST({df.columns[0]} AS STRING) AS key", "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
        .option("topic", topic) \
        .save()
    # Construct a streaming Dataframe that reads from topic
    lines = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
            .option("subscribe", topic) \
            .load()
    dbtable_data = lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .select(from_json("value", schema).alias("data")).select("data.*")
    dbtable_data.printSchema()
    print("--------------------------------------")
    print(f"| Batching write {dbtable} to mysql |")
    print("--------------------------------------")
    # Declare the parameters MySQL
    HOST = 'localhost'
    PORT = '3306'
    USER = "root"
    PASSWORD = "root"
    DB_NAME = "datawarehouse"
    URL = f'jdbc:mysql://{HOST}:{PORT}/{DB_NAME}'
    dbtable_data.withColumn("year", lit(arg_year)).withColumn("month", lit(arg_month)).withColumn("day", lit(arg_day)) \
        .write \
        .partitionBy("year", "month", "day") \
        .format("jdbc") \
        .option("url", URL) \
        .option("dbtable", f"{dbtable}") \
        .option("user", USER) \
        .option("password", PASSWORD) \
        .mode("append") \
        .save()
    print(f"| Successfully save {dbtable} table |")
    print("--------------------------------------")
    
if __name__ == '__main__':
    print("-------------------------------------")
    print("| Start reading data from Cassandra |")
    print("-------------------------------------")
    # Take the argument and partition them
    arg = str(date.today())
    runTime = arg.split("-")
    arg_year = runTime[0]
    arg_month = runTime[1]
    arg_day = runTime[2]
    # Declare table names for Cassandra
    KEYSPACE = "datalake"
    INVENTORY_TABLE = "inventory"
    PRODUCT_TABLE = "product"
    ORDER_TABLE = "orderr"
    CUSTOMER_TABLE = "customer"
    ADDRESS_TABLE = "address"
    inventoryDf = read_from_cassandra(INVENTORY_TABLE, KEYSPACE, arg_month)
    productDf = read_from_cassandra(PRODUCT_TABLE, KEYSPACE, arg_month)
    orderDf = read_from_cassandra(ORDER_TABLE, KEYSPACE, arg_month)
    customerDf = read_from_cassandra(CUSTOMER_TABLE, KEYSPACE, arg_month)
    addressDf = read_from_cassandra(ADDRESS_TABLE, KEYSPACE, arg_month)
    print("----------------------")
    print("| Start process data |")
    df = processing_data(inventoryDf, productDf, orderDf, customerDf, addressDf)
    print("-----------------------------------")
    print("| Start put data into Order table |")
    print("-----------------------------------")
    order_table = fact_Order(df)
    print("-------------------------------------")
    print("| Start put data into Product table |")
    print("-------------------------------------")
    product_table = dim_Product(df)
    print("--------------------------------------")
    print("| Start put data into Customer table |")
    print("--------------------------------------")
    customer_table = dim_Customer(df)
    print("-----------------------------------------------")
    print("| Start put data into Dilivery Location table |")
    print("-----------------------------------------------")
    dilivery_location_table = dim_DiliveryLocation(df)
    print("----------------------------------")
    print("| Start put data into Date table |")
    print("----------------------------------")
    date_table = dim_Date(df)
    print("----------------------")
    print("| Start Data Process |") 
    # Define schema for the data
    orderSchema = StructType([StructField("id", IntegerType(), True),
                    StructField("product_id", IntegerType(), True),
                    StructField("customer_id", IntegerType(), True),
                    StructField("city_id", IntegerType(), True),
                    StructField("timeuuid", StringType(), True)])
    productSchema = StructType([StructField("id", IntegerType(), True),
                    StructField("product_name", StringType(), True),
                    StructField("category", StringType(), True),
                    StructField("type", IntegerType(), True),
                    StructField("actual_price", IntegerType(), True),
                    StructField("discount_price", IntegerType(), True),
                    StructField("ratings", DoubleType(), True),
                    StructField("no_of_ratings", DoubleType(), True),
                    StructField("link", StringType(), True),
                    StructField("image", StringType(), True),
                    StructField("quantity_sold", IntegerType(), True),
                    StructField("inv_quantity", IntegerType(), True),
                    StructField("delivery_status", StringType(), True)])
    customerSchema = StructType([StructField("id", IntegerType(), True),
                    StructField("customer_name", StringType(), True),
                    StructField("gender", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("segment", StringType(), True)])
    dilivery_locationSchema = StructType([StructField("id", IntegerType(), True),
                    StructField("city", StringType(), True),
                    StructField("state", StringType(), True),
                    StructField("country", StringType(), True)])
    dateSchema = StructType([StructField("timeuuid", StringType(), True),
                    StructField("year", IntegerType(), True),
                    StructField("quarter", IntegerType(), True),
                    StructField("quarter_name", StringType(), True),
                    StructField("month", IntegerType(), True),
                    StructField("month_name", StringType(), True),
                    StructField("day", IntegerType(), True),
                    StructField("weekday_name", StringType(), True)])
    # Declare table names for MySQL
    ORDER_TOPIC = "cass-mysql-factOrder"
    PRODUCT_TOPIC = "cass-mysql-dimProduct"
    CUSTOMER_TOPIC = "cass-mysql-dimCustomer"
    DILI_LOCA_TOPIC = "cass-mysql-dimDiliveryLocation"
    DATE_TOPIC = "cass-mysql-dimDate"
    ORDER_TABLE = "factOrder"
    PRODUCT_TABLE = "dimProduct"
    CUSTOMER_TABLE = "dimCustomer"
    DILI_LOCA_TABLE = "dimDiliveryLocation"
    DATA_TABLE = "dimDate"
    # Start to transform data
    order_to_mysql = kafkaTopic_to_mysql(order_table, ORDER_TOPIC, orderSchema, ORDER_TABLE, arg_year, arg_month, arg_day)
    product_to_mysql = kafkaTopic_to_mysql(product_table, PRODUCT_TOPIC, productSchema, PRODUCT_TABLE,
                                                arg_year, arg_month, arg_day)
    customer_to_mysql = kafkaTopic_to_mysql(customer_table, CUSTOMER_TOPIC, customerSchema, CUSTOMER_TABLE,
                                                arg_year, arg_month, arg_day)
    dilivery_location_to_mysql = kafkaTopic_to_mysql(dilivery_location_table, DILI_LOCA_TOPIC, dilivery_locationSchema,
                                                DILI_LOCA_TABLE, arg_year, arg_month, arg_day)
    date_to_mysql = kafkaTopic_to_mysql(date_table, DATE_TOPIC, dateSchema, DATA_TABLE, arg_year, arg_month, arg_day)
    print("| Successful Processing |")
    print("-------------------------")