from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from randomtimestamp import *
from datetime import date
import random
import uuid
import glob

# Create spark connecting cassandra
spark = SparkSession.builder.appName("Ingestion - from SOURCE to KAFKA to CASSANDRA").master("local") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.3.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.apache.kafka:kafka-clients:3.3.1") \
    .config("spark.jars", "mysql-connector-java-8.0.33.jar") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "1") \
    .getOrCreate()

# Set up Spark Logs
spark.sparkContext.setLogLevel("ERROR")

# Combine files from Kaggle
def combine_files(path):
    files = glob.glob(path)
    df = spark.read.csv(files[0], header=True)
    for file in files[1:]:
        df_1 = spark.read.csv(file, header=True).drop(col("_c0"))
        df = df.union(df_1)
    df = df.withColumn("actual_price", regexp_replace("actual_price", "[^\d]+", "")) \
            .withColumn("discount_price", regexp_replace("discount_price", "[^\d]+", ""))
    # Read files containing old data
    df = df.withColumnRenamed("main_category", "category")
    df = df.withColumnRenamed("sub_category", "type")
    df = df.withColumnRenamed("name", "product_name")
    df = df.withColumn("actual_price", col("actual_price").cast("int"))
    df = df.withColumn("discount_price", col("discount_price").cast("int"))
    df.printSchema()
    return df

# Incremental loading
def check_input(df, table, keyspace):
    # Take largest id from cassandra
    old_df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table=table, keyspace=keyspace) \
    .load()
    old_id = old_df.agg(max("id")).head()[0]
    # Take latest id from cassandra
    windowSpec_id = Window.orderBy("product_name")
    df = df.withColumn("id", row_number().over(windowSpec_id))
    new_id = df.agg(max("id")).head()[0]
    # Compare them
    if new_id > old_id:
        print(f"| new id:{new_id} > old id:{old_id} |")
        print("|-----Data are already process------|")
        return True
    return False

# Process data and create inventory table based on database schema
def process_inventoryDf(df):
    inventoryDf = df.select("product_name", "category", "type").distinct()
    inventoryDf = inventoryDf.withColumn("inv_quantity", (rand() * 9950 + 50).cast("integer")) \
                            .withColumn("quantity_sold", (rand() * 9950 + 50).cast("integer"))
    windowSpec_iventoryid = Window.orderBy("product_name")
    inventoryDf = inventoryDf.withColumn("id", row_number().over(windowSpec_iventoryid))
    inventoryDf = inventoryDf.select("id", "inv_quantity", "quantity_sold")
    inventoryDf.printSchema()
    return inventoryDf

# Process data and create product table based on database schema
def process_productDf(df):
    windowSpec_productid = Window.orderBy("product_name")
    product_id = df.select("product_name", "category", "type").distinct()
    product_id = product_id.withColumn("id", row_number().over(windowSpec_productid))
    product_id = product_id.withColumn("inventory_id", row_number().over(windowSpec_productid))
    product_id = product_id.withColumn("order_id", row_number().over(windowSpec_productid))
    productDf = df.join(product_id, on=["product_name", "category", "type"], how="inner")
    productDf = productDf.select("id", "product_name", "category", "type", "actual_price", "discount_price", "ratings",
            "no_of_ratings", "link", "image", "inventory_id", "order_id")
    productDf.printSchema()
    return productDf

# Process data and create customer table based on database schema
def process_customerDf(df):
    # Create column customer_name
    names = spark.read.csv(r"./Customer-name.csv", header=True, inferSchema=True)
    names = names.select(collect_set("name")).first()[0]
    name_udf = udf(lambda: random.choice(names), StringType())
    customerDf = df.withColumn("customer_name",name_udf())
    # Create column gender
    gender_list = ["Male", "Female", "Other"]
    gender_udf = udf(lambda: random.choice(gender_list), StringType())
    customerDf = customerDf.withColumn("gender",gender_udf())
    # Create column email
    email_domain = ["Gmail.com", "Yahoo.com", "Outlook.com", "iCloud.com"]
    email_domain_udf = udf(lambda: random.choice(email_domain), StringType())
    randint_udf = udf(lambda: random.randint(100,500), StringType())
    customerDf = customerDf.withColumn("email", 
            concat((split(col("customer_name"), " ").getItem(0)), lit("_"), randint_udf(), lit("@"), email_domain_udf()))
    # Create column segment
    segment_list = ["Home Office", "Consumer", "Corporate"]
    segment_udf = udf(lambda: random.choice(segment_list), StringType())
    customerDf = customerDf.withColumn("segment", segment_udf())
    # Prepare for customer_id creation step
    customerDf = customerDf.select("customer_name", "gender", "email", "segment").distinct()
    # Create customer_id
    windowSpec_id = Window.orderBy("customer_name")
    customerDf = customerDf.withColumn("id", row_number().over(windowSpec_id))
    customerDf = customerDf.select("id", "customer_name", "gender", "email", "segment")
    customerDf.printSchema()
    return customerDf

# Process data and create address table based on database schema
def process_addressDf():
    # Create column city_id, city, state
    addressDf = spark.read.csv(r"./US-Cities-100K+.csv", header=True, inferSchema=True).select("city", "state")
    windowSpec_address = Window.orderBy("city", "state")
    addressDf = addressDf.withColumn("id", row_number().over(windowSpec_address))
    addressDf = addressDf.select(concat_ws(" - ", "id", "city", "state"))
    address_set = addressDf.select(collect_set("concat_ws( - , id, city, state)")).first()[0]
    address_udf = udf(lambda: random.choice(address_set), StringType())
    addressDf = addressDf.withColumn("address", address_udf())
    addressDf = addressDf.withColumn("id", split(addressDf["address"], " - ").getItem(0)).withColumn("id", col("id").cast("int")) \
        .withColumn("city", split(addressDf["address"], " - ").getItem(1)) \
    .withColumn("state", split(addressDf["address"], " - ").getItem(2))
    # Create column country 
    COUNTRY = "United States"
    addressDf = addressDf.withColumn("country", lit(COUNTRY))
    # Create column customer_id
    addressDf = addressDf.select("id", "city", "state", "country")
    addressDf.printSchema()
    return addressDf

# Process data and create order table based on database schema
def process_orderDf(df, customerDf, addressDf):
    # Create column order_id
    windowSpec_orderid = Window.orderBy("product_name")
    order_id = df.select("product_name", "category", "type").distinct()
    orderDf = order_id.withColumn("id", row_number().over(windowSpec_orderid))
    # Create column timestamp
    timestamp_udf = udf(lambda: randomtimestamp(start_year=2023, text=True, pattern="%Y-%m-%d %H:%M:%S"), StringType())
    orderDf = orderDf.withColumn("timestamp", to_timestamp(timestamp_udf()))
    # Create column timeuuid
    uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())
    orderDf = orderDf.withColumn("timeuuid", uuid_udf())
    # Create column order_date
    orderDf = orderDf.withColumn("order_date", to_timestamp(timestamp_udf())) \
                    .withColumn("order_date", col("order_date").cast("timestamp"))
    # Create column ship_date
    randdate_udf = udf(lambda: random.randint(1, 7), IntegerType())
    orderDf = orderDf.withColumn("ship_date", date_add(col("order_date"), randdate_udf())) \
                    .withColumn("ship_date", col("ship_date").cast("timestamp"))
    # Create column quarter
    orderDf = orderDf.withColumn("quarter", when(month("timestamp").between(1, 3), 1)
                                .when(month("timestamp").between(4, 6), 2)
                                .when(month("timestamp").between(7, 9), 3)
                                .otherwise(4))
    # Create column delivery_status
    delivery_list = ["Preparing", "Shipped", "Delivered", "Undelivered"]
    delivery_udf = udf(lambda: random.choices(delivery_list, weights=(15, 10, 80, 5))[0], StringType())
    orderDf = orderDf.withColumn("delivery_status", delivery_udf())
    # Create column city_id
    total_cities = addressDf.select("id").count()
    orderDf = orderDf.withColumn("city_id", (rand() * total_cities + 1).cast("integer"))
    # Create column customer_id
    total_customers = customerDf.select("id").count()
    orderDf = orderDf.withColumn("customer_id", (rand() * total_customers + 1).cast("integer"))

    orderDf = orderDf.select("id", "customer_id", "order_date", "ship_date", "timestamp", "timeuuid", "quarter",
                            "delivery_status", "city_id")
    orderDf.printSchema()
    return orderDf

# Process data and create customerAddress table based on database schema
def process_customerAddressDf(customerDf, addressDf):
    customer_id = customerDf.select(col("id").alias("customer_id")).distinct()
    address_id = addressDf.select(col("id").alias("address_id")).distinct()
    customerAddressDf = customer_id.join(address_id)
    customerAddressDf.printSchema()
    return customerAddressDf

def source_to_cassandra(df, topic, schema, table, keyspace):
    print("-------------------------------")
    print(f"| Process {table} with Kafka |")
    print("-------------------------------")
    # Write data to Kafka topic
    KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
    df.selectExpr(f"CAST({df.columns[0]} AS STRING) AS key", "to_json(struct(*)) AS value") \
      .write \
      .format("kafka") \
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
      .option("topic", topic) \
      .save()
    # Construct a batching Dataframe that reads from topic
    lines = spark.read \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
                .option("subscribe", topic) \
                .option("startingOffsets", "earliest") \
                .option("endingOffsets", "latest") \
                .load()
    # Take and convert data from Kafka topic
    table_data = lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
                    .select(from_json("value", schema).alias("data")).select("data.*")
    table_data.printSchema()
    table_data = table_data.filter(col(f"{table_data.columns[0]}").isNotNull())
    print("----------------------------------------")
    print(f"| Batching write {table} to cassandra |")
    print("----------------------------------------")
    # Batching write into Cassandra database
    spark.conf.set("spark.cassandra.output.batch.size.rows", "1000") # increase batch size from default 100 to 1000
    arg = str(date.today())
    runTime = arg.split("-")
    arg_year = runTime[0]
    arg_month = runTime[1]
    arg_day = runTime[2]
    table_data.withColumn("year", lit(arg_year)).withColumn("month", lit(arg_month)).withColumn("day", lit(arg_day)) \
                .write \
                .partitionBy("year", "month", "day") \
                .format("org.apache.spark.sql.cassandra") \
                .options(table=table, keyspace=keyspace) \
                .mode("append") \
                .save()
    print(f"| Successfully save {table} table |")
    print("------------------------------------")
    
if __name__ == "__main__":
    print("-------------------------------------------------------")
    print("| Process data and increase the difficulty of project |")
    print("-------------------------------------------------------")
    path = r"./amazon_sale_2023/Dataset/Kaggle_data/*.csv"
    df = combine_files(path)
    print("-----------------------------------------------------------------")
    print("| Start to check to prepare for the Incremental loading process |")
    print("-----------------------------------------------------------------")
    PRODUCT_TABLE = "product"
    KEYSPACE = "datalake"
    # check_value = check_input(df, PRODUCT_TABLE, KEYSPACE)
    check_value = True
    if check_value:
        print("-------------------------------------")
        print("| Start process inventory dataframe |")
        print("-------------------------------------")
        inventoryDf = process_inventoryDf(df)
        print("-------------------------------------")
        print("| Start process productDf dataframe |")
        print("-------------------------------------")
        productDf = process_productDf(df)
        print("--------------------------------------")
        print("| Start process customerDf dataframe |")
        print("--------------------------------------")
        customerDf = process_customerDf(df)
        print("-------------------------------------")
        print("| Start process addressDf dataframe |")
        print("-------------------------------------")
        addressDf = process_addressDf()
        print("-----------------------------------")
        print("| Start process orderDf dataframe |")
        print("-----------------------------------")
        orderDf = process_orderDf(df, customerDf, addressDf)
        print("---------------------------------------------")
        print("| Start process customerAddressDf dataframe |")
        print("---------------------------------------------")
        customerAddressDf = process_customerAddressDf(customerDf, addressDf)
        print("---------------------------")
        print("| Start Data Transmission |")
        # Define a schema for the data
        inventorySchema = StructType([StructField("id", IntegerType(), True),
                            StructField("inv_quantity", IntegerType(), True),
                            StructField("quantity_sold", IntegerType(), True)])
        productSchema = StructType([StructField("id", IntegerType(), True),
                            StructField("product_name", StringType(), True),
                            StructField("category", StringType(), True),
                            StructField("type", StringType(), True),
                            StructField("actual_price", IntegerType(), True),
                            StructField("discount_price", IntegerType(), True),
                            StructField("ratings", IntegerType(), True),
                            StructField("no_of_ratings", IntegerType(), True),
                            StructField("link", StringType(), True),
                            StructField("image", StringType(), True),
                            StructField("inventory_id", IntegerType(), True),
                            StructField("order_id", IntegerType(), True)])
        orderSchema = StructType([StructField("id", IntegerType(), True),
                            StructField("customer_id", IntegerType(), True),
                            StructField("order_date", TimestampType(), True),
                            StructField("ship_date", TimestampType(), True),
                            StructField("timestamp", TimestampType(), True),
                            StructField("timeuuid", StringType(), True),
                            StructField("quarter", IntegerType(), True),
                            StructField("delivery_status", StringType(), True),
                            StructField("city_id", IntegerType(), True)])
        customerSchema = StructType([StructField("id", IntegerType(), True),
                            StructField("customer_name", StringType(), True),
                            StructField("gender", StringType(), True),
                            StructField("email", StringType(), True),
                            StructField("segment", StringType(), True)])
        addressSchema = StructType([StructField("id", IntegerType(), True),
                            StructField("city", StringType(), True),
                            StructField("state", StringType(), True),
                            StructField("country", StringType(), True)])
        custAddrSchema = StructType([StructField("customer_id", IntegerType(), True),
                            StructField("address_id", IntegerType(), True)])
        # Declare the parameters
        TOPIC_INVENTORY = "source-cass-inventory"
        TOPIC_PRODUCT = "source-cass-product"
        TOPIC_ORDER = "source-cass-order"
        TOPIC_CUSTOMER = "source-cass-customer"
        TOPIC_ADDRESS = "source-cass-address"
        TOPIC_CUST_ADD = "source-cass-customerAddress"
        INVENTORY_TABLE = "inventory"
        ORDER_TABLE = "orderr"
        CUSTOMER_TABLE = "customer"
        ADDRESS_TABLE = "address"
        CUST_ADD_TABLE = "customerAddress"
        inventoryDf_to_cass = source_to_cassandra(inventoryDf, TOPIC_INVENTORY, inventorySchema, INVENTORY_TABLE, KEYSPACE)
        productDf_to_cass = source_to_cassandra(productDf, TOPIC_PRODUCT, productSchema, PRODUCT_TABLE, KEYSPACE)
        orderDf_to_cass = source_to_cassandra(orderDf, TOPIC_ORDER, orderSchema, ORDER_TABLE, KEYSPACE)
        customerDf_to_cass = source_to_cassandra(customerDf, TOPIC_CUSTOMER, customerSchema, CUSTOMER_TABLE, KEYSPACE)
        addressDf_to_cass = source_to_cassandra(addressDf, TOPIC_ADDRESS, addressSchema, ADDRESS_TABLE, KEYSPACE)
        customerAddressDf_to_cass = source_to_cassandra(customerAddressDf, TOPIC_CUST_ADD, custAddrSchema, CUST_ADD_TABLE, KEYSPACE)
        print("---------------------------")
        print("| Successfully Processing |")
        print("---------------------------")
    print("| Error: There is no new data!! |")
    print("---------------------------------")