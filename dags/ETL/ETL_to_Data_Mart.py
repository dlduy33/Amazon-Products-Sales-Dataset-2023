from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import date

# Create Spark session
spark = SparkSession.builder.appName("Making Revenue, Order, Customer Report") \
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
    df = df.withColumn("actual_price", col("actual_price").cast("int"))
    df = df.withColumn("discount_price", col("discount_price").cast("int"))
    df = df.filter(col("actual_price").isNotNull()).filter(col("actual_price") != 0)
    df = df.fillna(value=0, subset=["ratings", "no_of_ratings", "discount_price"])
    df = df.filter(col("discount_price") < col("actual_price"))
    df.printSchema()
    df.show(10)
    return df

# Construct a dataframe of Revenue analysis
def revenue_analysis(df):
    revenue_analysis = df.withColumn("projected_income",
        when((col("delivery_status").isin("Shipped", "Preparing")) & (col("discount_price") == 0), col("actual_price")) \
        .when((col("delivery_status").isin("Shipped", "Preparing")) & (col("discount_price") != 0), col("discount_price")) \
        .otherwise(0))
    revenue_analysis = revenue_analysis.withColumn("income",
        when((col("delivery_status") == "Delivered") & (col("discount_price") == 0), col("actual_price")*col("quantity_sold")) \
        .when((col("delivery_status") == "Delivered") & (col("discount_price") != 0), col("discount_price")*col("quantity_sold")) \
        .otherwise(0))
    revenue_analysis = revenue_analysis.withColumn("inv_value", 
        when((col("discount_price")!=0), col("discount_price")*col("inv_quantity")).otherwise(col("actual_price")*col("inv_quantity")))
    revenue_analysis = revenue_analysis.groupBy("category", "type", "city", "state", month("ship_date").alias("month"),
        "quarter", year("ship_date").alias("year")) \
        .agg(
            round(avg("ratings"), 2).alias("ratings"),
            sum("no_of_ratings").alias("no_of_ratings"),
            round(avg("discount_price"), 2).alias("avg_disc_price"),
            round(avg("actual_price"), 2).alias("avg_act_price"),
            ceil(avg(when(col("discount_price")!=0, ((col("actual_price")-col("discount_price"))/col("actual_price"))*100)
                .otherwise(0))).alias("avg_disc_rate"),
            sum("quantity_sold").alias("quantity_sold"),
            sum("projected_income").alias("projected_income"),
            sum("income").alias("income"),
            sum("inv_quantity").alias("inv_quantity"), 
            sum("inv_value").alias("inv_value")
        ).sort("month")
    revenue_analysis.printSchema()
    revenue_analysis.show(10)
    return revenue_analysis

# Construct a dataframe of Order analysis
def order_analysis(df):
    order_analysis = df.groupBy("order_id", "city", "state", month("ship_date").alias("month"), 
        "quarter", year("ship_date").alias("year")) \
        .agg(
            round(avg(
                when((col("delivery_status").isin("Shipped", "Preparing", "Delivered"))&(col("discount_price")==0),col("actual_price")) \
                    .otherwise(col("discount_price"))), 2).alias("avg_spending"),
            round(avg(datediff(col("ship_date"),(col("order_date").cast("date")))), 2).alias("processing_time"),
            count(when(col("delivery_status")=="Undelivered", col("delivery_status"))).alias("canceled_order"),
            count(col("delivery_status")).alias("total_order"),
            round((count(when(col("delivery_status")=="Undelivered", col("delivery_status")))/count(col("delivery_status")))*100, 2
                ).alias("cancellation_rate")
        ).sort("month", "order_id")
    order_analysis.printSchema()
    order_analysis.show(10)
    return order_analysis

# Construct a dataframe of Customer analysis
def customer_analysis(df):
    customer_analysis = df.groupBy("customer_id", "gender", "segment", "city", "state", month("ship_date").alias("month"), 
                "quarter", year("ship_date").alias("year")) \
                    .agg(
                        when(count(col("customer_id"))>1, "True").otherwise("False").alias("loyal_customer"),
                        count(col("customer_id")).alias("total_orders")
                    ).sort("customer_id", "gender", "segment")
    customer_analysis.printSchema()
    customer_analysis.show(10)
    return customer_analysis

def kafkaTopic_to_mysql(df, topic, schema, dbtable, arg_year, arg_month, arg_day):
    print("-----------------------------------")
    print(f"| Process {dbtable} with Kafka |")
    print("-----------------------------------")
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
    print("----------------------------------------")
    print(f"| Batching write {dbtable} to mysql |")
    print("----------------------------------------")
    # Declare the parameters MySQL
    HOST = 'localhost'
    PORT = '3306'
    USER = "root"
    PASSWORD = "1"
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
    print("------------------------------------")
        
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
    print("--------------------------")
    print("| Start revenue analysis |")
    print("--------------------------")
    revenueDf_analysis = revenue_analysis(df)
    print("------------------------")
    print("| Start order analysis |")
    print("------------------------")
    orderDf_analysis = order_analysis(df)
    print("---------------------------")
    print("| Start customer analysis |")
    print("---------------------------")
    customerDf_analysis = customer_analysis(df)
    print("----------------------")
    print("| Start Data Process |")
    # Define a schema for the data
    revenueSchema = StructType([StructField("category", StringType(), True),
                        StructField("type", StringType(), True),
                        StructField("city", StringType(), True),
                        StructField("state", StringType(), True),
                        StructField("month", IntegerType(), True),
                        StructField("quarter", IntegerType(), True),
                        StructField("year", IntegerType(), True),
                        StructField("ratings", DoubleType(), True),
                        StructField("no_of_ratings", DoubleType(), True),
                        StructField("avg_disc_price", DoubleType(), True),
                        StructField("avg_act_price", DoubleType(), True),
                        StructField("quantity_sold", IntegerType(), True),
                        StructField("projected_income", IntegerType(), True),
                        StructField("income", IntegerType(), True),
                        StructField("inv_quantity", IntegerType(), True),
                        StructField("inv_value", IntegerType(), True)])
    orderSchema = StructType([StructField("order_id", IntegerType(), True),
                        StructField("city", StringType(), True),
                        StructField("state", StringType(), True),
                        StructField("month", IntegerType(), True),
                        StructField("quarter", IntegerType(), True),
                        StructField("year", IntegerType(), True),
                        StructField("ratings", IntegerType(), True),
                        StructField("avg_spending", DoubleType(), True),
                        StructField("processing_time", DoubleType(), True),
                        StructField("canceled_order", IntegerType(), True),
                        StructField("total_order", IntegerType(), True),
                        StructField("cancellation_rate", DoubleType(), True)])
    customerSchema = StructType([StructField("customer_id", IntegerType(), True),
                        StructField("gender", StringType(), True),
                        StructField("segment", StringType(), True),
                        StructField("city", StringType(), True),
                        StructField("state", StringType(), True),
                        StructField("month", IntegerType(), True),
                        StructField("quarter", IntegerType(), True),
                        StructField("year", IntegerType(), True),
                        StructField("loyal_customer", StringType(), True),
                        StructField("total_orders", IntegerType(), True)])
    # Declare table names for MySQL
    REVENUE_TOPIC = "cass-mysqlMart-revenue"
    ORDER_TOPIC = "cass-mysqlMart-order"
    CUSTOMER_TOPIC = "cass-mysqlMart-customer"
    REVENUE_TABLE = "revenue_table"
    ORDER_TABLE = "order_table"
    CUSTOMER_TABLE = "customer_table"
    # Start to transform data
    revenue_to_mysql = kafkaTopic_to_mysql(revenueDf_analysis, REVENUE_TOPIC, revenueSchema, REVENUE_TABLE,
                                            arg_year, arg_month, arg_day)
    order_to_mysql = kafkaTopic_to_mysql(orderDf_analysis, ORDER_TOPIC, orderSchema, ORDER_TABLE,
                                            arg_year, arg_month, arg_day)
    customer_to_mysql = kafkaTopic_to_mysql(customerDf_analysis, CUSTOMER_TOPIC, customerSchema, CUSTOMER_TABLE,
                                            arg_year, arg_month, arg_day)
    print("| Successful Processing |")
    print("-------------------------")