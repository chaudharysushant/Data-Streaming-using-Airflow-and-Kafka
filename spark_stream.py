import logging
import time
import os
from uuid import UUID

os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
# os.environ.setdefault("CASSANDRA_HOST", "cassandra")
# os.environ.setdefault("SPARK_MASTER_HOST", "spark://spark-master:7077")

os.environ.setdefault("CASSANDRA_HOST", "127.0.0.1")
# os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
os.environ.setdefault("SPARK_MASTER_HOST", "spark://127.0.0.1:7077")

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType

logging.basicConfig(level=logging.INFO)

def wait_for_cassandra(host=None, port=9042, timeout=120):
    host = host or os.environ.get("CASSANDRA_HOST", "127.0.0.1")
    start = time.time()
    while True:
        try:
            cluster = Cluster([host], port=port)
            session = cluster.connect()
            logging.info(f"Connected to Cassandra at {host}:{port}!")
            return session
        except Exception as e:
            elapsed = time.time() - start
            if elapsed > timeout:
                logging.error(f"Cassandra not reachable after {timeout}s")
                raise e
            logging.info(f"Waiting for Cassandra ({int(elapsed)}s)...")
            time.sleep(5)

def create_keyspace_and_table(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
        );
    """)
    logging.info("Keyspace and table ensured!")

def create_spark_connection(spark_master=None, cassandra_host=None):
    spark_master = spark_master or os.environ.get("SPARK_MASTER_HOST", "spark://spark-master:7077")
    cassandra_host = cassandra_host or os.environ.get("CASSANDRA_HOST", "cassandra")
    
    spark = (SparkSession.builder
             .appName('SparkKafkaToCassandra')
             .master(spark_master)
             .config('spark.jars.packages',
                     "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                     "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.2")
             .config('spark.cassandra.connection.host', "127.0.0.1")
             .config("spark.cassandra.connection.port", "9042")
             .config("spark.executor.memory", "2g")
             .config("spark.driver.memory", "1g")
             .getOrCreate())
    
    spark.sparkContext.setLogLevel("ERROR")
    logging.info(f"Spark session created successfully! Connected to {spark_master}")
    return spark

def connect_to_kafka(spark_conn, kafka_bootstrap=None, topic="users"):
    kafka_bootstrap = kafka_bootstrap or os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
    logging.info(f"Connecting to Kafka at {kafka_bootstrap}...")
    df = (spark_conn.readStream
          .format('kafka')
          .option('kafka.bootstrap.servers', kafka_bootstrap)
          .option('subscribe', topic)
          .option('startingOffsets', 'earliest')
          .option('failOnDataLoss', 'false')
          .load())
    logging.info(f"Kafka DataFrame created successfully from topic '{topic}'")
    return df

def parse_kafka_json(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])
    sel = (spark_df.selectExpr("CAST(value AS STRING)")
           .select(from_json(col('value'), schema).alias('data'))
           .select("data.*"))
    return sel

def uuid_conversion(batch_df):
    def to_uuid(id_str):
        try:
            return str(UUID(id_str))  # Spark prefers string type for UUID columns
        except:
            return None

    uuid_udf = udf(to_uuid)
    batch_df = batch_df.withColumn("id", uuid_udf(col("id")))
    return batch_df.filter(col("id").isNotNull())

# Write batch to Cassandra
def write_to_cassandra(batch_df, batch_id):
    batch_df = uuid_conversion(batch_df)
    count = batch_df.count()
    logging.info(f"Batch {batch_id} has {count} rows")
    if count == 0:
        logging.info(f"Batch {batch_id} skipped: no valid rows")
        return

    try:
        (batch_df.write
            .format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(keyspace="spark_streams", table="created_users")
            .save()
        )
        logging.info(f"Batch {batch_id} written to Cassandra successfully")
    except Exception as e:
        logging.error(f"Batch {batch_id} failed to write to Cassandra: {e}")

if __name__ == "__main__":
    cassandra_host = os.environ.get("CASSANDRA_HOST")
    session = wait_for_cassandra(cassandra_host)
    create_keyspace_and_table(session)

    spark_master = os.environ.get("SPARK_MASTER_HOST")
    spark_conn = create_spark_connection(spark_master, cassandra_host)

    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    kafka_df = connect_to_kafka(spark_conn, kafka_bootstrap)
    selection_df = parse_kafka_json(kafka_df)

    logging.info("Starting Spark Structured Streaming...")
    streaming_query = selection_df.writeStream \
        .foreachBatch(write_to_cassandra) \
        .option("checkpointLocation", "file:///tmp/checkpoint_new") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()

    streaming_query.awaitTermination()