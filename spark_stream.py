#pour enregister des message lors du débogage
import logging
from datetime import datetime
#pour se connecter à un cluster Cassandra
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
from pyspark.sql.types import StructType, StructField, StringType
import os
import uuid
from pyspark.sql.functions import lit  # Importez la fonction lit depuis pyspark.sql.functions
from pyspark.sql.functions import expr

#frist step : la creation du spark session
def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,""org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
            .config('spark.cassandra.connection.host', '172.19.0.4') \
            .config('spark.cassandra.connection.port', '9042')\
            .getOrCreate()
        # Cela signifie que seules les erreurs seront affichées dans les journaux
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception : {e}")

    return s_conn

#step 2: la connection du spark avec kafka et creation des subscribe pour lire de la data 
def connect_to_kafka(spark_conn):
    #utilisée pour stocker le DataFrame Spark contenant les données provenant de Kafka.
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        
            #"earliest" indique que la lecture commencera depuis le début des données disponibles dans le topic.
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

#step 3 :creation de la connection cassandra
def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['172.19.0.4'])
        cas_session = cluster.connect()
        return cas_session
    
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None
    
#step 4 :la creation du KEYSPACE de casandra
def create_keyspace(session):
    session.execute("""
                    CREATE KEYSPACE IF NOT EXISTS spark_streams
                    WITH replication={'class': 'NetworkTopologyStrategy', 'datacenter1': 1};
                """)
    
    print("Keyspace created successfully!")

#step 4 :la creation de la table sous cansandra
def create_table(session):
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
        picture TEXT);
    """)

    print("Table created successfully!")
    


def create_selection_df_from_kafka(spark_df):
    # Generate UUID for each record
    #spark_df = spark_df.withColumn("id", lit(str(uuid.uuid4())))  # Generate UUID and add it as a column
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

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    
    # Generate UUID for each record
    sel = sel.withColumn("id", expr("uuid()"))

    return sel


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        print(spark_df)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()