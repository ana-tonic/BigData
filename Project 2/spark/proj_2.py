import pyspark.sql.functions as sparkFun
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark import SparkConf


def writeToCassandra(df_data, epochId):
    df_data.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="statistics", keyspace="example") \
        .mode("append") \
        .save()
    df_data.show()


def writeToCassandra1(df_data, epochId):
    df_data.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="visits_spark", keyspace="example") \
        .mode("append") \
        .save()
    df_data.show()


if __name__ == "__main__":
    N = 3

    conf = SparkConf()
    conf.setMaster("spark://spark-master:7077")
    conf.set("spark.driver.memory", "4g")
    conf.set("spark.cassandra.connection.host", "docker-kafka-flink-cassandra-1")
    conf.set("spark.cassandra.connection.port", "9042")

    spark = SparkSession.builder.config(conf=conf).appName("Nis car").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "topic1") \
        .option("startingOffsets", "latest") \
        .load()

    schema = StructType([
        StructField("vehicle_id", StringType(), False),
        StructField("timestep_time", StringType(), False),
        StructField("location_id", StringType(), False),
        StructField("vehicle_CO", StringType(), False),
        StructField("vehicle_CO2", StringType(), False),
        StructField("vehicle_HC", StringType(), False),
        StructField("vehicle_noise", StringType(), False),
        StructField("vehicle_PMx", StringType(), False),
        StructField("vehicle_speed", StringType(), False),
        StructField("vehicle_waiting", StringType(), False),
        StructField("vehicle_x", StringType(), False),
        StructField("vehicle_y", StringType(), False)
    ])

    parsed_df = df.select("timestamp", sparkFun.from_json(sparkFun.col("value").cast("string"), schema)
                          .alias("parsed_values"))

    selected = parsed_df.selectExpr("timestamp",
                                    "parsed_values.vehicle_speed as vehicle_speed",
                                    "parsed_values.location_id as location_id")

    # min, max, avg_speed and count
    statistics = selected.groupBy(sparkFun.window(selected.timestamp, "10 seconds"), selected.location_id) \
        .agg(
        sparkFun.avg("vehicle_speed").alias("avg_vehicle_speed"),
        sparkFun.min("vehicle_speed").alias("min_vehicle_speed"),
        sparkFun.max("vehicle_speed").alias("max_vehicle_speed"),
        sparkFun.count("*").alias("count"),
        sparkFun.col("window.start").alias("start_time"),
        sparkFun.col("window.end").alias("end_time"),
    ).drop("window")

    query = (statistics
             .writeStream
             .outputMode("update")
             .queryName("Input")
             .foreachBatch(writeToCassandra)
             .start()
             )

    # Most visited locations
    most_visited = selected.groupBy(selected.location_id, sparkFun.window(selected.timestamp, "10 seconds")) \
        .agg(sparkFun.count("*").alias("visited_count")) \
        .orderBy(sparkFun.desc("visited_count")) \
        .select(sparkFun.col("location_id"), sparkFun.col("visited_count"),
                sparkFun.col("window.start").alias("start_time"),
                sparkFun.col("window.end").alias("end_time"))
    topN = most_visited.limit(N)

    query2 = (topN
              .writeStream
              .outputMode("complete")
              .queryName("MostVisited")
              .foreachBatch(writeToCassandra1)
              .start()
              )

    query.awaitTermination()
    query2.awaitTermination()
