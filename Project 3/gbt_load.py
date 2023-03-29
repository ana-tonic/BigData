import sys
import pyspark.sql.functions as spark_fun
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark import SparkConf
from pyspark.ml import PipelineModel
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


def influxdb_writer(df, epoch_id):
    bucket = "niscardb"
    org = "anaorg"
    token = "7FS8SA2DTJoZNTbA7mZImxixNB8nFtQow3x6SMnDG1bWkvepAZNL73ZOREBoFm1ilXlfxfdbXhqSx84du31Y5g=="
    url = "http://influxdb:8086"

    client = InfluxDBClient(url=url, token=token, org=org, username='ana', password='anatonic')
    write_api = client.write_api(write_options=SYNCHRONOUS)

    for row in df.collect():
        point = Point("nis_car_prediction") \
            .field("location_id", row.location_id) \
            .field("vehicle_speed", row.vehicle_speed) \
            .field("timestep_time", row.timestep_time) \
            .field("vehicle_CO", row.vehicle_CO) \
            .field("vehicle_CO2", row.vehicle_CO2) \
            .field("vehicle_HC", row.vehicle_HC) \
            .field("vehicle_noise", row.vehicle_noise) \
            .field("vehicle_PMx", row.vehicle_PMx) \
            .field("vehicle_waiting", row.vehicle_waiting) \
            .field("vehicle_x", row.vehicle_x) \
            .field("vehicle_y", row.vehicle_y) \
            .field("prediction", int(row.prediction))

        write_api.write(bucket=bucket, record=point)


if __name__ == "__main__":
    N = 3

    conf = SparkConf()
    conf.setMaster("spark://spark-master:7077")
    conf.set("spark.driver.memory", "4g")

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

    parsed_df = df.select("timestamp", spark_fun.from_json(spark_fun.col("value").cast("string"), schema)
                          .alias("parsed_values"))

    selected = parsed_df.selectExpr("parsed_values.vehicle_speed as vehicle_speed",
                                    "parsed_values.location_id as location_id",
                                    "parsed_values.timestep_time as timestep_time",
                                    "parsed_values.vehicle_CO as vehicle_CO",
                                    "parsed_values.vehicle_CO2 as vehicle_CO2",
                                    "parsed_values.vehicle_HC as vehicle_HC",
                                    "parsed_values.vehicle_noise as vehicle_noise",
                                    "parsed_values.vehicle_PMx as vehicle_PMx",
                                    "parsed_values.vehicle_waiting as vehicle_waiting",
                                    "parsed_values.vehicle_x as vehicle_x",
                                    "parsed_values.vehicle_y as vehicle_y")

    for col in selected.columns:
        if col != "location_id":
            selected = selected.withColumn(col, selected[col].cast("float"))

    gbt_model = PipelineModel.load(sys.argv[1])

    # predict on test data
    predictions = gbt_model.transform(selected)

    # print on console
    query1 = (predictions.writeStream
              .outputMode("append")
              .format("console")
              .start())

    stream = predictions.writeStream \
        .outputMode("update") \
        .foreachBatch(influxdb_writer) \
        .trigger(processingTime="10 seconds") \
        .start()

    query1.awaitTermination()
    stream.awaitTermination()
