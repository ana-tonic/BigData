#!/bin/bash

spark/bin/spark-submit --master spark://spark-master:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 proj_2.py 