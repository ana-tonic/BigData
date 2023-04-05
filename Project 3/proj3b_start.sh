#!/bin/bash

spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 gbt_load.py hdfs://namenode:9000/gradient_boost