#!/bin/bash

/spark/bin/spark-submit --master spark://spark-master:7077 pyspark_nis_car.py hdfs://namenode:9000/dir1 1 21.91 43.32 SMALL 1 10