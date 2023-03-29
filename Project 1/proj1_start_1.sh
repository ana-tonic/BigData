#!/bin/bash

/spark/bin/spark-submit --master spark://spark-master:7077 pyspark_nis_car.py hdfs://namenode:9000/dir 1 21.895337 43.321110 MEDIUM 300 54