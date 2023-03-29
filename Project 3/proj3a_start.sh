#!/bin/bash

spark/bin/spark-submit --master spark://spark-master:7077 gbt.py hdfs://namenode:9000/binary hdfs://namenode:9000/gradient_boost

