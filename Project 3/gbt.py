import sys
from pyspark.sql import SparkSession
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as spark_fun

if len(sys.argv) != 3:
    print("Wrong number of arguments.")
    exit(-1)

spark = SparkSession \
    .builder \
    .appName("GradientBoost_PySpark") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# load the dataset
df = spark.read.option("inferSchema", True).option("header", True).csv(sys.argv[1])
index = StringIndexer().setInputCol("location_id").setOutputCol("location_id_index").setHandleInvalid("keep")

# feature vector
features = ['timestep_time', 'vehicle_speed', 'vehicle_x',
            'vehicle_y', 'vehicle_CO', 'vehicle_CO2', 'vehicle_HC', 'vehicle_PMx',
            'vehicle_noise', 'vehicle_waiting', 'location_id_index']

assembler = VectorAssembler(inputCols=features, outputCol="features", handleInvalid='keep')

g_boost = GBTClassifier(featuresCol="features",
                        labelCol="pollution_index"
                        )

pipeline = Pipeline().setStages([index, assembler, g_boost])
train, test = df.randomSplit([0.7, 0.3], seed=1712)
test.show(10)

xgb_model = pipeline.fit(train)
xgb_model.write().overwrite().save(sys.argv[2])

# Check if model stored properly
stored_model = PipelineModel.load(sys.argv[2])
predictions = stored_model.transform(test)

# predict on test data
predictionAndLabels = predictions.select(['prediction', 'pollution_index']
                                         ).withColumn('pollution_index',
                                                      spark_fun.col('pollution_index').cast(DoubleType())).rdd

metrics = MulticlassMetrics(predictionAndLabels)

cm = metrics.confusionMatrix().toArray()

accuracy = (cm[0][0] + cm[1][1]) / cm.sum()
precision = (cm[1][1]) / (cm[0][1] + cm[1][1])
recall = (cm[1][1]) / (cm[1][0] + cm[1][1])

print(accuracy, precision, recall)
print(cm)
