import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pyspark.sql.functions as func
from pyspark.sql.types import *
import datetime
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator


# apk update
# apk add make automake gcc g++ subversion python3-dev
# pip3 install numpy

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

conf = SparkConf().setAppName("regression").setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)

df_cgm = spark.read.option("sep", "|").option("header", "true").csv(
    HDFS_NAMENODE + "/processed/HDeviceCGM.csv/Year=*/Month=*")

df_cgm = df_cgm.drop("DexInternalDtTmDaysFromEnroll").drop("DexInternalTm").drop("RecordType")
df_cgm = df_cgm.withColumn("GlucoseValue", col("GlucoseValue").cast(FloatType()))

df_cgm.agg(min("GlucoseValue"), max("GlucoseValue")).show()


df_cgm = df_cgm.withColumn("DateTime", concat(col("Date"), lit(' '), col("Time"))) \
    .withColumn("DateTime", to_timestamp(col("DateTime"))) \
    .withColumn("GlucoseValue", col("GlucoseValue").cast(FloatType())) \
    .drop("Date").drop("Time").drop("Day")
df_cgm.show(5)

windowSpec = Window.partitionBy("PtID").orderBy("DateTime")

# DT - DateTime
# GV - GlucoseValue
# P - Previous
# N - Next
# S - second
df_cgm = df_cgm.withColumn("PDT", lag("DateTime", 1).over(windowSpec)) \
    .withColumn("PGV", lag("GlucoseValue", 1).over(windowSpec)) \
    .withColumn("SPDT", lag("DateTime", 2).over(windowSpec)) \
    .withColumn("SPGV", lag("GlucoseValue", 2).over(windowSpec)) \
    .withColumn("NDT", lag("DateTime", -1).over(windowSpec)) \
    .withColumn("NGV", lag("GlucoseValue", -1).over(windowSpec)) \
    .withColumn("SNDT", lag("DateTime", -2).over(windowSpec)) \
    .withColumn("SNGV", lag("GlucoseValue", -2).over(windowSpec)) \
    .withColumn("Hour", hour("DateTime")) \

df_filled = df_cgm
df_cgm = df_cgm.withColumn("PDT", col("DateTime").cast(LongType()) - col("PDT").cast(LongType())) \
    .withColumn("SPDT", col("DateTime").cast(LongType()) - col("SPDT").cast(LongType())) \
    .withColumn("NDT", col("NDT").cast(LongType()) - col("DateTime").cast(LongType())) \
    .withColumn("SNDT", col("SNDT").cast(LongType()) - col("DateTime").cast(LongType()))

df_filled = df_filled.withColumn("PDT", col("PDT").cast(LongType())) \
    .withColumn("SPDT", col("SPDT").cast(LongType())) \
    .withColumn("NDT", col("NDT").cast(LongType())) \
    .withColumn("SNDT", col("SNDT").cast(LongType()))\
    .withColumn("DateTime", col("DateTime").cast(LongType()))

print("--------------------------------------------------------------------------------------")
df_cgm = df_cgm.na.drop()
df_filled = df_filled.na.drop()
df_cgm.show(10)

# # Assemble all the features with VectorAssembler
required_features = [
    'Hour',
    'PGV',
    'PDT',
    'SPDT',
    'SPGV',
    'NDT',
    'NGV',
    'SNDT',
    'SNGV'
]

patient1 = df_cgm.filter("PtID == '127'")
patient1 = patient1.limit(30000)
df_filled = df_filled.filter("PtID == '127'")
df_filled = df_filled.limit(30000)

assembler = VectorAssembler(inputCols=required_features, outputCol='input')
dataset = assembler.transform(patient1)
(training_data, test_data) = dataset.randomSplit([0.7, 0.3])

print("TRAIN DATA")
training_data.show(5)

rf = RandomForestRegressor(labelCol='GlucoseValue',
                           featuresCol='input',
                           numTrees=50,
                           )

model = rf.fit(training_data)
predictions = model.transform(test_data)
predictions = predictions.withColumn("prediction", round(col("prediction"))) \

evaluator = RegressionEvaluator(
    labelCol='GlucoseValue',
    predictionCol='prediction',
    metricName='rmse')

rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

predictions.select("prediction", "GlucoseValue", "DateTime").show(15)


def interpol(x, x_prev, x_next, y_prev, y_next):
    y_interpol = y_prev + (y_next - y_prev) / (x_next - x_prev) * (x - x_prev)
    return y_interpol


# convert function to udf
interpol_udf = func.udf(interpol, FloatType())
#
# # add interpolated columns to dataframe and clean up
(training_data, test_data) = df_filled.randomSplit([0.7, 0.3])

df_filled = test_data.withColumn('prediction', interpol_udf('DateTime', 'PDT', 'NDT', 'PGV', 'NGV'))\
    .withColumn('prediction', round(col('prediction')))
rmse = evaluator.evaluate(df_filled)
print("Interpolation - Root Mean Squared Error (RMSE) on test data = %g" % rmse)
df_filled.select("prediction", "GlucoseValue", "DateTime").show(15)