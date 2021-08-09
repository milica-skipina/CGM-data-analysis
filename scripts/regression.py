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


print(df_cgm.count())
df_cgm.agg(min("GlucoseValue"), max("GlucoseValue")).show()

# df_cgm = df_cgm.withColumn("Year", year(df_cgm["Date"])).withColumn("Month", month(df_cgm["Date"])) \
#     .withColumn("Day", dayofmonth(df_cgm["Date"]))
#
# df_cgm = df_cgm.withColumn("Hour", hour(df_cgm["Time"]))\
#     .withColumn("Minute", minute(df_cgm["Time"])) \
#     .withColumn("Second", second(df_cgm["Time"]))


# patient1 = df_cgm.filter("PtID == '193'")
# patient1.show(30)
# patient1.agg(count("*")).show()
#
# seconds = patient1.groupBy("Second").agg(count("*"))
# seconds.show(truncate=False)
# df.createOrReplaceTempView("patients")

# cgm_file = "/processed2/HDeviceCGM.csv"
#
# df_cgm.partitionBy("Year", "Month").mode("overwrite").option("header", "true").option("sep", "|").csv(
#     HDFS_NAMENODE + cgm_file)


# print("REGRESIJA")
# # Prepare training data from a list of (label, features) tuples.
# training = spark.createDataFrame([
#     (0.0, 0.0),
#     (1.0, 1.0),
#     (4.0, 2.0),
#     (9.0, 3.0),
#     (16.0, 4.0),
#     (25.0, 5.0),
#     (36.0, 6.0),
#     (49.0, 7.0),
#     (64.0, 8.0),
#     (81.0, 9.0),
#     (144.0, 12.0),
#     (169.0, 13.0),
#     (196.0, 14.0),
#     (225.0, 25.0),
#     (16.0, 4.0),
#     (25.0, 5.0),
#     (36.0, 6.0),
#     (49.0, 7.0),
#     (64.0, 8.0),
#     (81.0, 9.0)
# ], ["label", "feature"])
#
# # Assemble all the features with VectorAssembler
# required_features = [
#     # 'Date',
#     # 'Time',
#     'Hour',
#     'Minute',
#     'Day',
#     'Month',
#     'Year'
# ]
#
#
# patient1 = patient1.limit(30000)
# dataset = patient1.select(col('Hour').cast('int'),
#                     col('Minute').cast('int'),
#                     col('Day').cast('int'),
#                     col('Month').cast('int'),
#                     col('Year').cast('int'),
#                     col('GlucoseValue').cast('float'),
#                     col('Date'),
#                     col('Time')
#             ).filter('GlucoseValue <= 300').filter('GlucoseValue >= 0')
#
# assembler = VectorAssembler(inputCols=required_features, outputCol='input')
# dataset = assembler.transform(dataset)
# (training_data, test_data) = dataset.randomSplit([0.7, 0.3])
# # Prepare test data
# test = spark.createDataFrame([
#     (121.0, 11.0),
#     (100.0, 10.0),
#     (9.0, 3.0),
#     (2500.0, 50.0)], ["label", "feature"])
#
# # test_data = assembler.transform(test)
# print("TRAIN DATA")
# training_data.show(5)
#
# rf = RandomForestRegressor(labelCol='GlucoseValue',
#                             featuresCol='input',
#                             maxDepth=30,
#                            numTrees=100,
#                            )
#
# model = rf.fit(training_data)
# predictions = model.transform(test_data)
#
# evaluator = RegressionEvaluator(
#     labelCol='GlucoseValue',
#     predictionCol='prediction',
#     metricName='rmse')
#
# rmse = evaluator.evaluate(predictions)
# print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)
#
# predictions.select("prediction", "GlucoseValue", "Date", 'Time').show(20)


# za predikciju slati 3 prethodna i sledeca

# iskoristiti ono da se pronadje prvi najblizi pa pomocu toga predvidjati
# koristiti lag, sacuvati datum i vrijeme i vrijednost glukoze u tom trenutku

# windowSpec = Window.partitionBy("PtID").orderBy("Year", "Month", "Day", "Hour", "Minute", "Second")

# def interpol(x, x_prev, x_next, y_prev, y_next, y):
#     if x_prev == x_next:
#         return y
#     else:
#         m = (y_next - y_prev) / (x_next - x_prev)
#         y_interpol = y_prev + m * (x - x_prev)
#         return y_interpol
#
#
# # convert function to udf
# interpol_udf = func.udf(interpol, FloatType())
#
# # add interpolated columns to dataframe and clean up
# df_filled = df_filled.withColumn('readvalue_interpol',
#                                  interpol_udf('readtime', 'readtime_ff', 'readtime_bf', 'readvalue_ff', 'readvalue_bf',
#                                               'readvalue')) \
#     .drop('readtime_existent', 'readtime_ff', 'readtime_bf') \
#     .withColumnRenamed('reads_all', 'readvalue') \
#     .withColumn('readtime', func.from_unixtime(col('readtime')))


df_cgm = df_cgm.withColumn("DateTime", concat(col("Date"), lit(' '), col("Time")))\
    .withColumn("DateTime", to_timestamp(col("DateTime")))
df_cgm.show(5)


windowSpec = Window.partitionBy("PtID").orderBy("DateTime")
# df_cgm.withColumn("row_number", row_number().over(windowSpec)).show()
# df_cgm.withColumn("dense_rank", dense_rank().over(windowSpec)).show()

# .withColumn("DiffInSeconds",current_timestamp().cast(LongType) -
#         col("input_timestamp").cast(LongType))
df_cgm = df_cgm.withColumn("PreviousDateTime", lag("DateTime", 1).over(windowSpec))
    # .withColumn("PreviousDateTime", to_timestamp(col("PreviousDateTime")))
df_cgm.show(10)
df_cgm = df_cgm.withColumn("DiffInSeconds", col("DateTime").cast(LongType()) - col("PreviousDateTime").cast(LongType()))\
    .withColumn("DiffInMinutes",round(col("DiffInSeconds")/60))

df_cgm.show(10)

patient1 = df_cgm.filter("PtID == '193'")



gbnt = not_using.groupBy("PtID").agg(count("*"))
gbnt.show()
