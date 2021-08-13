import os
from itertools import chain

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import sys
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
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
print(df_cgm.count())


# # fill missing values
# # define function to create date range
def generate_time_interval(t1, t2, step=300):
    """Return a list of equally spaced points between t1 and t2 with step size step."""
    return [t1 + step * x for x in range(int((t2 - t1) / step) + 1)]


# define udf
date_range_udf = udf(generate_time_interval, ArrayType(LongType()))

# obtain min and max of time period for each house
pt_interval = df_cgm.select("RecID", "PtID", "DateTime", "GlucoseValue") \
    .withColumn("DateTime", col("DateTIme").cast(LongType())) \
    .groupBy("PtID") \
    .agg(min("DateTime").alias("FirstDateTime"), max("DateTime").alias("LastDateTime"))

print("PT_INTERVAL")
# pt_interval.show(10)
# generate timegrid and explode
df_base = pt_interval.withColumn("DateTime", explode(date_range_udf("FirstDateTime", "LastDateTime"))) \
    .drop('FirstDateTime', 'LastDateTime')\
    .withColumn("DateTime", to_timestamp(col("DateTime")))
print("DF_BASE")
# df_base.show(10)
# left outer join existing read values
df_all_dates = df_base.join(df_cgm, ["PtID", "DateTime"], "outer")
# df_all_dates.show(10)

# prepare data for training
windowSpec_prev = Window.partitionBy("PtID").orderBy("DateTime").rowsBetween(-sys.maxsize, -1)
windowSpec_next = Window.partitionBy("PtID").orderBy("DateTime").rowsBetween(1, sys.maxsize)
# DT - DateTime
# GV - GlucoseValue
# P - Previous
# N - Next
# S - second
# print(df_cgm.filter("PtID == '227'").count())

df_all_dates = df_all_dates.withColumn("PRecID", last("RecID", ignorenulls=True).over(windowSpec_prev))\
    .withColumn("NRecID", first("RecID", ignorenulls=True).over(windowSpec_next))
# df_all_dates = df_all_dates.withColumn("SPDT", df_all_dates.filter(df_all_dates.RecID == col("PDT")).PDT) \
#     .withColumn("SNDT", df_all_dates.filter(df_all_dates.RecID == col("NDT")).NDT)
# df_all_dates = df_all_dates.withColumn("rec_pdt", create_map(["RecID", "PDT"]))

df_all_dates = df_all_dates.filter("PtID == '227'")
df_all_dates = df_all_dates.sort("DateTime").limit(10)

print("TIPOVI")
print(df_all_dates.dtypes)
print(df_all_dates.count())
df_all_dates = df_all_dates.withColumn("SPRecID", col("PRecID"))
df_all_dates = df_all_dates.na.fill('', subset=["SPDT", "PRecID"])
# df_all_dates = df_all_dates.withColumn("SPDT", col("SPDT").cast(StringType())) \
#     .withColumn("PRecID", col("PRecID").cast(StringType()))
help_df = df_all_dates.select("RecID", "PRecID")
# help_df = help_df.withColumn("RecID", col("RecID").cast(StringType()))\
#     .withColumn("PRecID", col("PRecID").cast(StringType()))

# df = df.withColumn('col_with_string', when(df.col_with_string.isNull(),
# lit('0')).otherwise(df.col_with_string))

help_df = help_df.na.fill('')

newrdd = help_df.select("RecID", "PRecID").rdd
keypair_rdd = newrdd.map(lambda x : (x[0],x[1]))
my_dict = keypair_rdd.collectAsMap()
# help_df = help_df.withColumnRenamed("RecID", "Help")
# map_rc = create_map([help_df["RecID"], help_df["PDT"]])
print("map created")

my_dict[''] = ''

# print(set(map(type, my_dict)))
print("before replace")
df_all_dates = df_all_dates.replace(to_replace=my_dict, subset=['SPRecID'])
print("replace done")
# mapping_expr = create_map([lit(x) for x in chain(*my_dict.items())])

# df_all_dates = df_all_dates.withColumn("SPDT", mapping_expr[col("PRecID")]).show(10, truncate=False)

# df_all_dates.select(mapping_expr['10774808'].alias('device_type')).show(10)

# df_all_dates = df_all_dates.withColumn("SPDT", when(map_rc.getItem(col("PDT")).isNotNull(),map_rc[col("PDT")]).otherwise(lit(None)).show(10, truncate=False))
# df_all_dates = df_all_dates.withColumn("SPDT", df_all_dates.rec_pdt.getItem(col("PDT")))
# SPDT = when(
#     col("PDT").isNotNull(), df_all_dates.first(df_all_dates.filter(df_all_dates.RecID == col("PDT"))).PDT).otherwise(None)
# df_all_dates = df_all_dates.withColumn("SPDT", SPDT)
df_all_dates.show(5, truncate=False)
# res_file = "/help/all_dates.csv"
# df_all_dates.repartition(1).write.mode("overwrite").option("header", "true").option("sep", "|").csv(
#     HDFS_NAMENODE + res_file)

# df_cgm = df_all_dates.withColumn("PDT", lag("DateTime", 1).over(windowSpec)) \
#     .withColumn("PGV", lag("GlucoseValue", 1).over(windowSpec)) \
#     .withColumn("SPDT", lag("DateTime", 2).over(windowSpec)) \
#     .withColumn("SPGV", lag("GlucoseValue", 2).over(windowSpec)) \
#     .withColumn("NDT", lag("DateTime", -1).over(windowSpec)) \
#     .withColumn("NGV", lag("GlucoseValue", -1).over(windowSpec)) \
#     .withColumn("SNDT", lag("DateTime", -2).over(windowSpec)) \
#     .withColumn("SNGV", lag("GlucoseValue", -2).over(windowSpec)) \
#     .withColumn("Hour", hour("DateTime")) \
#     .withColumn("TPDT", lag("DateTime", 3).over(windowSpec)) \
#     .withColumn("TPGV", lag("GlucoseValue", 3).over(windowSpec)) \
#     .withColumn("TNDT", lag("DateTime", -3).over(windowSpec)) \
#     .withColumn("TNGV", lag("GlucoseValue", -3).over(windowSpec))
# df_filled = df_cgm
# df_cgm = df_cgm.withColumn("PDT", col("DateTime").cast(LongType()) - col("PDT").cast(LongType())) \
#     .withColumn("SPDT", col("DateTime").cast(LongType()) - col("SPDT").cast(LongType())) \
#     .withColumn("NDT", col("NDT").cast(LongType()) - col("DateTime").cast(LongType())) \
#     .withColumn("SNDT", col("SNDT").cast(LongType()) - col("DateTime").cast(LongType())) \
#     .withColumn("TPDT", col("DateTime").cast(LongType()) - col("TPDT").cast(LongType())) \
#     .withColumn("TNDT", col("TNDT").cast(LongType()) - col("DateTime").cast(LongType()))

# df_cgm = df_cgm.filter("PtID == '227'")
# print(df_cgm.count())
#
# temp = df_cgm.filter((col("RecID").isNotNull()) | (col("PGV").isNotNull()) | (col("SPGV").isNotNull()) | (col("TPGV").isNotNull()))
# print(temp.count())
# temp.filter(col("RecID").isNull()).show(50)





# df_cgm = df_cgm.filter("PtID == '227'")
# df_cgm = df_cgm.filter('PDT < 1500 AND SPDT < 2200 AND TPDT < 2900 AND NDT < 1500 AND SNDT < 2200 AND TNDT < 2900')
#
# df_filled = df_filled.withColumn("PDT", col("PDT").cast(LongType())) \
#     .withColumn("SPDT", col("SPDT").cast(LongType())) \
#     .withColumn("NDT", col("NDT").cast(LongType())) \
#     .withColumn("SNDT", col("SNDT").cast(LongType())) \
#     .withColumn("TPDT", col("TPDT").cast(LongType())) \
#     .withColumn("TNDT", col("TNDT").cast(LongType())) \
#     .withColumn("DateTime", col("DateTime").cast(LongType()))
#
# df_filled = df_filled.filter(df_filled.RecID.isin(df_cgm.RecID))
#
# print("--------------------------------------------------------------------------------------")
# df_cgm = df_cgm.na.drop()
# df_filled = df_filled.na.drop()
# df_cgm.show(10)
# patient1 = df_cgm.filter("PtID == '227'")
# patient1 = patient1.limit(30000)
# df_filled = df_filled.filter("PtID == '227'")
# df_filled = df_filled.limit(30000)
# # # Assemble all the features with VectorAssembler
# required_features = [
#     'Hour',
#     'PGV',
#     'PDT',
#     'NDT',
#     'NGV',
#     'SPDT',
#     'SPGV',
#     'SNDT',
#     'SNGV',
#     'TPDT',
#     'TPGV',
#     'TNDT',
#     'TNGV'
# ]
#
# assembler = VectorAssembler(inputCols=required_features, outputCol='input')
# dataset = assembler.transform(patient1)
# (training_data, test_data) = dataset.randomSplit([0.7, 0.3])
#
# print("TRAIN DATA")
# training_data.show(5)
#
# rf = RandomForestRegressor(labelCol='GlucoseValue',
#                            featuresCol='input',
#                            numTrees=80,
#                            maxDepth=8
#                            )
#
# model = rf.fit(training_data)
# predictions = model.transform(test_data)
# predictions = predictions.withColumn("prediction", round(col("prediction"))) \
#  \
# evaluator = RegressionEvaluator(
#     labelCol='GlucoseValue',
#     predictionCol='prediction',
#     metricName='rmse')
#
# rmse = evaluator.evaluate(predictions)
# print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)
#
# predictions.select("prediction", "GlucoseValue", "DateTime").show(15)
#
#
# def interpol(x, x_prev, x_next, y_prev, y_next):
#     y_interpol = y_prev + (y_next - y_prev) / (x_next - x_prev) * (x - x_prev)
#     return y_interpol
#
#
# # convert function to udf
# interpol_udf = udf(interpol, FloatType())
#
# # (training_data, test_data) = df_filled.randomSplit([0.7, 0.3])
# test_data = df_filled.filter(df_filled.RecID.isin(test_data.RecID))
# df_filled = test_data.withColumn('prediction', interpol_udf('DateTime', 'PDT', 'NDT', 'PGV', 'NGV')) \
#     .withColumn('prediction', round(col('prediction')))
# rmse = evaluator.evaluate(df_filled)
# print("Interpolation - Root Mean Squared Error (RMSE) on test data = %g" % rmse)
# df_filled.select("prediction", "GlucoseValue", "DateTime").show(15)
