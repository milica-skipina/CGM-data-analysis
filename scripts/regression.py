import os
from itertools import chain

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import sys
from pyspark.ml.regression import RandomForestRegressor, RandomForestRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator


# apk update
# apk add make automake gcc g++ subversion python3-dev
# pip3 install numpy

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


# fill missing values
# define function to create date range
def generate_time_interval(t1, t2, step=300):
    """Return a list of equally spaced points between t1 and t2 with step size step."""
    return [t1 + step * x for x in range(int((t2 - t1) / step) + 1)]


def interpol(x, x_prev, x_next, y_prev, y_next):
    y_interpol = y_prev + (y_next - y_prev) / (x_next - x_prev) * (x - x_prev)
    return y_interpol


HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

conf = SparkConf().setAppName("regression").setMaster("spark://spark-master:7077")
conf.set("spark.executor.memory", "6g")
conf.set("spark.driver.memory", "6g")
# conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)

# ---------------------------------------------------------------------------------------------------------------------
df_cgm = spark.read.option("sep", "|").option("header", "true").csv(
    HDFS_NAMENODE + "/processed/HDeviceCGM.csv/Year=*/Month=*")

df_cgm = df_cgm.drop("DexInternalDtTmDaysFromEnroll").drop("DexInternalTm").drop("RecordType")
df_cgm = df_cgm.withColumn("GlucoseValue", col("GlucoseValue").cast(FloatType()))

df_cgm.agg(min("GlucoseValue"), max("GlucoseValue")).show()

df_cgm = df_cgm.withColumn("DateTime", concat(col("Date"), lit(' '), col("Time"))) \
    .withColumn("DateTime", to_timestamp(col("DateTime"))) \
    .withColumn("GlucoseValue", col("GlucoseValue").cast(FloatType())) \
    .drop("Date").drop("Time").drop("Day")
print("CGM DF")
df_cgm.show(5)

df_cgm = df_cgm.filter("PtID == '265'")
df_cgm = df_cgm.limit(30000)
# DT - DateTime
# GV - GlucoseValue
# P - Previous
# N - Next
# S - second
# T - third
window_spec = Window.partitionBy("PtID").orderBy("DateTime")
df_cgm = df_cgm.withColumn("PDT", lag("DateTime", 1).over(window_spec)) \
    .withColumn("PGV", lag("GlucoseValue", 1).over(window_spec)) \
    .withColumn("SPDT", lag("DateTime", 2).over(window_spec)) \
    .withColumn("SPGV", lag("GlucoseValue", 2).over(window_spec)) \
    .withColumn("NDT", lag("DateTime", -1).over(window_spec)) \
    .withColumn("NGV", lag("GlucoseValue", -1).over(window_spec)) \
    .withColumn("SNDT", lag("DateTime", -2).over(window_spec)) \
    .withColumn("SNGV", lag("GlucoseValue", -2).over(window_spec)) \
    .withColumn("Hour", hour("DateTime")) \
    .withColumn("TPDT", lag("DateTime", 3).over(window_spec)) \
    .withColumn("TPGV", lag("GlucoseValue", 3).over(window_spec)) \
    .withColumn("TNDT", lag("DateTime", -3).over(window_spec)) \
    .withColumn("TNGV", lag("GlucoseValue", -3).over(window_spec))

df_random_forest = df_cgm.withColumn("PDT", col("DateTime").cast(LongType()) - col("PDT").cast(LongType())) \
    .withColumn("SPDT", col("DateTime").cast(LongType()) - col("SPDT").cast(LongType())) \
    .withColumn("NDT", col("NDT").cast(LongType()) - col("DateTime").cast(LongType())) \
    .withColumn("SNDT", col("SNDT").cast(LongType()) - col("DateTime").cast(LongType())) \
    .withColumn("TPDT", col("DateTime").cast(LongType()) - col("TPDT").cast(LongType())) \
    .withColumn("TNDT", col("TNDT").cast(LongType()) - col("DateTime").cast(LongType()))

print("Random Forest DF")
df_random_forest.show(5)

# prepare for training RF
df_random_forest = df_random_forest.na.drop()
patient1 = df_random_forest.filter("PtID == '265'")
# patient1 = patient1.limit(30000)

# # Assemble all the features with VectorAssembler
required_features = [
    'Hour',
    'PGV',
    'PDT',
    'NDT',
    'NGV',
    'SPDT',
    'SPGV',
    'SNDT',
    'SNGV',
    'TPDT',
    'TPGV',
    'TNDT',
    'TNGV'
]

assembler = VectorAssembler(inputCols=required_features, outputCol='input')

dataset = assembler.transform(patient1)
(training_data, test_data) = dataset.randomSplit([0.7, 0.3])
print("Random Forest - Training data")
training_data.show(5)

rf = RandomForestRegressor(labelCol='GlucoseValue',
                           featuresCol='input',
                           numTrees=80,
                           maxDepth=8
                           )
print("RF training started...")
model = rf.fit(training_data)
model.write().overwrite().save(HDFS_NAMENODE + "/models/rf_model.model")
print("Model saved")
print("Prediction:")
predictions = model.transform(test_data)
predictions = predictions.withColumn("prediction", round(col("prediction")))

# Evaluate
evaluator = RegressionEvaluator(
    labelCol='GlucoseValue',
    predictionCol='prediction',
    metricName='rmse')

rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)
print("Random Forest Predictions - Test data")
predictions.select("prediction", "GlucoseValue", "DateTime").show(15)

# interpolation
df_interpolation = df_cgm.withColumn("PDT", col("PDT").cast(LongType())) \
    .withColumn("NDT", col("NDT").cast(LongType())) \
    .withColumn("DateTime", col("DateTime").cast(LongType()))

df_interpolation = df_interpolation.filter(df_interpolation.RecID.isin(df_random_forest.RecID))
df_interpolation = df_interpolation.na.drop()

# convert function to udf
interpol_udf = udf(interpol, FloatType())

test_data = df_interpolation.filter(df_interpolation.RecID.isin(test_data.RecID))
interpolation_predictions = test_data.withColumn('prediction', interpol_udf('DateTime', 'PDT', 'NDT', 'PGV', 'NGV')) \
    .withColumn('prediction', round(col('prediction')))
rmse = evaluator.evaluate(interpolation_predictions)
print("Interpolation - Root Mean Squared Error (RMSE) on test data = %g" % rmse)
print("Interpolation Predictions - Test data")
interpolation_predictions.select("prediction", "GlucoseValue", "DateTime").show(15)

# ---------------------------------------------------------------------------------------------------------------------
print("finding missing timestamps...")
date_range_udf = udf(generate_time_interval, ArrayType(LongType()))

pt_interval = df_cgm.select("RecID", "PtID", "DateTime") \
    .withColumn("DateTime", col("DateTIme").cast(LongType())) \
    .groupBy("PtID") \
    .agg(min("DateTime").alias("FirstDateTime"), max("DateTime").alias("LastDateTime"))

pt_interval = pt_interval.withColumn("DateTime", explode(date_range_udf("FirstDateTime", "LastDateTime"))) \
    .drop('FirstDateTime', 'LastDateTime') \
    .withColumn("DateTime", to_timestamp(col("DateTime")))

df_all_dates = df_cgm.withColumn("DT", col("DateTime"))
df_all_dates = pt_interval.join(df_all_dates, ["PtID", "DateTime"], "outer")

# prepare data for regression
windowSpec_prev = Window.partitionBy("PtID").orderBy("DateTime").rowsBetween(-sys.maxsize, -1)
windowSpec_next = Window.partitionBy("PtID").orderBy("DateTime").rowsBetween(1, sys.maxsize)
df_all_dates = df_all_dates.withColumn("PDT", last("DT", ignorenulls=True).over(windowSpec_prev)) \
    .withColumn("PGV", last("GlucoseValue", ignorenulls=True).over(windowSpec_prev)) \
    .withColumn("NDT", first("DT", ignorenulls=True).over(windowSpec_next)) \
    .withColumn("NGV", first("GlucoseValue", ignorenulls=True).over(windowSpec_next)) \
    .withColumn("SPDT", last("PDT", ignorenulls=True).over(windowSpec_prev)) \
    .withColumn("SPGV", last("PGV", ignorenulls=True).over(windowSpec_prev)) \
    .withColumn("SNDT", first("NDT", ignorenulls=True).over(windowSpec_next)) \
    .withColumn("SNGV", first("NGV", ignorenulls=True).over(windowSpec_next)) \
    .withColumn("TPDT", last("SPDT", ignorenulls=True).over(windowSpec_prev)) \
    .withColumn("TPGV", last("SPGV", ignorenulls=True).over(windowSpec_prev)) \
    .withColumn("TNDT", first("SNDT", ignorenulls=True).over(windowSpec_next)) \
    .withColumn("TNGV", first("SNGV", ignorenulls=True).over(windowSpec_next))

df_all_dates = df_all_dates.withColumn("PDT", col("DateTime").cast(LongType()) - col("PDT").cast(LongType())) \
    .withColumn("SPDT", col("DateTime").cast(LongType()) - col("SPDT").cast(LongType())) \
    .withColumn("NDT", col("NDT").cast(LongType()) - col("DateTime").cast(LongType())) \
    .withColumn("SNDT", col("SNDT").cast(LongType()) - col("DateTime").cast(LongType())) \
    .withColumn("TPDT", col("DateTime").cast(LongType()) - col("TPDT").cast(LongType())) \
    .withColumn("TNDT", col("TNDT").cast(LongType()) - col("DateTime").cast(LongType()))

res_file = "/regression/all_dates.csv"
df_all_dates.repartition(1).write.mode("overwrite").option("header", "true").option("sep", "|").csv(
    HDFS_NAMENODE + res_file)
# ---------------------------------------------------------------------------------------------------------------------
res_file = "/regression/all_dates.csv"

df_all_dates = spark.read.option("sep", "|").option("header", "true").csv(
    HDFS_NAMENODE + res_file)
model = RandomForestRegressionModel.load(HDFS_NAMENODE + "/models/rf_model.model")
print("model loaded")
interpol_udf = udf(interpol, FloatType())
required_features = [
    'Hour',
    'PGV',
    'PDT',
    'NDT',
    'NGV',
    'SPDT',
    'SPGV',
    'SNDT',
    'SNGV',
    'TPDT',
    'TPGV',
    'TNDT',
    'TNGV'
]

assembler = VectorAssembler(inputCols=required_features, outputCol='input')
# ---------------------------------------------------------------------------------------------------------------------

df_all_dates.show(5)
df_all_dates = df_all_dates.withColumn("Hour", col("Hour").cast(LongType()))\
    .withColumn("GlucoseValue", col("GlucoseValue").cast(LongType()))\
    .withColumn("PDT", col("PDT").cast(LongType())) \
    .withColumn("PGV", col("PGV").cast(LongType()))  \
    .withColumn("SPDT", col("SPDT").cast(LongType())) \
    .withColumn("SPGV", col("SPGV").cast(LongType()))\
    .withColumn("NDT", col("NDT").cast(LongType())) \
    .withColumn("NGV", col("NGV").cast(LongType())) \
    .withColumn("SNDT", col("SNDT").cast(LongType())) \
    .withColumn("SNGV", col("SNGV").cast(LongType())) \
    .withColumn("TPDT", col("TPDT").cast(LongType())) \
    .withColumn("TPGV", col("TPGV").cast(LongType())) \
    .withColumn("TNDT", col("TNDT").cast(LongType())) \
    .withColumn("TNGV", col("TNGV").cast(LongType()))\
    .withColumn("DateTime", to_timestamp(col("DateTime")))

missing_values = df_all_dates.filter(col("RecID").isNull())
missing_values = missing_values.filter((col("PDT") >= 250) & (col("PDT") < 1500) & (col("SPDT") < 2200) &
                                       (col("TPDT") < 2900) & (col("NDT") >= 250) & (col("NDT") < 1500) &
                                       (col("SNDT") < 2200) & (col("TNDT") < 2900))

missing_values = missing_values.na.drop(subset=["PDT", "PGV", "SPDT", "SPGV", "TPDT", "TPGV",
                                                "NGV", "NDT", "SNDT", "SNGV", "TNDT", "TNGV"])
missing_values = missing_values.withColumn("Hour", hour("DateTime"))\
    .drop("GlucoseValue").drop("RecID").drop("DT")
print("Missing values")
missing_values.show(5)

dataset = assembler.transform(missing_values)
print("Dataset")
dataset.show(5)
predictions = model.transform(dataset)
print("Predictions")
predictions = predictions.withColumn("GlucoseValue", round(col("prediction"))).drop('input').drop('prediction')
print("Random Forest Predictions")
predictions.show(15, truncate=False)

# print("DF all dates")
# df_help = df_all_dates.persist()
# # print(df_help.limit(20).toPandas())
# df_help.show(20)
# # print(df_help.filter(col("RecID").isNotNull()).limit(20).toPandas())
# df_help.filter(col("RecID").isNull()).show(20)
# df_help.unpersist()

# interpolation
df_interpolation = missing_values.select("PtID", "PDT", "NDT", "PGV", "NGV", "DateTime")
df_interpolation = df_interpolation.withColumn("DateTime", to_timestamp(col("DateTime")).cast(LongType())) \
    .withColumn("PDT",  col("DateTime") - col("PDT").cast(LongType()))\
    .withColumn("NDT",  col("DateTime") + col("NDT").cast(LongType())) \
    .withColumn("PGV", col("PGV").cast(LongType()))\
    .withColumn("NGV", col("NGV").cast(LongType()))

interpolation_predictions = df_interpolation.withColumn('prediction',
                                                        interpol_udf('DateTime', 'PDT', 'NDT', 'PGV', 'NGV')) \
    .withColumn('prediction', round(col('prediction')))
print("Interpolation Predictions")
interpolation_predictions = interpolation_predictions.withColumn("DateTime", to_timestamp(col("DateTime")))\
    .withColumn("PDT", to_timestamp(col("PDT")))\
    .withColumn("NDT", to_timestamp(col("NDT")))\
    .withColumn("GlucoseValue", col("prediction"))\
    .drop("prediction")
interpolation_predictions.show(15, truncate=False)

res_file = "/regression/missing_rf.csv"
predictions.repartition(1).write.mode("overwrite").option("header", "true").option("sep", "|").csv(
    HDFS_NAMENODE + res_file)

res_file = "/regression/missing_interpolation.csv"
interpolation_predictions.repartition(1).write.mode("overwrite").option("header", "true").option("sep", "|").csv(
    HDFS_NAMENODE + res_file)

wind = Window.partitionBy("PtID").orderBy("DateTime")
final_data = predictions.select("PtID", "DateTime", "GlucoseValue")
final_data = final_data.withColumn("RecID", col("PtID")*10000 + rank().over(wind))
initial_data = df_all_dates.filter(col("RecID").isNotNull()).select("PtID", "DateTime", "GlucoseValue", "RecID")\

final_data = final_data.union(initial_data)

print("Final data")
final_data.show(5)

res_file = "/regression/final.csv"
interpolation_predictions.repartition(1).write.mode("overwrite").option("header", "true").option("sep", "|").csv(
    HDFS_NAMENODE + res_file)

# ---------------------------------------------------------------------------------------------------------------------
# df_all_dates = df_all_dates.withColumn("SPDT", df_all_dates.filter(df_all_dates.RecID == col("PDT")).PDT) \
#     .withColumn("SNDT", df_all_dates.filter(df_all_dates.RecID == col("NDT")).NDT)
# df_all_dates = df_all_dates.withColumn("rec_pdt", create_map(["RecID", "PDT"]))

# df_all_dates = df_all_dates.filter("PtID == '227'")
# df_all_dates = df_all_dates.sort("DateTime").limit(30000)
#
# print("TIPOVI")
# print(df_all_dates.dtypes)
# # print(df_all_dates.count())
# df_all_dates = df_all_dates.withColumn("SPRecID", col("PRecID"))
# df_all_dates = df_all_dates.withColumn("SPRecID", col("SPRecID").cast(LongType())) \
#     .withColumn("PRecID", col("PRecID").cast(LongType())) \
#     .withColumn("RecID", col("RecID").cast(LongType()))
# df_all_dates = df_all_dates.na.fill(-1, subset=["SPRecID", "PRecID"])
#
# help_df = df_all_dates.select("RecID", "PRecID")
# # help_df = help_df.withColumn("RecID", col("RecID").cast(StringType()))\
# #     .withColumn("PRecID", col("PRecID").cast(StringType()))
#
# # df = df.withColumn('col_with_string', when(df.col_with_string.isNull(),
# # lit('0')).otherwise(df.col_with_string))
#
# help_df = help_df.na.fill(-1)
#
# newrdd = help_df.select("RecID", "PRecID").rdd.cache()
# keypair_rdd = newrdd.map(lambda x: (x[0], x[1]))
#
# my_dict = keypair_rdd.collectAsMap()
# # help_df = help_df.withColumnRenamed("RecID", "Help")
# # map_rc = create_map([help_df["RecID"], help_df["PDT"]])
# print("map created")
#
# my_dict[-1] = -1
#
# print(set(map(type, my_dict)))
# print("before replace")
# df_all_dates = df_all_dates.replace(to_replace=my_dict, subset=['SPRecID'])
# my_dict = my_dict.clear()
# print("replace done")
# df_temp = df_all_dates.persist()
# df_temp.show(10, truncate=False)
# df_temp.unpersist()
# # mapping_expr = create_map([lit(x) for x in chain(*my_dict.items())])
#
# # df_all_dates = df_all_dates.withColumn("SPDT", mapping_expr[col("PRecID")]).show(10, truncate=False)
#
# # df_all_dates.select(mapping_expr['10774808'].alias('device_type')).show(10)
#
# df_all_dates = df_all_dates.withColumn("SPDT", when(map_rc.getItem(col("PDT")).isNotNull(),map_rc[col("PDT")])
# .otherwise(lit(None)).show(10, truncate=False))
# df_all_dates = df_all_dates.withColumn("SPDT", df_all_dates.rec_pdt.getItem(col("PDT")))
# SPDT = when(
# col("PDT").isNotNull(), df_all_dates.first(df_all_dates.filter(df_all_dates.RecID == col("PDT"))).PDT).otherwise(None)
# df_all_dates = df_all_dates.withColumn("SPDT", SPDT)
