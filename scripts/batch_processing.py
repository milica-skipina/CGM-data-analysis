import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

conf = SparkConf().setAppName("statistics_hour").setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)

read_file = "/processed/HDeviceCGM.csv/Year=*/Month=*"
df = spark.read.option("sep", "|").option("header", "true").csv(HDFS_NAMENODE + read_file)
df = df.drop("DexInternalDtTmDaysFromEnroll").drop("DexInternalTm").drop("RecordType")\
    .withColumn("GlucoseValue", col("GlucoseValue").cast(FloatType()))\
    .withColumn("DateTime", concat(col("Date"), lit(' '), col("Time")))\
    .withColumn("DateTime", to_timestamp(col("DateTime")))\
    .withColumn("Hour", hour("Time"))


df.show(5)
# df = df.withColumn("Year", year(df["Date"])).withColumn("Month", month(df["Date"])).withColumn("Day", dayofmonth(df["Date"]))

# find how many times glucose value could be measured
# device_not_used = df_cgm.select("PtID", "DateTime", "GlucoseValue")\
#     .groupBy("PtID")\
#     .agg(min("DateTime").alias("FirstDateTime"), max("DateTime").alias("LastDateTime"), count("*").alias("Total"))\
#     .withColumn("MaxPossible", round((col("LastDateTime").cast(LongType()) - col("FirstDateTime").cast(LongType()))/300))\
#     .withColumn("DeviceNotUsed", col("MaxPossible").cast(LongType()) - col("Total"))
# device_not_used.show()
# device_not_used = device_not_used.select("PtID", "DeviceNotUsed")

# glucose level severity by hour
group_very_high = df.filter("GlucoseValue >= 250").groupBy("PtID", "Hour") \
    .agg(count("*").alias("VeryHigh"))
group_high = df.filter("GlucoseValue < 250 AND GlucoseValue >= 180").groupBy("PtID", "Hour") \
    .agg(count("*").alias("High"))
group_in_range = df.filter("GlucoseValue < 180 AND GlucoseValue >= 70").groupBy("PtID", "Hour") \
    .agg(count("*").alias("InRange"))
group_low = df.filter("GlucoseValue < 70 AND GlucoseValue >= 54").groupBy("PtID", "Hour") \
    .agg(count("*").alias("Low"))
group_very_low = df.filter("GlucoseValue < 54").groupBy("PtID", "Hour") \
    .agg(count("*").alias("VeryLow"))

join = group_very_high.join(group_high, ["PtID", "Hour"], "inner"). \
    join(group_in_range, ["PtID", "Hour"], "inner"). \
    join(group_low, ["PtID", "Hour"], "inner"). \
    join(group_very_low, ["PtID", "Hour"], "inner")

print("Glucose level severity by hour")
join.show(50)
file = "/results/severity_by_hour.csv"
join.repartition(1).write.mode("overwrite").option("header", "true").option("sep", "|").csv(
    HDFS_NAMENODE + file)