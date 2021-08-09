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

conf = SparkConf().setAppName("statistics").setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)

read_file = "/processed/HDeviceCGM.csv/Year=*/Month=*"
df = spark.read.option("sep", "|").option("header", "true").csv(HDFS_NAMENODE + read_file)
df_cgm = df.drop("DexInternalDtTmDaysFromEnroll").drop("DexInternalTm").drop("RecordType")\
    .withColumn("GlucoseValue", col("GlucoseValue").cast(FloatType()))\
    .withColumn("DateTime", concat(col("Date"), lit(' '), col("Time")))\
    .withColumn("DateTime", to_timestamp(col("DateTime")))

# df = df.withColumn("Year", year(df["Date"])).withColumn("Month", month(df["Date"])).withColumn("Day", dayofmonth(df["Date"]))

# find how many times glucose value could be measured
device_not_used = df_cgm.select("PtID", "DateTime", "GlucoseValue")\
    .groupBy("PtID")\
    .agg(min("DateTime").alias("FirstDateTime"), max("DateTime").alias("LastDateTime"), count("*").alias("Total"))\
    .withColumn("MaxPossible", round((col("LastDateTime").cast(LongType()) - col("FirstDateTime").cast(LongType()))/300))\
    .withColumn("DeviceNotUsed", col("MaxPossible").cast(LongType()) - col("Total"))
device_not_used.show()
device_not_used = device_not_used.select("PtID", "DeviceNotUsed")

# values by glucose level severity for grouped by patient and date
group_very_high = df_cgm.filter("GlucoseValue >= 250").groupBy("PtID", "Date") \
    .agg(count("*").alias("VeryHigh"))
group_high = df_cgm.filter("GlucoseValue < 250 AND GlucoseValue >= 180").groupBy("PtID", "Date") \
    .agg(count("*").alias("High"))
group_in_range = df_cgm.filter("GlucoseValue < 180 AND GlucoseValue >= 70").groupBy("PtID", "Date") \
    .agg(count("*").alias("InRange"))
group_low = df_cgm.filter("GlucoseValue < 70 AND GlucoseValue >= 54").groupBy("PtID", "Date") \
    .agg(count("*").alias("Low"))
group_very_low = df_cgm.filter("GlucoseValue < 54").groupBy("PtID", "Date") \
    .agg(count("*").alias("VeryLow"))

join = group_very_high.join(group_high, ["PtID", "Date"], "inner"). \
    join(group_in_range, ["PtID", "Date"], "inner"). \
    join(group_low, ["PtID", "Date"], "inner"). \
    join(group_very_low, ["PtID", "Date"], "inner")

print("Number of measured values by glucose level severity for individual patient and date")
join.show(20)
file = "/results/statistics.csv"
join.repartition(1).write.mode("overwrite").option("header", "true").option("sep", "|").csv(
    HDFS_NAMENODE + file)

# statistics for all dates in dataset for individual patient
all_statistics = join.groupBy("PtID").agg(
    sum(col("VeryHigh")).alias("VeryHigh"),
    sum(col("High")).alias("High"),
    sum(col("InRange")).alias("InRange"),
    sum(col("Low")).alias("Low"),
    sum(col("VeryLow")).alias("VeryLow"),
)
all_statistics = all_statistics.join(device_not_used, ["PtID"], "inner")
# all_statistics.show(10)

result = all_statistics.withColumn("sum", all_statistics["VeryHigh"].cast(IntegerType())
                                   + all_statistics["High"].cast(IntegerType())
                                   + all_statistics["InRange"].cast(IntegerType())
                                   + all_statistics["Low"].cast(IntegerType())
                                   + all_statistics["VeryLow"].cast(IntegerType())
                                   + all_statistics["DeviceNotUsed"].cast(IntegerType()))

result = result.withColumn("VeryHigh", 100*result["VeryHigh"].cast(FloatType())/result["sum"].cast(IntegerType()))\
    .withColumn("High", 100*result["High"].cast(FloatType())/result["sum"].cast(IntegerType()))\
    .withColumn("InRange", 100*result["InRange"].cast(FloatType())/result["sum"].cast(IntegerType()))\
    .withColumn("Low", 100*result["Low"].cast(FloatType())/result["sum"].cast(IntegerType()))\
    .withColumn("VeryLow", 100*result["VeryLow"].cast(FloatType())/result["sum"].cast(IntegerType())) \
    .withColumn("DeviceNotUsed", 100 * result["DeviceNotUsed"].cast(FloatType()) / result["sum"].cast(IntegerType()))

result = result.drop("sum")
print("Percentage values for every measured value for the individual patient")
result.show(10)

all_stat_file = "/results/statistics_percentage.csv"
result.repartition(1).write.mode("overwrite").option("head", "ture").csv(HDFS_NAMENODE + all_stat_file)
