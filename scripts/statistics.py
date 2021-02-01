import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, FloatType


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
df = df.withColumn("Year", year(df["Date"])).withColumn("Month", month(df["Date"])).withColumn("Day", dayofmonth(df["Date"]))

group_very_high = df.filter("GlucoseValue >= 250").groupBy("PtID", "Year", "Month", "Day") \
    .agg(count("*").alias("VeryHigh"))
group_high = df.filter("GlucoseValue < 250 AND GlucoseValue >= 180").groupBy("PtID", "Year", "Month", "Day") \
    .agg(count("*").alias("High"))
group_in_range = df.filter("GlucoseValue < 180 AND GlucoseValue >= 70").groupBy("PtID", "Year", "Month", "Day") \
    .agg(count("*").alias("InRange"))
group_low = df.filter("GlucoseValue < 70 AND GlucoseValue >= 54").groupBy("PtID", "Year", "Month", "Day") \
    .agg(count("*").alias("Low"))
group_very_low = df.filter("GlucoseValue < 54").groupBy("PtID", "Year", "Month", "Day") \
    .agg(count("*").alias("VeryLow"))

join = group_very_high.join(group_high, ["PtID", "Year", "Month", "Day"], "inner"). \
    join(group_in_range, ["PtID", "Year", "Month", "Day"], "inner"). \
    join(group_low, ["PtID", "Year", "Month", "Day"], "inner"). \
    join(group_very_low, ["PtID", "Year", "Month", "Day"], "inner")

join.show(20)
file = "/results/statistics.csv"
join.repartition(1).write.mode("overwrite").option("header", "true").option("sep", "|").csv(
    HDFS_NAMENODE + file)

all_statistics = join.groupBy("PtID").agg(
    sum(col("VeryHigh")).alias("VeryHigh"),
    sum(col("High")).alias("High"),
    sum(col("InRange")).alias("InRange"),
    sum(col("Low")).alias("Low"),
    sum(col("VeryLow")).alias("VeryLow"),
)

all_statistics.show(10)

result = all_statistics.withColumn("sum", all_statistics["VeryHigh"].cast(IntegerType())
                                   + all_statistics["High"].cast(IntegerType()) + all_statistics["InRange"].cast(
    IntegerType())
                                   + all_statistics["Low"].cast(IntegerType()) + all_statistics["VeryLow"].cast(
    IntegerType()))

result = result.withColumn("VeryHigh", 100*result["VeryHigh"].cast(FloatType())/result["sum"].cast(IntegerType()))\
    .withColumn("High", 100*result["High"].cast(FloatType())/result["sum"].cast(IntegerType()))\
    .withColumn("InRange", 100*result["InRange"].cast(FloatType())/result["sum"].cast(IntegerType()))\
    .withColumn("Low", 100*result["Low"].cast(FloatType())/result["sum"].cast(IntegerType()))\
    .withColumn("VeryLow", 100*result["VeryLow"].cast(FloatType())/result["sum"].cast(IntegerType()))
result = result.drop("sum")
result.show(10)

all_stat_file = "/results/statistics_percentage.csv"
result.repartition(1).write.mode("overwrite").option("head", "ture").csv(HDFS_NAMENODE + all_stat_file)
