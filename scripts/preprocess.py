import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, year, month, dayofmonth, date_add, expr, lit
from pyspark.sql.types import *
import datetime

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

conf = SparkConf().setAppName("cgm").setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)

basedate = '2015-05-22'
# basedate = datetime.date(2015, 5, 22) 

df = spark.read.option("sep", "|").option("header", "true").csv("hdfs://namenode:9000/data/HDeviceCGM.txt")

df = df.filter("RecordType == 'CGM'")

df = df.withColumnRenamed("DeviceDtTmDaysFromEnroll", "Date")\
    .withColumnRenamed("DeviceTm", "Time")\

df = df.withColumn("Date", df["Date"].cast(IntegerType()))\
    .withColumn("DexInternalDtTmDaysFromEnroll", df["DexInternalDtTmDaysFromEnroll"].cast(IntegerType()))
df = df.withColumn("BaseDate", lit(basedate))

df.show(10)
df.printSchema()

df = df.withColumn("Date", expr("date_add(BaseDate, Date)"))\
     .withColumn("DexInternalDtTmDaysFromEnroll",expr("date_add(BaseDate, DexInternalDtTmDaysFromEnroll)"))

df = df.withColumn("Year", year(df["Date"])).withColumn("Month", month(df["Date"]))\
    .withColumn("Day", dayofmonth(df["Date"]))

df_data = df.filter("Year < 2016")
df_produce = df.filter("Year >= 2016")

df_data = df_data.repartition("Year", "Month")
df.drop("BaseDate")
file = "/processed/HDeviceCGM.csv"
file_produce = "/produce/streaming.csv"
df_data.write.partitionBy("Year", "Month").mode("overwrite").option("header", "true").option("sep", "|").csv(HDFS_NAMENODE + file)
df_produce.repartition(1).write.mode("overwrite").option("header", "true").csv(HDFS_NAMENODE + file_produce)

bgm_file = "/data/HDeviceBGM.txt"
df_bgm = spark.read.option("sep", "|").option("header", "true").csv(HDFS_NAMENODE + bgm_file)

write_bgm = "/processed/HDeviceBGM.csv"


df_bgm = df_bgm.withColumnRenamed("DeviceDtTmDaysFromEnroll", "Date")\
    .withColumnRenamed("DeviceTm", "Time")
df_bgm.show(5)
df_bgm = df_bgm.withColumn("Date", df_bgm["Date"].cast(IntegerType()))
df_bgm = df_bgm.withColumn("BaseDate", lit(basedate))
df_bgm = df_bgm.withColumn("Date", expr("date_add(BaseDate, Date)"))
df_bgm.repartition(1).write.mode("overwrite").option("header", "true").csv(HDFS_NAMENODE + write_bgm)
df_bgm.drop("BaseDate")
df.show(20)
df_bgm.show(20)
