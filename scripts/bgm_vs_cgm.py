import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


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
df_cgm = spark.read.option("sep", "|").option("header", "true").csv(HDFS_NAMENODE + read_file)
df_cgm = df_cgm.drop("DexInternalDtTmDaysFromEnroll").drop("DexInternalTm").drop("RecordType") \
    .drop("Day") \
    .withColumn("RecIDCGM", col("RecID").cast(IntegerType()))\
    .drop("RecID")\
    .withColumn("GlucoseValueCGM", col("GlucoseValue").cast(FloatType())) \
    .withColumn("DateTimeCGM", concat(col("Date"), lit(' '), col("Time"))) \
    .withColumn("DateTimeCGM", to_timestamp(col("DateTimeCGM")))\
    .drop("Time")\
    .drop("GlucoseValue")
df_cgm.show(5)

bgm_file = "/processed/HDeviceBGM.csv"
df_bgm = spark.read.option("sep", "|").option("header", "true").csv(HDFS_NAMENODE + bgm_file)
df_bgm = df_bgm.withColumn("GlucoseValueBGM", col("GlucoseValue").cast(FloatType())) \
    .drop("RecordType")\
    .drop("GlucoseValue")\
    .drop("RecordSubType")\
    .withColumn("DateTimeBGM", concat(col("Date"), lit(' '), col("Time"))) \
    .withColumn("DateTimeBGM", to_timestamp(col("DateTimeBGM")))\
    .drop("Time")
df_bgm.show(5)

join_df = df_bgm.join(df_cgm, ["PtID", "Date"], "inner")
join_df.show(5)

join_df = join_df.withColumn("DiffInSeconds", abs(col("DateTimeBGM").cast(LongType()) - col("DateTimeCGM").cast(LongType())))\
    .withColumn("DiffInMinutes",round(col("DiffInSeconds")/60))

join_df.filter("PtID == 19").orderBy(["DiffInSeconds", "DateTimeBGM"]).show(50)

join_df = join_df.filter("DiffInSeconds < 300")\
    .withColumn("GlucoseValueDiff", abs(col("GlucoseValueCGM") - col("GlucoseValueBGM")))

join_df.show(50)
join_df.agg(min("GlucoseValueDiff"), max("GlucoseValueDiff")).show()
join_df.orderBy("GlucoseValueDiff", ascending = False).show(50)
join_df.orderBy("GlucoseValueDiff", ascending = True).show(50)

join_df.orderBy("GlucoseValueDiff").groupBy("GlucoseValueDiff").count().show(250)


bgm_vs_cgm_file = "/results/bgm_vs_cgm.csv"
join_df.repartition(1).write.mode("overwrite").option("header", "true").option("sep", "|").csv(
    HDFS_NAMENODE + bgm_vs_cgm_file)
# dupes = join_df.groupBy("RecID").count().filter("count > 1")
# dupes.join(join_df, "RecID").orderBy("RecID").show(50)

