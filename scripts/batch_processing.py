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
conf = SparkConf().setAppName("batch_processing").setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)
quiet_logs(spark)

print("-------------------- Patients - Screening data --------------------")
screening_file = "/data/HScreening.txt"
screening_df = spark.read.option("sep", "|").option("header", "true").csv(HDFS_NAMENODE + screening_file)
screening_df.createOrReplaceTempView("patients")
query = "SELECT PtID, Gender, Ethnicity, Race, DiagAge, Weight, Height, CGMUseStatus FROM patients"
df_patients = spark.sql(query)
df_patients.show(5, False)

print("-------------------- CGM data --------------------")
cgm_file = "/processed/HDeviceCGM.csv/Year=*/Month=*"
df_cgm = spark.read.option("sep", "|").option("header", "true").csv(HDFS_NAMENODE + cgm_file)
df_cgm = df_cgm.drop("DexInternalDtTmDaysFromEnroll").drop("DexInternalTm").drop("RecordType") \
    .withColumn("GlucoseValue", col("GlucoseValue").cast(FloatType())) \
    .withColumn("DateTime", concat(col("Date"), lit(' '), col("Time"))) \
    .withColumn("DateTime", to_timestamp(col("DateTime"))) \
    .withColumn("Hour", hour("Time"))
df_cgm.show(5)
# df_cgm = df_cgm.filter("GlucoseValue <= 500")

# find how many times glucose value could be measured
device_not_used = df_cgm.select("PtID", "DateTime", "GlucoseValue") \
    .groupBy("PtID") \
    .agg(min("DateTime").alias("FirstDateTime"), max("DateTime").alias("LastDateTime"), count("*").alias("Total")) \
    .withColumn("MaxPossible",
                round((col("LastDateTime").cast(LongType()) - col("FirstDateTime").cast(LongType())) / 300)) \
    .withColumn("DeviceNotUsed", col("MaxPossible").cast(LongType()) - col("Total"))
device_not_used = device_not_used.select("PtID", "DeviceNotUsed")
print("-------------------- Device not used by patient --------------------")
device_not_used.show(5)


print("-------------------- BGM data --------------------")
bgm_file = "/processed/HDeviceBGM.csv"
df_bgm = spark.read.option("sep", "|").option("header", "true").csv(HDFS_NAMENODE + bgm_file)
df_bgm = df_bgm.withColumn("GlucoseValueBGM", col("GlucoseValue").cast(FloatType())) \
    .withColumn("RecIDBGM", col("RecID").cast(IntegerType())) \
    .drop("RecID") \
    .drop("RecordType") \
    .drop("GlucoseValue") \
    .drop("RecordSubType") \
    .withColumn("DateTimeBGM", concat(col("Date"), lit(' '), col("Time"))) \
    .withColumn("DateTimeBGM", to_timestamp(col("DateTimeBGM"))) \
    .drop("Time")
df_bgm.show(5)
df_bgm = df_bgm.filter("GlucoseValueBGM <= 500")

print("-------------------- CGMUseStatus --------------------")
cgm_use = df_patients.groupBy("CGMUseStatus").agg(count("*"))
cgm_use.show()

print("-------------------- Race --------------------")
race = df_patients.groupBy("Race").agg(count("*"))
race.show()

print("-------------------- Ethnicity --------------------")
ethnicity = df_patients.groupBy("Ethnicity").agg(count("*"))
ethnicity.show()

print("-------------------- Gender --------------------")
gender = df_patients.groupBy("Gender").agg(count("*"))
gender.show()

print("-------------------- Screening and CGM data --------------------")
patients_cgm = df_patients.join(df_cgm, "PtID", "inner")
patients_cgm.show(10)

print("-------------------- Gender + Glucose --------------------")
patients_cgm.createOrReplaceTempView("patient_cgm")
query = "SELECT PtID, Gender, GlucoseValue FROM patient_cgm"
df_gender_glucose = spark.sql(query)
df_gender_glucose.show(10)
gender_glucose_file = "/results/gender_glucose.csv"
df_gender_glucose.repartition(1).write.mode("overwrite").option("header", "true") \
    .csv(HDFS_NAMENODE + gender_glucose_file)

# statistics
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

statistics_df = group_very_high.join(group_high, ["PtID", "Date"], "inner"). \
    join(group_in_range, ["PtID", "Date"], "inner"). \
    join(group_low, ["PtID", "Date"], "inner"). \
    join(group_very_low, ["PtID", "Date"], "inner")

print("Number of measured values by glucose level severity for individual patient and date")
statistics_df.show(10)
statistics_file = "/results/statistics.csv"
statistics_df.repartition(1).write.mode("overwrite").option("header", "true").option("sep", "|").csv(
    HDFS_NAMENODE + statistics_file)

print("Percentage values for every measured value for every patient")
all_statistics = statistics_df.groupBy("PtID").agg(
    sum(col("VeryHigh")).alias("VeryHigh"),
    sum(col("High")).alias("High"),
    sum(col("InRange")).alias("InRange"),
    sum(col("Low")).alias("Low"),
    sum(col("VeryLow")).alias("VeryLow"),
)
all_statistics = all_statistics.join(device_not_used, ["PtID"], "inner")
result = all_statistics.withColumn("sum", all_statistics["VeryHigh"].cast(IntegerType())
                                   + all_statistics["High"].cast(IntegerType())
                                   + all_statistics["InRange"].cast(IntegerType())
                                   + all_statistics["Low"].cast(IntegerType())
                                   + all_statistics["VeryLow"].cast(IntegerType())
                                   + all_statistics["DeviceNotUsed"].cast(IntegerType()))
result = result.withColumn("VeryHigh", 100 * result["VeryHigh"].cast(FloatType()) / result["sum"].cast(IntegerType())) \
    .withColumn("High", 100 * result["High"].cast(FloatType()) / result["sum"].cast(IntegerType())) \
    .withColumn("InRange", 100 * result["InRange"].cast(FloatType()) / result["sum"].cast(IntegerType())) \
    .withColumn("Low", 100 * result["Low"].cast(FloatType()) / result["sum"].cast(IntegerType())) \
    .withColumn("VeryLow", 100 * result["VeryLow"].cast(FloatType()) / result["sum"].cast(IntegerType())) \
    .withColumn("DeviceNotUsed", 100 * result["DeviceNotUsed"].cast(FloatType()) / result["sum"].cast(IntegerType()))
result = result.drop("sum")
result.show(10)

all_stat_file = "/results/statistics_percentage.csv"
result.repartition(1).write.mode("overwrite").option("head", "ture").csv(HDFS_NAMENODE + all_stat_file)

# glucose level severity by hour
group_very_high = df_cgm.filter("GlucoseValue >= 250").groupBy("PtID", "Hour") \
    .agg(count("*").alias("VeryHigh"))
group_high = df_cgm.filter("GlucoseValue < 250 AND GlucoseValue >= 180").groupBy("PtID", "Hour") \
    .agg(count("*").alias("High"))
group_in_range = df_cgm.filter("GlucoseValue < 180 AND GlucoseValue >= 70").groupBy("PtID", "Hour") \
    .agg(count("*").alias("InRange"))
group_low = df_cgm.filter("GlucoseValue < 70 AND GlucoseValue >= 54").groupBy("PtID", "Hour") \
    .agg(count("*").alias("Low"))
group_very_low = df_cgm.filter("GlucoseValue < 54").groupBy("PtID", "Hour") \
    .agg(count("*").alias("VeryLow"))

print("-------------------- Glucose level severity by hour --------------------")
glucose_by_hour = group_very_high.join(group_high, ["PtID", "Hour"], "inner"). \
    join(group_in_range, ["PtID", "Hour"], "inner"). \
    join(group_low, ["PtID", "Hour"], "inner"). \
    join(group_very_low, ["PtID", "Hour"], "inner")
glucose_by_hour.show(10)
glucose_by_hour_file = "/results/severity_by_hour.csv"
glucose_by_hour.repartition(1).write.mode("overwrite").option("header", "true").option("sep", "|").csv(
    HDFS_NAMENODE + glucose_by_hour_file)

print("Glucose severity by gender")
df_gender_glucose_sev = df_gender_glucose.withColumn("GlucoseSeverity",
                             when(col("GlucoseValue") >= 250, "VeryHigh").
                             when((col("GlucoseValue") < 250) & (col("GlucoseValue") >= 180), "High").
                             when((col("GlucoseValue") < 180) & (col("GlucoseValue") >= 70), "InRange").
                             when((col("GlucoseValue") < 70) & (col("GlucoseValue") >= 54), "Low").
                             otherwise("VeryLow"))
df_gender_glucose_sev.groupBy("Gender", "GlucoseSeverity").count().show()

print("Device not used by gender")
device_not_used_gender = device_not_used.join(df_gender_glucose, ["PtID"], "inner")
device_not_used_gender.groupBy("Gender").agg(mean("DeviceNotUsed").alias("DeviceNotUsedMean"))\
    .withColumn("DeviceNotUsedMean(hour)", col("DeviceNotUsedMean")*5/3600).show()


# CGM VS BGM
print("-------------------- CGM and BGM --------------------")
cgm_vs_bgm = df_bgm.join(df_cgm, ["PtID", "Date"], "inner")
cgm_vs_bgm.show(5)

print("-------------------- CGM VS BGM --------------------")
cgm_vs_bgm = cgm_vs_bgm.withColumn("DiffInSeconds",
                                   abs(col("DateTimeBGM").cast(LongType()) - col("DateTime").cast(LongType()))) \
    .withColumn("DiffInMinutes", round(col("DiffInSeconds") / 60))

cgm_vs_bgm = cgm_vs_bgm.filter("DiffInSeconds < 300") \
    .withColumn("GlucoseValueDiff", abs(col("GlucoseValue") - col("GlucoseValueBGM")))
cgm_vs_bgm.show(10)
cgm_vs_bgm.agg(max("GlucoseValueDiff"), mean("GlucoseValueDiff")).show()
print("Top 10 with the highest glucose value difference")
cgm_vs_bgm.orderBy("GlucoseValueDiff", ascending=False).show(10)
print("Top 10 with the smallest glucose value difference")
cgm_vs_bgm.orderBy("GlucoseValueDiff", ascending=True).show(10)
temp = cgm_vs_bgm.groupBy("GlucoseValueDiff").count()
print("GlucoseValueDiff count - top 20")
temp.orderBy("count", ascending=False).show(20)

cgm_vs_bgm = cgm_vs_bgm.withColumn("PercentageDiff", when((col("GlucoseValueBGM") > 80),
                                                          col("GlucoseValueDiff") / col("GlucoseValueBGM")).otherwise(
    -1))

bad_value = cgm_vs_bgm.filter((col("PercentageDiff") > 20) |
                              ((col("PercentageDiff") == -1) & (col("GlucoseValueDiff") > 20)))

print("Bad value CGM VS BGM " + str(bad_value.count()) + "of total " + str(cgm_vs_bgm.count()) + " values")
bad_value.show(10)

bgm_vs_cgm_file = "/results/bgm_vs_cgm.csv"
cgm_vs_bgm.repartition(1).write.mode("overwrite").option("header", "true").option("sep", "|").csv(
    HDFS_NAMENODE + bgm_vs_cgm_file)

print("GlucoseSeverity in bad values")
bad_value = bad_value.withColumn("GlucoseSeverity",
                                 when(col("GlucoseValueBGM") >= 250, "VeryHigh").
                                 when((col("GlucoseValueBGM") < 250) & (col("GlucoseValueBGM") >= 180), "High").
                                 when((col("GlucoseValueBGM") < 180) & (col("GlucoseValueBGM") >= 70), "InRange").
                                 when((col("GlucoseValueBGM") < 70) & (col("GlucoseValueBGM") >= 54), "Low").
                                 otherwise("VeryLow"))
bad_value.groupBy("GlucoseSeverity").count().show()

print("Does CGM higher or lower than BGM")
temp = bad_value.withColumn("Higher_Lower", when((col("GlucoseValue") - col("GlucoseValueBGM")) > 0, "H").otherwise("L"))
temp.groupBy("Higher_Lower").count().show()

print("Mean value of BGM for GlucoseDiff > 100")
bad_value.agg(min("GlucoseValueBGM"), max("GlucoseValueBGM"), mean("GlucoseValueBGM")).show()

print("GlucoseValueDiff > 100")
bad_value.filter("GlucoseValueDiff > 100").groupBy("GlucoseSeverity").count().show()
