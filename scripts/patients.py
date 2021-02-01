import os
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import datetime

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

# upis na hdfs
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]


conf = SparkConf().setAppName("patients").setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

quiet_logs(spark)

basedate = datetime.date(2015, 5, 22)

df = spark.read.option("sep", "|").option("header", "true").csv(HDFS_NAMENODE + "/data/HScreening.txt")

df_cgm = spark.read.option("sep", "|").option("header", "true").csv(HDFS_NAMENODE + "/processed/HDeviceCGM.csv/Year=*/Month=*")

df.createOrReplaceTempView("patients")

query = "SELECT PtID, Gender, Ethnicity, Race, DiagAge, Weight, Height, CGMUseStatus FROM patients"
df_patients = spark.sql(query)
df_patients.show(20, False)


print("CGMUseStatus")
cgm_use = df_patients.groupBy("CGMUseStatus").agg(count("*"))
cgm_use.show()

print("Race")
race = df_patients.groupBy("Race").agg(count("*"))
race.show()

print("Ethnicity")
ethnicity = df_patients.groupBy("Ethnicity").agg(count("*"))
ethnicity.show()

print("Gender")
gender = df_patients.groupBy("Gender").agg(count("*"))
gender.show()

join_table = df_patients.join(df_cgm, "PtID", "inner")
join_table.show(5)

join_table.createOrReplaceTempView("patient_cgm")
query = "SELECT Gender, GlucoseValue FROM patient_cgm"
df_gender_glucose = spark.sql(query)

gender_glucose_file = "/results/gender_glucose.csv"

print("Gender + Glucose")
df_gender_glucose.show(5)

df_gender_glucose.repartition(1).write.mode("overwrite").option("header", "true")\
  .csv(HDFS_NAMENODE + gender_glucose_file)
