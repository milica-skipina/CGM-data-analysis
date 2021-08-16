from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.sql import Row, SparkSession, DataFrame
from pyspark.sql.types import *
import json
import time
from datetime import datetime
import os


KAFKA_BROKER = "kafka:19092"
TOPIC = "warning"
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
# client_hdfs = InsecureClient(HDFS_NAMENODE)

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession \
            .builder \
            .config(conf=sparkConf) \
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)


if __name__ == "__main__":

    sc = SparkContext(appName="Warning-App")
    quiet_logs(sc)
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 15)

    ssc.checkpoint("stateful_checkpoint_direcory")

    kvs = KafkaUtils.createStream(ssc, "zoo:2181", "spark-streaming-consumer", {"cgm": 1})

    lines = kvs.map(lambda x: json.loads(x[1]))


    def reduceFunc(a, b):
        return a[0] + b[0], a[1] + b[1]


    def invFunc(a, b):
        return a[0] - b[0], a[1] - b[1]


    def mapGlucoseValue(json_value):
        ptID = json_value["PtID"]
        value = json_value["GlucoseValue"]
        return ptID, (value, 1)


    def filterFunction(value):
        file_name = '/streaming/' + str(value[0]) + '.csv'
        columns = ["DateTime", "PtID", "meanValue"]
        date = datetime.now()
        data = [(date, value[0], value[1])]
        spark = SparkSession.builder.getOrCreate()
        # spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        df = spark.createDataFrame(data, columns)
        df.coalesce(1).write.save(path=file_name, format='csv', mode='append', sep='|', header=True)
        if value[1] > 180:
            print("Patient: " + str(value[0]) + " has too high glucose value (mean value: " + str(value[1]) + ")")
        if value[1] < 70:
            print("Patient: " + str(value[0]) + " has too low glucose value (mean value: " + str(value[1]) + ")")
            return True
        return False


    values = lines.map(mapGlucoseValue).reduceByKeyAndWindow(reduceFunc, invFunc, 60, 15) \
        .map(lambda k: (k[0], k[1][0] / k[1][1])).filter(filterFunction)
    values.pprint(10)

    ssc.start()
    ssc.awaitTermination()
