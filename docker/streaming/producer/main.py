#!/usr/bin/python3

import os
import time
from kafka import KafkaProducer
import kafka.errors
import pandas as pd
from hdfs import InsecureClient
from pyspark.sql import SparkSession

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = os.environ["TOPIC"]
HDFS_NAMENODE = os.environ["HDFS_HOSTNAME"]

client_hdfs = InsecureClient(HDFS_NAMENODE)

while True:
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER.split(","))
        print("Producer: Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)


file_name = "/produce/file/streaming.csv"
with client_hdfs.read(file_name, encoding = 'utf-8') as reader:
    df = pd.read_csv(reader, low_memory=False)
    df.info(verbose=False)
    df = df[df['PtID'].notna()]
    df['PtID'] = df['PtID'].astype(int)
    df['Year'] = df['Year'].astype(int)
    df['Month'] = df['Month'].astype(int)
    df['Day'] = df['Day'].astype(int)
    print(df.head(10))
    df = df.sort_values(by=['Date', 'Time'])
    for i, row in df.iterrows():
        print(row.to_json())
        producer.send(TOPIC, key=bytes(str(row['RecID']), 'utf-8'), value=bytes(row.to_json(), 'utf-8'))
        if i % 132 == 0:
            time.sleep(15)
        df_write = df.loc[i]

        # # Write to hdfs
        # file = '/data/HDeviceCGM.txt'
        # spark = SparkSession.builder.getOrCreate()
        # df = spark.createDataFrame(df_write)
        # df.coalesce(1).write.save(path=file_name, format='txt', mode='append', sep='|', header=True)
