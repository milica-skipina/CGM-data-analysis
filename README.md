# Analysis of continuous glucose monitor device data

Project done as part of Big Data Architectures course
## Author
Milica Škipina

## Architecture

![Architecture](https://github.com/milica-skipina/ASVSP/blob/master/Architecture.png)

## Dataset

Dataset has data collected from continuous glucose monitor (CGM) and blood glucose monitor (BGM) devices as informations about participants which include demographic and socioeconomic informations, weight, height, medications, diabetes history, hospitalizations,...

To prepare data for analysis:
* Download Replace-BG dataset from [link](https://public.jaeb.org/datasets/diabetes).
* Extract downloaded data into **data** folder.

## Batch processing

### Goals 

* Analysis of blood glucose trends (very low, low, in range, high, very high) for periods of one day, month, year or all time data
* Difference of blood glucose values between male and female participants
* Dataset details (distribution of patricipants by gender, race, ethnicity and CGM use status)
* Comparison of CGM and BGM values
* Filling missing values
### Environment setup
Open terminal from root folder and run:
```
$ cd docker/batch
$ docker-compose up --build
```
### Data initialization

* Open terminal from **data** folder and run:
```
$ chmod +x prepare_data.sh
$ ./prepare_data.sh
$ chmod +x /hadoop/dfs/copy_data.sh
$ ./hadoop/dfs/copy_data.sh
```
### Data preprocessing

* Open terminal and run:
```
$ docker exec -it spark-master bash
$ /spark/bin/spark-submit home/scripts/preprocess.py
```

### Run batch processing
* Open terminal and run:
```
$ docker exec -it spark-master bash
$ apk update
$ apk add make automake gcc g++ subversion python3-dev
$ pip3 install numpy
$ /spark/bin/spark-submit home/scripts/regression.py
$ /spark/bin/spark-submit home/scripts/batch_processing.py
```
### Prepare data for stream processing
* Open terminal from root folder and run:
```
$ docker exec -it namenode bash
$ hadoop fs -getmerge /produce/streaming.csv /hadoop/dfs/streaming.csv
$ exit
$ docker cp namenode:/hadoop/dfs/streaming.csv ./data/streaming.csv
```

## Stream processing 

### Goal
* Detection of high/low blood glucose levels

### Environment setup
Open terminal from root folder and run:
```
$ cd docker/streaming
$ docker-compose up --build
```

### Run stream processing
* Open terminal from root folder and run:
```
$ docker cp "./data/streaming.csv" namenode-streaming:/hadoop/dfs/streaming.csv
$ docker exec -it namenode-streaming bash
$ hdfs dfs -mkdir /produce
$ hdfs dfs -mkdir /produce/file
$ hdfs dfs -copyFromLocal ./hadoop/dfs/streaming.csv /produce/file/streaming.csv
$ exit
$ docker start kafka_producer
$ docker exec -it spark-master-streaming bash
$ /spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 home/scripts/warning-app.py
```
