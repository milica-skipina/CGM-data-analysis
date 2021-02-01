# Analysis of continuous glucose monitor device data

Project done as part of Big Data Architectures course
## Author
Milica Škipina

## Dataset

Dataset has data collected from continuous glucose monitor (CGM) and blood glucose monitor (BGM) devices as informations about participants which include demographic and socioeconomic informations, weight, height, medications, diabetes history, hospitalizations,...

## Batch processing goals

* Analysis of blood glucose trends (very low, low, in range, high, very high) for periods of one day, month, year or all time data
* Difference of blood glucose values between male and female participants
* Dataset details (distribution of patricipants by gender, race, ethnicity and CGM use status)

## Stream processing goal

* Detection of high/low blood glucose levels

## Architecture

![Architecture](https://github.com/milica-skipina/ASVSP/blob/master/Architecture.png)

## Environment setup
Open terminal from root folder and run:
```
$ cd docker
$ docker-compose up --build
```

## Data initialization

* Download dataset from [link](https://public.jaeb.org/t1dx/stdy/329).
* Extract downloaded data into **data** folder.
* Open terminal from **data** folder and run:
```
$ chmod +x prepare_data.sh
$ ./prepare_data.sh
$ chmod +x /hadoop/dfs/copy_data.sh
$ ./hadoop/dfs/copy_data.sh
```
## Data preprocessing

* Open terminal and run:
```
$ docker exec -it spark-master bash
$ /spark/bin/spark-submit home/scripts/preprocess.py
```
## Run batch processing
* Open terminal and run:
```
$ docker exec -it spark-master bash
$ /spark/bin/spark-submit home/scripts/patients.py
$ /spark/bin/spark-submit home/scripts/statistics.py
```
## Run stream processing
* Open terminal and run:
```
$ docker exec -it namenode bash
$ hadoop fs -getmerge /produce/streaming.csv /hadoop/dfs/streaming.csv
$ hdfs dfs -copyFromLocal ./hadoop/dfs/streaming.csv /produce/file/streaming.csv
$ exit
$ docker start kafka_producer
$ docker exec -it spark-master bash
$ /spark/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 home/scripts/warning-app.py
```

## Visualization
* Open terminal from root folder and run:
```
$ docker exec -it namenode bash
$ hadoop fs -getmerge /results/gender_glucose.csv /hadoop/dfs/gender_glucose.csv
$ hadoop fs -getmerge /results/statistics.csv /hadoop/dfs/statistics.csv
$ hadoop fs -getmerge /results/statistics_percentage.csv /hadoop/dfs/statistics_percentage.csv
$ exit
$ docker cp namenode:/hadoop/dfs/gender_glucose.csv ./results/gender_glucose.csv
$ docker cp namenode:/hadoop/dfs/statistics.csv ./results/statistics.csv
$ docker cp namenode:/hadoop/dfs/statistics_percentage.csv ./results/statistics_percentage.csv
$ cd scripts
$ pip install -r requirements.txt
$ python ./visualization.py
```
