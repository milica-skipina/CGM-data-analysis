hdfs dfs -mkdir /data
hdfs dfs -mkdir /produce
hdfs dfs -mkdir /results
hdfs dfs -mkdir /processed
hdfs dfs -mkdir /produce/file
hdfs dfs -copyFromLocal ./hadoop/dfs/HDeviceCGM.txt /data/HDeviceCGM.txt
hdfs dfs -copyFromLocal ./hadoop/dfs/HDeviceBGM.txt /data/HDeviceBGM.txt
hdfs dfs -copyFromLocal ./hadoop/dfs/HScreening.txt /data/HScreening.txt
