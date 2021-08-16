docker cp "./Protocol_H/Data Tables/HDeviceCGM.txt" namenode-streaming:/hadoop/dfs/HDeviceCGM.txt
docker cp "./Protocol_H/Data Tables/HDeviceBGM.txt" namenode-streaming:/hadoop/dfs/HDeviceBGM.txt
docker cp "./Protocol_H/Data Tables/HScreening.txt" namenode-streaming:/hadoop/dfs/HScreening.txt
docker cp copy_data.sh namenode-streaming:hadoop/dfs/copy_data.sh
docker cp ../scripts/ spark-master-streaming:/home

docker exec -it namenode-streaming bash
# docker exec -it spark-master bash

# /spark/bin/spark-submit home/scripts/preprocess.py
