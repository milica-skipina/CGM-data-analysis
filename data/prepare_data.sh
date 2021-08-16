docker cp "./REPLACE-BG Dataset/Data Tables/HDeviceCGM.txt" namenode:/hadoop/dfs/HDeviceCGM.txt
docker cp "./REPLACE-BG Dataset/Data Tables/HDeviceBGM.txt" namenode:/hadoop/dfs/HDeviceBGM.txt
docker cp "./REPLACE-BG Dataset/Data Tables/HScreening.txt" namenode:/hadoop/dfs/HScreening.txt
docker cp copy_data.sh namenode:hadoop/dfs/copy_data.sh
docker cp ../scripts/ spark-master:/home
docker exec -it namenode bash
