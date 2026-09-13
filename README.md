# CS4371_HW1

## NAVIGATE TO FILE LOCATION AND COPY TO DOCKER
```docker cp Q1Analysis.java resourcemanager:/tmp/Q1Analysis.java```

## COMPILE
```
docker exec -it resourcemanager bash -c 'mkdir -p /tmp/classes'
docker exec -it resourcemanager bash -c 'cd /tmp && javac -classpath $(hadoop classpath) -d classes Q1Analysis.java'
```
## UNZIP
```
docker exec -it resourcemanager bash -c 'cd /tmp && jar xf Q1Analysis.jar META-INF/MANIFEST.MF -O 2>/dev/null || jar xf Q1Analysis.jar META-INF/MANIFEST.MF'
```
## UPLOAD TEXT FILE
```
docker exec -it resourcemanager hadoop fs -mkdir -p /inputA
docker cp q1_dataset-1.txt resourcemanager:/tmp/q1_dataset-1.txt
docker exec -it resourcemanager hadoop fs -put /tmp/q1_dataset-1.txt /inputA/
```

## RUN JOB
```docker exec -it resourcemanager hadoop jar /tmp/Q1Analysis.jar /inputA /q1_output_A A```

## CHECK OUTPUT
```docker exec -it resourcemanager hadoop fs -cat /q1_output_A/part-r-00000```

