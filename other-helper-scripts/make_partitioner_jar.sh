#!/bin/bash
rm -rf hadoop_partitioner_classes/
mkdir hadoop_partitioner_classes
javac -classpath /usr/local/hadoop/hadoop-0.20.2-core.jar:/home/semih/projects/libs/commons-cli-1.2.jar:/home/semih/projects/libs/commons-logging-1.1.1.jar -d hadoop_partitioner_classes ../src/java/gps/partitioner/HadoopPartitioner.java
jar -cvf hadoop_partitioner.jar -C hadoop_partitioner_classes/ .