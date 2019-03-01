#!/bin/bash
rm -rf hadoop_normalizer_classes/
mkdir hadoop_normalizer_classes
javac -classpath /usr/local/hadoop/hadoop-0.20.2-core.jar:/home/semih/projects/libs/commons-cli-1.2.jar:/home/semih/projects/libs/commons-logging-1.1.1.jar -d hadoop_normalizer_classes ../src/java/gps/partitioner/HadoopNormalizer.java
jar -cvf hadoop_normalizer.jar -C hadoop_normalizer_classes/ .