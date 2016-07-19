#!/bin/bash

#-----------------
# 
#-----------------

3 To execute scala code from java, you must add the scala libraries to the java class path, which on the cluster, are contained in spark-native-yarn/lib

# takes in three input parameters
# 1)  path to delimited file in HDFS
# 2)  delimiter of the file
# 3)  spark-submit --master parameter (e.g. local or yarn-cluster)

file_path=$1
delimiter=$2
master=$3

#spark-submit --class Spark_HelloWorld --jars /usr/hdp/current/share/lzo/0.6.0/lib/hadoop-lzo-0.6.0.jar --master $master target/Spark_HelloWorld.jar $file_path "$delimiter" $master

spark-submit --class Spark_HelloWorld --master $master target/Spark_HelloWorld.jar $file_path "$delimiter" $master
