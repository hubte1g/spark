# submit example
nohup spark-submit --master yarn --deploy-mode client --conf spark.driver.memory=5G \
--conf spark.yarn.executor.extraClassPath=./ --conf spark.dynamicAllocation.minExecutors=6 \
--conf spark.dynamicAllocation.enabled=true --class com.org.App --executor-memory 2G \
--packages com.typesafe.scala-logging:scala-logging_2.11:3.9.0,com.typesafe:config:1.3.1,org.scalaj:scalaj-http_2.11:2.3.0 \
--jars ./lib/a-SNAPSHOT.jar,lib/another.jar,lib/yet-another-.jar app.jar>nohup1.txt &..
