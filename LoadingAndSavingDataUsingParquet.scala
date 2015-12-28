
[tkb2171@phgaa005 ~]$ mkdir person
[tkb2171@phgaa005 ~]$ echo "Joshua,Lickteig,32" >> person/person.txt
[tkb2171@phgaa005 ~]$ echo "Barack,Obama,53" >> person/person.txt
[tkb2171@phgaa005 ~]$ echo "Bill,Clinton,68" >> person/person.txt

[tkb2171@phgaa005 ~]$ hdfs dfs -put person /kohls/eim/lab/im/person
[tkb2171@phgaa005 ~]$ hdfs dfs -ls /kohls/eim/lab/im

import sqlContext.createSchemaRDD
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext._

case class Person(firstName: String, lastName: String, age:Int)
val personRDD = sc.textFile("/kohls/eim/lab/im/person").map(_.split(",")).map(p=>Person(p(0),p(1),p(2).toInt))
personRDD.registerTempTable("person")
val sixtyplus = sql("select * from person where age > 60")
sixtyplus.collect.foreach(println)
sixtyplus.saveAsParquetFile("kohls/eim/lab/im/person/sp.parquet")
val ParquetLoad = sqlContext.parquetFile("kohls/eim/lab/im/person/sp.parquet")

scala> ParquetLoad.printSchema()
root
 |-- firstName: string (nullable = true)
 |-- lastName: string (nullable = true)
 |-- age: integer (nullable = false)
 
ParquetLoad.registerTempTable("sixty_plus")
sql("select firstName from sixty_plus").collect()


