//https://spark.apache.org/docs/1.5.1/api/java/org/apache/spark/sql/DataFrameStatFunctions.html

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrameStatFunctions

val df = sc.parallelize(0 until 10).toDF("id").withColumn("rand1", rand(seed=10))
      .withColumn("rand2", rand(seed=27))
    df.stat.cov("rand1", "rand2")
    res1: Double = 0.065...

// Audit metrics example
val cols: Seq[String] = Seq("e_", "m_")

    val colArr = df.columns.filter(c => (c.startsWith("e_") || c.startsWith("m_")))

    val rootCols = for (c <- colArr) yield c.slice(2,30)
    val rootColsSet = rootCols.toSet.toSeq

    rootColsSet.foreach(c => println(c + " update count: \t" + df.filter(
      s"$conditionExpr and ${"e_"+c} != ${"m_"+c}").count))
