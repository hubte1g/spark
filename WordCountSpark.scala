// src/main/scala/progscala2/bigdata/WordCountSpark.scalaX
package bigdata

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object SparkWordCount {
  def main(args: Array[String]) = {
    val sc = new SparkContext("local", "Word Count")
    //val input = sc.textFile(args(0)).map(_.toLowerCase)
    val input = sc.textFile("C:/Users/Kizmet/Documents/books/rgd/metapony.scala").map(_.toLowerCase)
    input
      //.flatMap(line => line.split("""\W+"""))
      .flatMap(line => line.split("""[^\p{IsAlphabetic}]+"""))
      .map(word => (word,1))
      .reduceByKey((count1, count2) => count1 + count2)
      .saveAsTextFile("C:/Users/Kizmet/Documents/books/rgd/out/metapony_alpha.txt")
    sc.stop()
  }
}

SparkWordCount("C:/Users/Kizmet/Documents/books/rgd/metapony.scala", "C:/Users/Kizmet/Documents/books/rgd/out/metapony.txt")
