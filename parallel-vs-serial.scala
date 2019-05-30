def input(i: Int) = sc.parallelize(1 to i*100000)
def serial = (1 to 10).map(i => input(i).reduce(_ + _)).reduce(_ + _)
def parallel = (1 to 10).map(i => Future(input(i).reduce(_ + _))).map(Await.result(_, 10.minutes)).reduce(_ + _)


case class Test(a: Int = Random.nextInt(1000000),
                b: Double = Random.nextDouble,
                c: String = Random.nextString(1000),
                d: Seq[Int] = (1 to 100).map(_ => Random.nextInt(1000000))) extends Serializable

val input = sc.parallelize(1 to 1000000, 42).map(_ => Test()).persist(DISK_ONLY)
input.count() // Force initialization
val shuffled = input.repartition(43).count()


/**
Spark as a distributed computing engine and its main abstraction is a resilient distributed dataset (RDD), which can be viewed as a distributed collection. Basically, RDD's elements are partitioned across the nodes of the cluster, but Spark abstracts this away from the user, letting the user interact with the RDD (collection) as if it were a local one.
For different transformations on a RDD (map, flatMap, filter and others), your transformation code (closure) is:

1. serialized on the driver node,
2. shipped to the appropriate nodes in the cluster,
3. deserialized,
4. and finally executed on the nodes
**/


val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
val tbl = sqlContext.table("schema.table")


//https://www.youtube.com/watch?v=daXEp4HmS-E
import scala.collection.parallel._
import scala.concurrent.forkjoin.ForkJoinPool
val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(100))

import scala.collection.mutable.ArrayBuffer
val parts: ArrayBuffer[monthPart] = ArrayBuffer[monthPart]()

Range(2018,2019).toArray.foreach(yr => {
      Range(1,13).toArray.foreach(mnth => parts.append(monthPart(yr,mnth)))
      })

val partMonth = parts.toArray.par

 /...
