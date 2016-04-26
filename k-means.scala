import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

val numEpsilon = 1e-10    // Tolerance
val numClusters = 5        // K -Value - Number of clusters
val numIterations = 100    //Max number of iterations

// Load and parse the data
val data = sc.textFile("hdfs://localhost:8020/user/musigma/norm_CAR/part-r-00000") // Load from HDFS

val parsedData = data.map(s => Vectors.dense(s.split("\t").map(_.toDouble))) //Parse into RDD Vector

parsedData.cache()    // Cache to Memory for high performance

//Initialize model

val myCluster=new KMeans()
myCluster.setEpsilon(numEpsilon)
myCluster.setK(numClusters)
myCluster.setMaxIterations(numIterations)
myCluster.setRuns(2)

//Run clustering and set object

val myClusterModel = myCluster.run(parsedData)

val myCentroids = myClusterModel.clusterCenters

val results = myClusterModel.predict(parsedData)

results.coalesce(1,true).saveAsTextFile("hdfs://localhost:8020/user/musigma/temp")

//creating two arrays and appending them

val result1 = data.collect()

val result2 = results.collect()

var myArr = List[String]()

for (i <- 0 to (result1.length - 1)) 
{
	myArr = result1(i) + "\t" + result2(i) :: myArr
}

//returns an rdd

sc.parallelize(myArr).saveAsTextFile("hdfs://localhost:8020/user/musigma/test_spark_out")
