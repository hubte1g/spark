// Databricks notebook source exported at Wed, 18 Nov 2015 17:14:43 UTC
import org.apache.spark._
import org.apache.spark.graphx._

// COMMAND ----------

import org.apache.spark.graphx._  // Edge comes from here
import org.apache.spark.rdd.RDD

type VertexType = Tuple2[Long, String]

val vertexArray = Array[VertexType](
  (1L, "San Francisco"), 
  (2L, "London"),
  (3L, "Tokyo"),
  (4L, "Amsterdam"),
  (5L, "New York")
)

// COMMAND ----------


val edgeArray = Array(
  Edge(1L, 3L, 10), 
  Edge(2L, 3L, 20), 
  Edge(3L, 2L, 15), 
  Edge(2L, 4L, 4), 
  Edge(3L, 4L, 7), 
  Edge(3L, 5L, 2)
)

// COMMAND ----------

// Vertices are NODES on the (social) graph
val vertexRDD: RDD[VertexType] = sc.parallelize(vertexArray)

// Edges are "arrows" direction (going from one node towards another node)
val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

val g: Graph[String, Int] = Graph(vertexRDD, edgeRDD)


// COMMAND ----------

g.numEdges

// COMMAND ----------

g.numVertices

// COMMAND ----------

g.inDegrees.collect()

// COMMAND ----------

g.outDegrees.collect()

// COMMAND ----------

val subGraph = g.subgraph(vpred = (id, attr) => attr == "Tokyo")

// COMMAND ----------

subGraph.vertices.collect()

// COMMAND ----------

g.pageRank(0.0001).vertices.collect()

// COMMAND ----------

import org.apache.spark.graphx.lib.ShortestPaths
val shortest = ShortestPaths.run(g, Seq(3L))

// COMMAND ----------

shortest.vertices.take(3)

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

val vertexArray = Array(
  (1L, ("Alice", 28)),
  (2L, ("Bob", 27)),
  (3L, ("Charlie", 65)),
  (4L, ("David", 42)),
  (5L, ("Ed", 55)),
  (6L, ("Fran", 50))
  )

// COMMAND ----------

val edgeArray = Array(
  Edge(2L, 1L, 7),
  Edge(2L, 4L, 2),
  Edge(3L, 2L, 4),
  Edge(3L, 6L, 3),
  Edge(4L, 1L, 1),
  Edge(5L, 2L, 2),
  Edge(5L, 3L, 8),
  Edge(5L, 6L, 3),
  Edge(1L, 2L, 8),
  Edge(1L, 2L, 9)
  )

// COMMAND ----------

val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

// COMMAND ----------

val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

// COMMAND ----------

graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect.foreach {case (id, (name, age)) => println(s"$name is $age")}

// COMMAND ----------

for (triplet <- graph.triplets.collect) {
  println(s"${triplet.srcAttr._1} likes ${triplet.dstAttr._1}")
}

// COMMAND ----------

for (triplet <- graph.triplets.filter(t => t.attr > 5).collect) {
  println(s"${triplet.srcAttr._1} loves ${triplet.dstAttr._1}")
}

// COMMAND ----------

val inDegrees: VertexRDD[Int] = graph.inDegrees

// COMMAND ----------

inDegrees.collect()

// COMMAND ----------

// Define a class to more clearly model the user property
case class User(name: String, age: Int, inDeg: Int, outDeg: Int)
// Create a user Graph
val initialUserGraph: Graph[User, Int] = graph.mapVertices{ case (id, (name, age)) => User(name, age, 0, 0) }

// COMMAND ----------

// Fill in the degree information
val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees) {
  case (id, u, inDegOpt) => User(u.name, u.age, inDegOpt.getOrElse(0), u.outDeg)
}.outerJoinVertices(initialUserGraph.outDegrees) {
  case (id, u, outDegOpt) => User(u.name, u.age, u.inDeg, outDegOpt.getOrElse(0))
}

// COMMAND ----------

for ((id, property) <- userGraph.vertices.collect) {
  println(s"User $id is called ${property.name} and is liked by ${property.inDeg} people.")
}

// COMMAND ----------


