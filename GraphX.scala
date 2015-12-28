// Databricks notebook source exported at Wed, 18 Nov 2015 17:25:28 UTC
// MAGIC %md
// MAGIC 
// MAGIC #![Wikipedia Logo](http://sameerf-dbc-labs.s3-website-us-west-2.amazonaws.com/data/wikipedia/images/w_logo_for_labs.png)
// MAGIC 
// MAGIC # Analyzing the Wikipedia clickstream with GraphX
// MAGIC ### Time to complete: 20 minutes
// MAGIC 
// MAGIC #### Business questions:
// MAGIC 
// MAGIC * Question # 1) Can you confirm that there is an article about Amsterdam in the graph?
// MAGIC * Question # 2) How many articles in the graph have the string "Amsterdam" or "amsterdam" somewhere in the title?
// MAGIC * Question # 3) Which articles refer to which other articles.. and how many times? (triplets view)
// MAGIC * Question # 4) Which articles have the highest number of other articles referring to them? (in-degree)
// MAGIC * Question # 5) What are the most influential articles in the Feb 2015 clickstream? (PageRank)
// MAGIC * Question # 6) What does the graph of connections between the top 7 most populated cities on earth look like? (Subgraph)
// MAGIC * Question # 7) In the cities subgraph, what is the shortest path between the Shanghai and Mumbai articles? (shortest path)
// MAGIC * Question # 8) Can you create a visualization of the shortest path between 2 articles?
// MAGIC 
// MAGIC 
// MAGIC #### Technical Accomplishments:
// MAGIC 
// MAGIC * Combine a vertex RDD and edge RDD to create a Property Graph
// MAGIC * Convert a DataFrame to an RDD
// MAGIC * Yank out filtered vertices, edges and triplets from a graph
// MAGIC * Learn how to count the number of vertices and edges in a graph (to understand it's complexity)
// MAGIC * Calculate in-degrees of a vertex
// MAGIC * Join a graph to a DataFrame (via RDD API)
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC Dataset: http://datahub.io/dataset/wikipedia-clickstream/resource/be85cc68-d1e6-4134-804a-fd36b94dbb82

// COMMAND ----------

// MAGIC %md Attach to, and then restart your cluster first to clear out old caches and get to a default, standard environment. The restart should take 1 - 2 minutes.
// MAGIC 
// MAGIC #![Restart cluster](http://i.imgur.com/xkRjRYy.png)

// COMMAND ----------

// MAGIC %md Import some packages that we'll be needing in this lab:

// COMMAND ----------

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.graphx.lib.ShortestPaths

// COMMAND ----------

// MAGIC %md
// MAGIC ### Thinking in graphs
// MAGIC 
// MAGIC In this lab, we will revisit the Clickstream data, but this time analyze it using graph algorithms.
// MAGIC 
// MAGIC Graphs are made of **vertices** and **edges**. Vertices are the items or nodes in the graph, which in our case will be the Wikipedia article titles. Edges are the connections between the items. In this lab the edges will be the `(referrer, resource)` pairs. Edges (connections) can have weights assigned to them, which in our case will be the number of occurrences of the `(referrer, resource)` pair (so the `n` column in the dataframe).

// COMMAND ----------

// MAGIC %md ### Create a DataFrame from the clickstream data
// MAGIC 
// MAGIC Let's re-use our code from the DataFrames clickstream lab to make a DataFrame:

// COMMAND ----------

// Create a DataFrame and cache it
val clickstreamDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\\t").option("mode", "PERMISSIVE").option("inferSchema", "true").load("dbfs:///mnt/wikipedia-readonly-eu/clickstream").select($"prev_id".cast("Int").alias("prev_id"), $"curr_id".cast("Int").alias("curr_id"),$"prev_title", $"curr_title", $"n".cast("Int").alias("n"), $"type").cache()

// COMMAND ----------

clickstreamDF.show(5)

// COMMAND ----------

// MAGIC %md Notice in the Spark UI's Storage tab that when the above command is run, that only 5% of the DF gets cached (b/c that is all we had to read from disk to do the `.show()`):

// COMMAND ----------

// MAGIC %md #![5percent](http://i.imgur.com/6rWWX7u.png)

// COMMAND ----------

// MAGIC %md ### Create a Property Graph
// MAGIC 
// MAGIC ##### + Step 1: Create an articles RDD (our vertex RDD)
// MAGIC ##### + Step 2: Create a connections RDD (our edge RDD)
// MAGIC ##### + Step 3: Combine the two RDDs into a Property Graph
// MAGIC 
// MAGIC Instead of modeling our data as rows and columns, let's model it as a graph. To make a graph in GraphX, we need two RDDs: one RDD holding the vertices (the article titles) and another RDD holding the edges (the referrer -> resource connections).

// COMMAND ----------

// MAGIC %md We'll create the vertices/articles RDD first. We'll begin by transforming the `clickstreamDF` dataframe into a new dataframe with just the article IDs and article titles:

// COMMAND ----------

// It will take 2-3 minutes to run this command as it has to read in 1.2 GB of data from S3

val uniqueArticlesDF = clickstreamDF.select($"curr_id", $"curr_title").distinct()

uniqueArticlesDF.show(10)

// COMMAND ----------

// MAGIC %md The remaining 95% of the `clickstreamDF` DataFrame has now been read from S3 (and then also cached). The Spark UI's Storage tab now shows the DataFrame as 100% cached:

// COMMAND ----------

// MAGIC %md #![100percent](http://i.imgur.com/0toIzFb.png)

// COMMAND ----------

// MAGIC %md How many articles are there total?:

// COMMAND ----------

uniqueArticlesDF.count()

// COMMAND ----------

// MAGIC %md The clickstream data contains 3.1 million unique articles.

// COMMAND ----------

// MAGIC %md In Feb 2015 there were about 3 million unique articles requested from Wikipedia.

// COMMAND ----------

// MAGIC %md #### Step 1: Create an articles RDD (our vertex RDD)

// COMMAND ----------

// MAGIC %md Convert the `uniqueArticlesDF` DataFrame into an RDD and look at the first 3 items:

// COMMAND ----------

uniqueArticlesDF.rdd.take(3)

// COMMAND ----------

// MAGIC %md The unique articles RDD contains a bunch of objects of type: `org.apache.spark.sql.Row`. Each object is a tuple of article ID and article name.

// COMMAND ----------

// MAGIC %md To use this RDD in GraphX, we have to convert from the `RDD[Row]` objects to objects of type: `RDD[(VertexId, String)]`. We can use a for loop like this to explode each `Row` object into an `Integer` and `String`:

// COMMAND ----------

for ( x <- uniqueArticlesDF.rdd.take(3)) {
  println(s"0 -> ${x(0).asInstanceOf[Int]}, 1 -> ${x(1).asInstanceOf[String]}")
}

// COMMAND ----------

// MAGIC %md This time instead of printing the converted objects, let's store the converted (transformed) objects in a new RDD, `articlesRDD`:

// COMMAND ----------

val articlesRDD = uniqueArticlesDF.rdd.map { row =>
  // Map to the vertex tuple type
  (row(0).asInstanceOf[Int].toLong, row(1).asInstanceOf[String])
}

// Here is a more idomatic Scala way to accomplish the same thing:

/*
val articlesRDD = uniqueArticlesDF.map {
  // Map to the vertex tuple type
  case Row(id: Int, title: String) =>
    (id.toLong, title)
}
*/

// COMMAND ----------

// MAGIC %md The `articlesRDD` contains an array of `(Long, String)` tuples:

// COMMAND ----------

articlesRDD.take(3)

// COMMAND ----------

// MAGIC %md So each vertex is keyed by a unique 64-bit long identifier, which will become our VertexID (the article ID).

// COMMAND ----------

// MAGIC %md #### Step 2: Create a connections RDD (our edge RDD)

// COMMAND ----------

// MAGIC %md Notice that our original DataFrame contains null values in the id columns:

// COMMAND ----------

clickstreamDF.show(2)

// COMMAND ----------

// MAGIC %md Drop out the null values:

// COMMAND ----------

val clickstreamWithoutNullsDF = clickstreamDF.na.drop()

clickstreamWithoutNullsDF.show(10)

// COMMAND ----------

// MAGIC %md Next, select out just the 3 columns we need to create edges, distinctly:

// COMMAND ----------

val clickstreamWithoutNulls_3cols_DF = clickstreamWithoutNullsDF.select($"prev_id", $"curr_id", $"n").distinct()

clickstreamWithoutNulls_3cols_DF.show(5)

// COMMAND ----------

// MAGIC %md Finally, we are ready to create the `edgesRDD`:

// COMMAND ----------

// Convert the DataFrame to an RDD, then use a map transformation to convert each row object to an Edge type
val edgesRDD = clickstreamWithoutNulls_3cols_DF.rdd.map { row =>
  // Map to the edge tuple type
  Edge(row(0).asInstanceOf[Int].toLong, row(1).asInstanceOf[Int].toLong, row(2).asInstanceOf[Int])
}

// COMMAND ----------

// MAGIC %md The `edgesRDD` contains `(referrer, resource, weight)` tuples:

// COMMAND ----------

edgesRDD.take(3)

// COMMAND ----------

// MAGIC %md Edges have corresponding source (referrer) and destination vertex (resource) identifiers and a third parameter for a label for the edge. In our case, we will use the weight of that edge/connection as the label.

// COMMAND ----------

// MAGIC %md #### Step 3: Combine the two RDDs into a Property Graph

// COMMAND ----------

// MAGIC %md Create a property graph named `clickstreamGRAPH`:

// COMMAND ----------

val clickstreamGRAPH: Graph[String, Int] = Graph(articlesRDD, edgesRDD)

// COMMAND ----------

// MAGIC %md Like RDDs, property graphs are immutable, distributed, and fault-tolerant. Changes to the values or structure of the graph are accomplished by producing a new graph with the desired changes.

// COMMAND ----------

// MAGIC %md We can extract out the vertices (articles) and edges (connections) from the graph. 
// MAGIC 
// MAGIC Take a look at the first few vertices:

// COMMAND ----------

clickstreamGRAPH.vertices.take(3)

// COMMAND ----------

// MAGIC %md Hmm, that took about 30 seconds. Let's cache our graph to speed it up!

// COMMAND ----------

// Note the full graph cache will actually materialize the first time we do a full read of the graph, not when we do smaller take operations
clickstreamGRAPH.cache

// COMMAND ----------

// MAGIC %md Here are the first few edges:

// COMMAND ----------

clickstreamGRAPH.edges.take(3)

// COMMAND ----------

// MAGIC %md How many vertices (nodes) are there in our graph? 

// COMMAND ----------

// Now our graph will be fully cached in memory
clickstreamGRAPH.vertices.count

// COMMAND ----------

// Here's another way to get the number of vertices

clickstreamGRAPH.numVertices

// COMMAND ----------

// MAGIC %md There are about 3 million vertices (articles) in the graph.

// COMMAND ----------

// MAGIC %md How many edges (connections) are there in the graph?

// COMMAND ----------

clickstreamGRAPH.edges.count

// COMMAND ----------

clickstreamGRAPH.numEdges

// COMMAND ----------

// MAGIC %md There are about 14 million connections.

// COMMAND ----------

// MAGIC %md You can learn about the Graph operations in the Spark API docs:
// MAGIC https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.graphx.Graph
// MAGIC 
// MAGIC You may want to keep this link open in a new tab as a reference for the rest of the lab.

// COMMAND ----------

// MAGIC %md #### Question 1: Can you confirm that there is an article about Amsterdam in the graph?

// COMMAND ----------

// MAGIC %md Is there an article about Amsterdam in our graph?

// COMMAND ----------

clickstreamGRAPH.vertices.filter {case (id, article) => article == "Amsterdam"}.count

// COMMAND ----------

// MAGIC %md 1 means true. So yes, there is.

// COMMAND ----------

// MAGIC %md ** Challenge:** Is there an article about Apache_Spark in our graph?

// COMMAND ----------

//Type in your answer below...



// COMMAND ----------

// Answer to Challenge
clickstreamGRAPH.vertices.filter {case (id, article) => article == "Apache_Spark"}.count

// COMMAND ----------

// MAGIC %md #### Question 2: How many articles in the graph have the string "Amsterdam" or "amsterdam" somewhere in the title?

// COMMAND ----------

// MAGIC %md We'll use regular expressions to accomplish this. First, let's define a Regex and test it locally in the Driver JVM:

// COMMAND ----------

import scala.util.matching.Regex

// Create the Regex
val pattern = new Regex("(A|a)msterdam")

// Create a string for testing
val str = "One of my favorite cities is Amsterdam. aaamsterdam!"

println((pattern findAllIn str).mkString(","))

// COMMAND ----------

// Test the Regex
println((pattern findAllIn str).mkString(","))

// COMMAND ----------

// MAGIC %md Great, our Regex worked. Next, use the Regex to filter all of the articles in the graph according to the pattern:

// COMMAND ----------

val pattern = new Regex("(A|a)msterdam")
clickstreamGRAPH.vertices.filter {case (id, article) => article != null && (pattern findAllIn article).nonEmpty}.count

// COMMAND ----------

// MAGIC %md Wow, 336 articles titles contain the pattern "Amsterdam" or "amsterdam" somewhere in the title.

// COMMAND ----------

// MAGIC %md #### Question 3: Which articles refer to which other articles.. and how many times? (triplet view)

// COMMAND ----------

// MAGIC %md In addition to the vertex and edge views of the property graph, GraphX also exposes a triplet view. The triplet view logically joins the vertex and edge properties yielding an RDD of triplets.

// COMMAND ----------

// MAGIC %md #![Triplets](http://i.imgur.com/P2ZgZKR.png)

// COMMAND ----------

// MAGIC %md Calling the `triplets()` method on our graph returns the triplets:

// COMMAND ----------

clickstreamGRAPH.triplets.take(20)

// COMMAND ----------

// MAGIC %md But that's hard to read. Let's format it better:

// COMMAND ----------

for (t <- clickstreamGRAPH.triplets.take(20)) {
  println(s""""${t.srcAttr}" refers to "${t.dstAttr}" ${t.attr} times""") 
}

// COMMAND ----------

// MAGIC %md #### Question 4: Which articles have the highest number of other articles referring to them? (in-degree)

// COMMAND ----------

// MAGIC %md Just as RDDs have basic operations like map, filter, and reduceByKey, property graphs also have a collection of basic operators that take user defined functions (UDFs) and produce new graphs with transformed properties and structure. 

// COMMAND ----------

// MAGIC %md The in-degree of a vertex v is the number of edges with v as their terminal vertex. So, for example, if the Amsterdam article is referred to by 100 other articles, then the Amsterdam vertex's in-degree is 100.
// MAGIC 
// MAGIC Note that `n`, the label/weight of the edge, is not taken into account when calculating the in-degree.

// COMMAND ----------

val inDegreesRDD = clickstreamGRAPH.inDegrees

inDegreesRDD.take(2)

// COMMAND ----------

inDegreesRDD

// COMMAND ----------

// MAGIC %md Hmm, the `inDegrees()` method has returned a new [VertexRDD](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.graphx.VertexRDD) with article ID and its respective in-count.

// COMMAND ----------

// MAGIC %md But which articles have the highest in-count? Meaning, which `(articleID, in-degrees)` pairs have the highest value for `in-degrees`?

// COMMAND ----------

val top5indegreesARRAY = inDegreesRDD.map(_.swap).top(5).map(_.swap)

// COMMAND ----------

// MAGIC %md Looks like article # 15580374 has the highest in-count. But what is that article's title?

// COMMAND ----------

// MAGIC %md Remember that we still have the original data source with the artile id + title available:

// COMMAND ----------

uniqueArticlesDF.show(5)

// COMMAND ----------

// MAGIC %md Maybe we can join the `uniqueArticlesDF` DataFrame above to `top5indegreesARRAY` to get `(article_title, in-degrees)` pairs?

// COMMAND ----------

// MAGIC %md First, convert the `uniqueArticlesDF` DataFrame to an RDD:

// COMMAND ----------

uniqueArticlesDF.rdd.take(3)

// COMMAND ----------

val idAndArticleRDD = uniqueArticlesDF.rdd.map { row =>
  // Map to a (Int, String) tuple type
  (row(0).asInstanceOf[Int], row(1).asInstanceOf[String])
}

// COMMAND ----------

idAndArticleRDD.take(3)

// COMMAND ----------

// MAGIC %md Next, create an RDD from the `top5indegreesARRAY` Scala array:

// COMMAND ----------

top5indegreesARRAY

// COMMAND ----------

val top5indegreesRDD = sc.parallelize(top5indegreesARRAY.map(x => (x._1.toInt,x._2)))

// COMMAND ----------

top5indegreesRDD.collect()

// COMMAND ----------

// MAGIC %md Join the 2 RDDs:

// COMMAND ----------

val joinedRDD = idAndArticleRDD.join(top5indegreesRDD)

// COMMAND ----------

println(joinedRDD.collect().sortBy { case (id, (title, indeg)) => -indeg}.deep.mkString("\n"))

// COMMAND ----------

// MAGIC %md We can see that the `Main_page` article had the highest in-count of 88,409 and the `United_States` article had the second highest in-count of 4,292, followed by the `Internet_Movie_Database`, `United_Kingdom`, and `India`.

// COMMAND ----------

// MAGIC %md #### Question 5: What are the most influential articles in the Feb 2015 clickstream? (PageRank)

// COMMAND ----------

// MAGIC %md [PageRank](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.graphx.lib.PageRank$) measures the importance of each vertex in a graph, assuming an edge from u to v represents an endorsement of v?s importance by u. For example, if a Wikipedia article is referred to by many other articles, the article will be ranked highly.
// MAGIC 
// MAGIC PageRage is different from in-degrees though because in PageRank the importance of a referrer is taken into account. So if the Main_Page refers to an article, that is considered as a stronger endorsement than if some other minor page refers to the same article.

// COMMAND ----------

// MAGIC %md Send our clickstreamGRAPH to the `PageRank.run()` method and request for 3 iterations:

// COMMAND ----------

// Note the calculation below takes thousands of tasks.. so please be patient! Run time should be 2-3 mins.
val prResultsGRAPH = PageRank.run(clickstreamGRAPH, 3)

// COMMAND ----------

// MAGIC %md The prResultsGRAPH contains `(article ID, pagerank results)`:

// COMMAND ----------

prResultsGRAPH.vertices.take(3)

// COMMAND ----------

// MAGIC %md Our results would be easier to interpret if we joined the `prResultsGRAPH` with the `idAndArticleRDD` so that we can easily see the article title and its respective PageRank.
// MAGIC 
// MAGIC First yank out the top ten highly ranked articles from the Page Rank results graph:

// COMMAND ----------

 val top10RanksARRAY = prResultsGRAPH.vertices.map(_.swap).top(10).map(_.swap)

// COMMAND ----------

// MAGIC %md Convert the Array to an RDD:

// COMMAND ----------

val top10RanksRDD = sc.parallelize(top10RanksARRAY.map(x => (x._1.toInt,x._2)))

// COMMAND ----------

// MAGIC %md Finally, join the top10RanksRDD with the idAndArticleRDD:

// COMMAND ----------

val prJoinedRDD = idAndArticleRDD.join(top10RanksRDD)

// COMMAND ----------

// MAGIC %md Let's see the results now:

// COMMAND ----------

println(prJoinedRDD.sortBy { case (id, (title, pageRank)) => -pageRank}.collect().deep.mkString("\n"))

// COMMAND ----------

// MAGIC %md We can see that the `Main_Page` article has the highest page rank with 1778 and the `Internet_Movie_Database` has the 2nd highest page rank.
// MAGIC 
// MAGIC These highly influencial articles tend to be in a locked/protected status on Wikipedia to prevent vandalism and biased edits.

// COMMAND ----------

// MAGIC %md #### Question 6: What does the graph of connections between the top 7 most populated cities on earth look like?

// COMMAND ----------

// MAGIC %md This a Wikipedia article lists the top cities by population:
// MAGIC https://en.wikipedia.org/wiki/List_of_cities_proper_by_population

// COMMAND ----------

// MAGIC %md Yank out a subgraph of the 7 top cities:

// COMMAND ----------

val citiesSubGRAPH = clickstreamGRAPH.subgraph(vpred = (id, attr) => Set("Shanghai", "Karachi", "Lagos", "Delhi", "Istanbul", "Tokyo", "Mumbai") contains attr)

// COMMAND ----------

// MAGIC %md Verify that our new subgraph has 7 vertices (one for each city):

// COMMAND ----------

citiesSubGRAPH.vertices.count()

// COMMAND ----------

// MAGIC %md How may connections are there between the 7 cities:

// COMMAND ----------

citiesSubGRAPH.edges.count()

// COMMAND ----------

// MAGIC %md Print out the triplets view of the subgraph:

// COMMAND ----------

for (t <- citiesSubGRAPH.triplets.takeOrdered(30)(Ordering.by(_.srcAttr)).sortBy(t => (t.srcAttr, -1 * t.attr))) {
  println(s""""${t.srcAttr}" refers to "${t.dstAttr}" ${t.attr} times""") 
}

// COMMAND ----------

// MAGIC %md #### Question 7: In the cities subgraph, what is the shortest path between the Shanghai and Mumbai articles? (shortest path)

// COMMAND ----------

// MAGIC %md The shortest path library in GraphX computes shortest paths to the given set of landmark vertices, returning a graph where each vertex attribute is a map containing the shortest-path distance to each reachable landmark.

// COMMAND ----------

// MAGIC %md Remember the `uniqueArticlesDF` DataFrame?

// COMMAND ----------

uniqueArticlesDF.show(5)

// COMMAND ----------

// MAGIC %md Let's get the id of the Mumbai article:

// COMMAND ----------

uniqueArticlesDF.filter($"curr_title" === "Mumbai").show()

// COMMAND ----------

// MAGIC %md We'll use Mumbai's ID to calculate the shortest path to Mumbai:

// COMMAND ----------

val shortestPathGRAPH = ShortestPaths.run(citiesSubGRAPH, Seq(19189L))

// COMMAND ----------

shortestPathGRAPH.vertices.take(20)

// COMMAND ----------

// sameer to-do: Need to do a join to pretty print the article names.

// COMMAND ----------

// MAGIC %md # sameer to-do: I left off here before leaving for AMS flight.

// COMMAND ----------

// MAGIC %md #### Question 8: Can you create a visualization of the shortest path between 2 articles?

// COMMAND ----------

// MAGIC %md It is possible to embed SVG images directly into Databricks Notebooks:

// COMMAND ----------

displayHTML("""<svg width="100" height="100">
   <circle cx="50" cy="50" r="30" stroke="Maroon" stroke-width="4" fill="Orange" />
   Sorry, your browser does not support inline SVG.
</svg>""")

// COMMAND ----------

val json = """
{"nodes":[
  {"currency":"L.A.","id":0,"size":20},
  {"currency":"Chicago","id":1,"size":20},
  {"currency":"Tokyo","id":2,"size":20},
  {"currency":"Amsterdam","id":3,"size":20},
  {"currency":"London","id":4,"size":20},
  {"currency":"NYC","id":5,"size":20},
  {"currency":"Paris","id":6,"size":20}
 ],
"links":[
  {"source":0,"target":0,"direction":"EdgeDirection.Both"},{"source":1,"target":0,"direction":"EdgeDirection.Both"},{"source":2,"target":0,"direction":"EdgeDirection.Both"},{"source":3,"target":0,"direction":"EdgeDirection.Both"},{"source":4,"target":0,"direction":"EdgeDirection.Both"},{"source":5,"target":0,"direction":"EdgeDirection.Both"},
  {"source":6,"target":0,"direction":"EdgeDirection.Both"}
]}"""

// COMMAND ----------

// Here is the HTML code to generate our D3 visualization

def d3GraphViz(json : String) : String = {
  """<!DOCTYPE html>
<meta charset="utf-8">
<style>

.link line {
  stroke: #696969;
}

.link line.separator {
  stroke: #fff;
  stroke-width: 2px;
}

.node circle {
  stroke: #111;
  stroke-width: 1.5px;
}

.node text {
  font: 10px sans-serif;
  pointer-events: none;
}

</style>
<body>
<script src="lib/d3.min.js"></script>
<script>

var graph =   """ + json + """;
var width = 960,
    height = 500;

var color = d3.scale.category20();

var radius = d3.scale.sqrt()
    .range([0, 6]);

var svg = d3.select("body").append("svg")
    .attr("width", width)
    .attr("height", height);

var force = d3.layout.force()
    .size([width, height])
    .charge(-400)
    .linkDistance(function(d) { return radius(d.source.size) + radius(d.target.size) + 20; });


  force
      .nodes(graph.nodes)
      .links(graph.links)
      .on("tick", tick)
      .start();

  var link = svg.selectAll(".link")
      .data(graph.links)
    .enter().append("g")
      .attr("class", "link");

  link.append("line")
      .style("stroke-width", function(d) { return (1) * 2 + "px"; });

  link.filter(function(d) { return 0 > 1; }).append("line")
      .attr("class", "separator");

  var node = svg.selectAll(".node")
      .data(graph.nodes)
    .enter().append("g")
      .attr("class", "node")
      .call(force.drag);

  node.append("circle")
      .attr("r", function(d) { return radius(d.size); })
      .style("fill", function(d) { return color(d.currency); });

  node.append("text")
      .attr("dy", ".35em")
      .attr("text-anchor", "middle")
      .text(function(d) { return d.currency; });

  function tick() {
    link.selectAll("line")
        .attr("x1", function(d) { return d.source.x; })
        .attr("y1", function(d) { return d.source.y; })
        .attr("x2", function(d) { return d.target.x; })
        .attr("y2", function(d) { return d.target.y; });

    node.attr("transform", function(d) { return "translate(" + d.x + "," + d.y + ")"; });
}
</script>
"""
}

// COMMAND ----------

displayHTML(d3GraphViz(json))

// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

// MAGIC %md See D3 Graph Viz here: https://trainers.cloud.databricks.com/#notebook/54880
