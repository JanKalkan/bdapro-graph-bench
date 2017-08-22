package com.tu.bdap.pagerank

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.TripletFields

object PageRank_BDAP {
  def main(args: Array[String]) = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("PageRank")
      .setMaster("local")
      .set("spark.driver.memory", "1g")
      .set("spark.driver.memory", "1g")

    val sc = new SparkContext(conf)

    // Load the graph
    val file = sc.textFile("C:/Users/Johannes/Desktop/usa.txt")
      .filter { x => x.startsWith("a") }
      .map { line =>
        val fields = line.split(" ")
        Edge(fields(1).toLong, fields(2).toLong, fields(3).toLong)
      }

    val graph = Graph.fromEdges(file, 1)

    var rankGraph: Graph[Double, Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)
      // Set the vertex attributes to the initial pagerank values
      .mapVertices { (id, attr) => 1.0
      }

    var iteration = 0
    while (iteration < 1) {
      rankGraph.cache()
      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

      rankGraph = rankGraph.joinVertices(rankUpdates)((vid, d, u) => d + u)
        .mapVertices((vid, d) => 0.15 + 0.85 * d)

      iteration += 1
    }

    println("Items: " + rankGraph.vertices.collect().size)
    // for (name <- rankGraph.vertices.collect()) println(name)

  }
}