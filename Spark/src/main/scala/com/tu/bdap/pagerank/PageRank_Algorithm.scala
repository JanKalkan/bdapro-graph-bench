package com.tu.bdap.pagerank

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.graphx.TripletFields
import org.apache.spark.storage.StorageLevel

object PageRank_Algorithm {
  def main(args: Array[String]) = {

    val Iterations = 1;


    //Start the Spark context
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("PageRank")

    val sc = new SparkContext(conf)

    // Load the graph
    val file = sc.textFile("/home/johannes/Downloads/test.txt")
      .filter { x => x.startsWith("a") }
      .map { line =>
        val fields = line.split(" ")
        Edge(fields(1).toLong, fields(2).toLong, fields(3).toLong)
      }

    val graph = Graph.fromEdges(file, "defaultProperty", StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK)

    
    var rankGraph = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) { (vid, vdata, deg) => deg.getOrElse(0) }
      // Set the weight on the edges based on the degree
      .mapTriplets(e => 1.0 / e.srcAttr, TripletFields.Src)
      // Set the vertex attributes to the initial pagerank values
      .mapVertices { (id, attr) => 1.0 }
      

    var iteration = 0
    while (iteration < 1) {

      val rankUpdates = rankGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr), _ + _, TripletFields.Src)

      rankGraph = rankGraph.outerJoinVertices(rankUpdates) {
        (id, oldRank, msgSumOpt) => 0.15 + 0.85 * msgSumOpt.getOrElse(0.0)
      }

      iteration += 1
    }

    // Print the result
    println(rankGraph.vertices.collect().mkString("\n"))
  // println(rankGraph.vertices.collect().find({ case (vid, d) => vid ==167995}))
  }
}