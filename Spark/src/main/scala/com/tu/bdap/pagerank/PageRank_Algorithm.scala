package com.tu.bdap.pagerank

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.lib.PageRank

object PageRank_Algorithm {
  def main(args: Array[String]) = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("PageRank")
      .setMaster("local")

    val sc = new SparkContext(conf)

    // Load the graph
    val file = sc.textFile(args(0))
      .filter { x => x.startsWith("a") }
      .map { line =>
        val fields = line.split(" ")
        Edge(fields(1).toLong, fields(2).toLong, fields(3).toLong)
      }

    val graph = Graph.fromEdges(file, "defaultProperty")

    // Run PageRank
  // val result = PageRank.run(graph, Integer.MAX_VALUE, 0.001)
  val result = PageRank.runUntilConvergence(graph, 0.001)
   

    // Print the result
    println(result.vertices.collect().mkString("\n"))
  }
}