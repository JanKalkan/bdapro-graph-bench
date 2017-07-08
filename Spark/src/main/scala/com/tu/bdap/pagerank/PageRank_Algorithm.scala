package com.tu.bdap.pagerank

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.lib.PageRank

object PageRank_Algorithm {
  def main(args: Array[String]) = {

    val Iterations = 20;
    val Dampening  = 0.15;

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("PageRank")

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
    val result = PageRank.run(graph, Iterations, Dampening)

    // Print the result
    println(result.vertices.collect().mkString("\n"))
  }
}