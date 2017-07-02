package com.tu.dima

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.lib.PageRank

object PageRankJob {
   def main(args: Array[String]) = {

      //Start the Spark context
    val conf = new SparkConf()
      .setAppName("PageRank")

     val sc = new SparkContext(conf)

    val file = sc.textFile(args(0))
      .filter { x => x.startsWith("a") }
      .map { line =>
        val fields = line.split(" ")
        Edge(fields(1).toLong, fields(2).toLong, 1)
      }

    val graph = Graph.fromEdges(file, "defaultProperty")

     val ranks = PageRank.run(graph, 100, 0.15).vertices;
   
    // Print the result
    println(ranks.collect().mkString("\n"))
  }
}