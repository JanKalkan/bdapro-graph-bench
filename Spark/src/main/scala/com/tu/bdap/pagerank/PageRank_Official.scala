package com.tu.bdap.pagerank

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.graphx.TripletFields
import org.apache.spark.storage.StorageLevel

object PageRank_Official {
  def main(args: Array[String]): Unit = {

    val Iterations = 1;

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("PageRank_Offical")

    val sc = new SparkContext(conf)

    // USA Dataset
//        val file = sc.textFile(args(0))
//          .filter { x => x.startsWith("a") }
//          .map { line =>
//            val fields = line.split(" ")
//            Edge(fields(1).toLong, fields(2).toLong, fields(3).toLong)
//          }

    // Twitter Dataset
    val file = sc.textFile(args(0))
      .filter { x => Character.isDigit(x.charAt(0)) }
      .map { line =>
        val fields = line.split(" ")
        Edge(fields(0).toLong, fields(1).toLong, 1)
      }

    val graph = Graph.fromEdges(file, "defaultProperty")

    PageRank.run(graph, 20, 0.15).vertices.collect()
    // Print the result
    // rankGraph.vertices.collect()
    // println(rankGraph.vertices.collect().find({ case (vid, d) => vid ==167995}))
  }
}