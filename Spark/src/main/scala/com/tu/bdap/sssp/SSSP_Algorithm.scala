package com.tu.bdap.sssp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.EdgeRDD
import org.apache.spark.graphx.lib.ShortestPaths

object SSSP_Algorithm {
  def main(args: Array[String]): Unit = {

    val t1 = System.nanoTime

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("SSSP")

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

    val graph = Graph.fromEdges(file, "")

    val sourceId = 1

    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)

    val sssp = initialGraph.pregel(Double.PositiveInfinity, 20)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => { // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
      )

    // Print the result
    sssp.vertices.collect

    val duration = (System.nanoTime - t1) / 1e9d

    print("Time:" + duration)

  }
}