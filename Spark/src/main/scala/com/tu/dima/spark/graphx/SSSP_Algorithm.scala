package com.tu.dima.spark.graphx

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

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("SSSP")
      .setMaster("local")

    val sc = new SparkContext(conf)

    // Load the graph
    val file = sc.textFile(args(0))
      .filter { x => x.startsWith("a") }
      .map { line =>
        val fields = line.split(" ")
        Edge(fields(1).toLong, fields(2).toLong, fields(3).toLong)
      }

    val graph = Graph.fromEdges(file, "")

    val sourceId = 1

    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)

    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
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
    println(sssp.vertices.collect.mkString("\n"))
    //println(sssp.vertices.collect().size)

  }
}