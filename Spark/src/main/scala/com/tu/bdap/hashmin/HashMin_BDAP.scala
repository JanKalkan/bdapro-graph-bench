package com.tu.bdap.hashmin

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.EdgeDirection

object HashMin_BDAP {
  def main(args: Array[String]): Unit = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("HashMin")
      .setMaster("local")

    val sc = new SparkContext(conf)

//     USA Dataset
        val file = sc.textFile(args(0))
          .filter { x => x.startsWith("a") }
          .map { line =>
            val fields = line.split(" ")
            Edge(fields(1).toInt, fields(2).toInt, fields(3).toInt)
          }.flatMap(edge => Seq(Edge(edge.srcId,edge.dstId,0),Edge(edge.dstId,edge.srcId,0)))

    // Twitter Dataset
//    val file = sc.textFile(args(0))
//      .filter { x => Character.isDigit(x.charAt(0)) }
//      .map { line =>
//        val fields = line.split(" ")
//        Edge(fields(0).toInt, fields(1).toInt, 1)
//      }.flatMap(edge => Seq(Edge(edge.srcId, edge.dstId, 0), Edge(edge.dstId, edge.srcId, 0)))

    var graph = Graph.fromEdges(file, 1.toInt)

    graph = graph.mapVertices((id, _) => id.toInt)

    graph = graph.pregel(Double.PositiveInfinity, 20)(
      (id, dist, newDist) => math.min(dist, newDist.toInt), // Vertex Program
      triplet => { // Send Message
        if (triplet.srcAttr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
      )

    graph.vertices.collect()

  }
}