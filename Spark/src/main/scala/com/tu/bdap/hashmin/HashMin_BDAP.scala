package com.tu.bdap.hashmin

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

import com.tu.bdap.utils.DataSetLoader


/**
  * This algorithm computes connected components by propagating the lowest vertex-id to all neighbours
  */
object HashMin_BDAP {
  /**
    * Loads a graph from local disk or hdfs and calculates CC using HashMin
    * @param args args[0] should contain path, args[1] is an integer identifying the dataset
    */
  def main(args: Array[String]): Unit = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("HashMin")
    //.setMaster("local")

    val sc = new SparkContext(conf)

    //Set NumIterations
    val numIterations = 10

    //Check input arguments
    if (args.length < 2) {
      System.err.println("Invalid input arguments")
      return
    }

    //Set input Arguments
    val dataSetPath = args(0)
    val dataSet = args(1).toInt

    //Load DataSet
    var edges = dataSet match {
      case 1 => DataSetLoader.loadUSA(sc, dataSetPath)
      case 2 => DataSetLoader.loadTwitter(sc, dataSetPath)
      case 3 => DataSetLoader.loadFriendster(sc, dataSetPath)
      case _ => null
    }

    //Make the Graph undirected
    edges = edges.flatMap(edge => Seq(Edge(edge.srcId, edge.dstId, 0), Edge(edge.dstId, edge.srcId, 0)))

    //Check if DataSet could be loaded
    if (edges == null) {
      System.err.println("Could not load DataSet")
      return
    }

    //Create Graph from edges
    var graph = Graph.fromEdges(edges, 0.0)

    graph = graph.mapVertices((id, _) => id.toDouble)


    //1. Superstep: Send vertex-id to all neighbours
    //2. Superstep: If smallest received vertex-id is smaller then current one, update it and send to all neighbours again
    graph = graph.pregel(Double.PositiveInfinity, numIterations)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
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