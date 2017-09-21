package com.tu.bdap.sssp

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

import com.tu.bdap.utils.DataSetLoader

//This algorithm computes the shortest path to all other nodes from one source vertex
object SSSP_BDAP {
  def main(args: Array[String]): Unit = {

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("SSSP")
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
    val edges = dataSet match {
      case 1 => DataSetLoader.loadUSA(sc, dataSetPath)
      case 2 => DataSetLoader.loadTwitter(sc, dataSetPath)
      case 3 => DataSetLoader.loadFriendster(sc, dataSetPath)
      case _ => null
    }
    //Check if DataSet could be loaded
    if (edges == null) {
      System.err.println("Could not load DataSet")
      return
    }

    //Create Graph from edges
    val graph = Graph.fromEdges(edges, null)

    //Source vertex
    val sourceId = 101

    //Set PositiveInfinity as distance to all nodes
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)

    //Send min distance to all other nodes
    val sssp = initialGraph.pregel(Double.PositiveInfinity, numIterations)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => { // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      //Only keep the shortest distance
      (a, b) => math.min(a, b) // Merge Message
      )

    // Print the result
    sssp.vertices.collect

  }
}