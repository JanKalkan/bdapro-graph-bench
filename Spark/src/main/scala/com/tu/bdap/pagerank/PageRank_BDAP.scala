package com.tu.bdap.pagerank

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.PageRank

import com.tu.bdap.utils.DataSetLoader


/**
  * This algorithm computes the relevance of every node in a graph <br>
  * by using the PageRank formula
  */
object PageRank_BDAP {
  /**
    * Loads a graph from local disk or hdfs and calculates PageRank value for each vertex
    * @param args args[0] should contain path, args[1] is an integer identifying the dataset
    */
  def main(args: Array[String]): Unit = {


    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("PageRank")
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
    val graph = Graph.fromEdges(edges, "")

    //Run PageRank provided by GraphX
    PageRank.run(graph, numIterations, 0.15).vertices.collect()
  }
}