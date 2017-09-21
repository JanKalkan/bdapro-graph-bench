package com.tu.bdap.bmm

import java.lang.Math.min



import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import com.tu.bdap.utils.DataSetLoader

/**
 * Created by simon on 8/26/17.
 */
object BMM_BDAP {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("BMM")
      //.setMaster("local")

     val sc = new SparkContext(conf)

    //Set NumIterations
    val numIterations = 20

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
      case 4 => DataSetLoader.loadLivejournal(sc, dataSetPath)
      case _ => null
    }
    
    //Check if DataSet could be loaded
    if (edges == null) {
      System.err.println("Could not load DataSet")
      return
    }
      
    var graph = Graph.fromEdges(edges, (0L, 0))
    val result = graph.pregel[Long](0L, numIterations, EdgeDirection.Out)(compute, sendMsg, mergeMsg)
    result.vertices.collect()
  }

  def compute(id: VertexId, value: (Long, Int), message: Long): (Long, Int) = {
    val superstep = value._2 % 4

    if (superstep == 0 & value._1 != 0L) return value

    // left node, 0th Superstep

    // right node, 1st Superstep
    if (superstep == 1 & id < 0) {
      return (message, value._2 + 1)
    }

    // left node 2nd Superstep
    if (superstep == 2 & id > 0) {
      return (message, value._2 + 1)
    }

    // right node 3rd
    if (superstep == 3 & id < 0) {
      if (message == value._1 & message != 0) {
        return (message, value._2 + 1)
      } else return (0L, value._2 + 1)
    }
    return (value._1, value._2 + 1)
  }

  def sendMsg(triplet: EdgeTriplet[(Long, Int), Long]): Iterator[(VertexId, Long)] = {

    if (triplet.srcAttr._2 % 4 == 1 & triplet.srcAttr._1 == 0L & triplet.dstAttr._1 == 0L) {
      return Iterator((triplet.srcId, 0L), (triplet.dstId, triplet.srcId))
    }

    if (triplet.srcAttr._2 % 4 == 2) {
      if (triplet.dstAttr._1 == triplet.srcId & triplet.srcAttr._1 == 0L) {
        return Iterator((triplet.srcId, triplet.dstId), (triplet.dstId, 0L))
      } else {
        return Iterator((triplet.srcId, 0L), (triplet.srcId, 0L))
      }
    }

    if (triplet.srcAttr._2 % 4 == 3) {
      if (triplet.srcAttr._1 == triplet.dstId & triplet.srcAttr._1 != 0L) {
        return Iterator((triplet.dstId, triplet.srcId), (triplet.srcId, 0L))
      } else {
        return Iterator((triplet.dstId, Long.MaxValue), (triplet.srcId, 0))
      }
    }
    if (triplet.srcAttr._2 % 4 == 0) {
      if (triplet.srcAttr._1 != 0L) {
        return Iterator.empty
      }
      return Iterator((triplet.dstId, 0), (triplet.srcId, 0))
    }

    Iterator.empty
  }

  def mergeMsg(msg1: Long, msg2: Long): Long = {
    min(msg1, msg2)
  }

}



