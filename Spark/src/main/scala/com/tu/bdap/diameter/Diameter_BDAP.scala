package com.tu.bdap.diameter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import scala.collection.immutable.StringOps
import Math._

import scala.util.Random
import com.tu.bdap.utils.DataSetLoader

/** A Diameter describes the longest possible path between two vertices,<br>
  * assuming one takes always the shortest path. <br>
  * The standard implementation would store all reachable vertices<br>
  * in each superstep and send the set to all neighbour and thereby
  * increase these sets.
  * Since exact calculation is very memory-expensive, <br>
  * probabilistic counting is used to estimate those sets <br>
  * @see <a href="http://www.cs.cmu.edu/~christos/PUBLICATIONS/kdd02-anf.pdf">http://www.cs.cmu.edu/~christos/PUBLICATIONS/kdd02-anf.pdf</a>
  */
object Diameter_BDAP {
  /**
    * Loads a graph from local disk or hdfs and estimates the Diameter
    * @param args args[0] should contain path, args[1] is an integer identifying the dataset
    */
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
      .setAppName("Diameter")
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
    
    //Check if DataSet could be loaded
    if (edges == null) {
      System.err.println("Could not load DataSet")
      return
    }
    var graph = Graph.fromEdges(edges, 0L)

    var count = graph.vertices.count()
    val k = 32

    // create random samples (bitstrings)
    val g = graph.mapVertices((id, value) => {
      val r = Random
      var sample1 = 0L
      var sample2 = 0L
      var sample3 = 0L
      for (i <- 0 to k) {
        if (r.nextFloat() <= pow(2, -(i + 1))) sample1 |= pow(2, i).toLong
        if (r.nextFloat() <= pow(2, -(i + 1))) sample2 |= pow(2, i).toLong
        if (r.nextFloat() <= pow(2, -(i + 1))) sample3 |= pow(2, i).toLong
      }
      Tuple4(sample1, sample2, sample3, 0L)
    })

    //execute pregel API
    val result = g.pregel(initialMsg,
      numIterations,
      EdgeDirection.Out)(
        vprog,
        sendMsg,
        mergeMsg) 
    result.vertices.collect

  }

  val initialMsg = (0L, 0L, 0L, 0L)

  /**
    * Merge current estimated set with received sets by doing a logical OR <br>
    * If estimated set did not increase by a certain threshold, <br>
    * stop calculation (set path length to negative).
    * @param id vertex ID
    * @param value current vertex value: contains three estimates(samples) and path length (the absolute value describes the longest path,
    *              <br> and a negative sign means the vertex votes to halt)
    * @param message incoming messages
    * @return updated vertex value
    */
  def vprog(id: VertexId, value: (Long, Long, Long, Long), message: (Long, Long, Long, Long)): (Long, Long, Long, Long) = {
    val e = 0.10

    // merge sets by logical OR
    val v1 = value._1 | message._1
    val v2 = value._2 | message._2
    val v3 = value._3 | message._3
    val iteration = max(message._4, 0L) + 1L

    //estimate size of old and current set
    var oldBit = (lowestZero(value._1) + lowestZero(value._2) + lowestZero(value._3)) / 3
    var newBit = (lowestZero(v1) + lowestZero(v2) + lowestZero(v3)) / 3
    val oldN = pow(2, oldBit) / 0.77351
    val newN = pow(2, oldBit) / 0.77351

    // vote for halt, if threshold is not reached
    if (newN <= (1 + e) * oldN & iteration > 1) {
      return (v1, v2, v3, -iteration)
    }
    (v1, v2, v3, iteration)
  }

  /** If vertex voted to halt, don't create a message, otherwise send current value to neighbour
    *
    * @param triplet triplets describing edge and adjacent values
    * @return message to neighbours
    */
  def sendMsg(triplet: EdgeTriplet[(Long, Long, Long, Long), Long]): Iterator[(Long, (Long, Long, Long, Long))] = {
    val sourceVertex = triplet.srcAttr
    if (sourceVertex._4 < 0) return Iterator.empty
    Iterator((triplet.dstId, sourceVertex))
  }

  /** An associative and commutative function, that combines messages. <br>
    * Merge all incoming messages by doing a logical OR of the estimated sets<br>
    * and choosing the largest path length
    * @param msg1 1st message
    * @param msg2 2nd message
    * @return combined message
    */
  def mergeMsg(msg1: (Long, Long, Long, Long), msg2: (Long, Long, Long, Long)): (Long, Long, Long, Long) = {
    val v1 = msg1._1 | msg2._1
    val v2 = msg1._2 | msg2._2
    val v3 = msg1._3 | msg2._3
    val iteration = max(msg1._4, msg2._4)
    (v1, v2, v3, iteration)
  }

  /** Calculate the position of the zero in a bitstring with the lowest index <br>
    * Examples: 111110 -> 0, 101101 -> 1, 011111-> 5
    * @param bits a bitstring encoded as a Long
    * @return zero with lowest index
    */
  def lowestZero(bits: Long): Double = {
    val zero = bits | (bits + 1)

    var difference = bits ^ zero
    var leadingZeros = 64;
    while (difference > 0) {
      difference = difference >> 1
      leadingZeros = leadingZeros - 1
    }
    val index = 64 - leadingZeros
    return index
  }
}