package com.tu.bdap.diameter

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import scala.collection.immutable.StringOps
import Math._

import scala.util.Random
/**
 * Created by simon on 11.07.17..
 */
object Diameter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Diameter")
      .setMaster("local")

    val sc = new SparkContext(conf)

//     USA Dataset
        val file = sc.textFile(args(0))
          .filter { x => x.startsWith("a") }
          .map { line =>
            val fields = line.split(" ")
            Edge(fields(1).toLong, fields(2).toLong, 0L)
          }

    //Twitter Dataset
//    val file = sc.textFile(args(0))
//      .filter { x => Character.isDigit(x.charAt(0)) }
//      .map { line =>
//        val fields = line.split(" ")
//        Edge(fields(0).toLong, fields(1).toLong, 0L)
//      }

    //Graph.fromEdgeTuples(file,0L)
    var graph = Graph.fromEdges(file, 0L)

    var count = graph.vertices.count()
    val k = 32

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

    val result = g.pregel(initialMsg,
      20,
      EdgeDirection.Out)(
        vprog,
        sendMsg,
        mergeMsg)
    println(result.vertices.collect.mkString("\n"))


  }

  val initialMsg = (0L, 0L, 0L, 0L)
  def vprog(id: VertexId, value: (Long, Long, Long, Long), message: (Long, Long, Long, Long)): (Long, Long, Long, Long) = {
    val e = 0.05

    val v1 = value._1 | message._1
    val v2 = value._2 | message._2
    val v3 = value._3 | message._3
    val iteration = max(message._4, 0L) + 1L

    var oldBit = (lowestZero(value._1) + lowestZero(value._2) + lowestZero(value._3)) / 3
    var newBit = (lowestZero(v1) + lowestZero(v2) + lowestZero(v3)) / 3

    val oldN = pow(2, oldBit) / 0.77351
    val newN = pow(2, oldBit) / 0.77351

    if(newN <= (1+e)*oldN & iteration > 1){
      return (v1, v2, v3, -iteration)
    }
    (v1, v2, v3, iteration)
  }

  def sendMsg(triplet: EdgeTriplet[(Long, Long, Long, Long), Long]): Iterator[(Long, (Long, Long, Long, Long))] = {
    val sourceVertex = triplet.srcAttr
    if(sourceVertex._4 < 0) return Iterator.empty
    Iterator((triplet.dstId, sourceVertex))
  }

  def mergeMsg(msg1: (Long, Long, Long, Long), msg2: (Long, Long, Long, Long)): (Long, Long, Long, Long) = {
    val v1 = msg1._1 | msg2._1
    val v2 = msg1._2 | msg2._2
    val v3 = msg1._3 | msg2._3
    val iteration = max(msg1._4, msg2._4)
    (v1, v2, v3, iteration)
  }

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
