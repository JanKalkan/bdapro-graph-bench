package com.tu.bdap.gc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.graphx._
import Math._

import scala.util.Random

/**
  * Created by simon on 21.08.17..
  */
object GC {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("GC")

    val sc = new SparkContext(conf)

    // USA Dataset
    var edges = sc.textFile(args(0))
      .filter { x => x.startsWith("a") }
      .map { line =>
        val fields = line.split(" ")
        Edge(fields(1).toLong, fields(2).toLong, 0)
      }
    edges = edges.flatMap(edge => Seq(Edge(edge.srcId,edge.dstId,0),Edge(edge.dstId,edge.srcId,0)))
    
    // Twitter Dataset
//    var edges = sc.textFile(args(0))
//      .filter { x => Character.isDigit(x.charAt(0)) }
//      .map { line =>
//        val fields = line.split(" ")
//        Edge(fields(0).toLong, fields(1).toLong, 0)
//      }
//    edges = edges.flatMap(edge => Seq(Edge(edge.srcId,edge.dstId,0),Edge(edge.dstId,edge.srcId,0)))

    //Graph.fromEdgeTuples(file,0L)
    var graph = Graph.fromEdges(edges, 0)

    val degrees = graph.outDegrees
    var init = graph.outerJoinVertices(degrees) { (_, _, optDegree) =>
      optDegree.getOrElse(1)
    }

    val gc = init.mapVertices((id,value) => {
      (-1,-1,value)
    })


    val result=gc.pregel[(VertexId,Int)](initialMsg ,20,EdgeDirection.Out)(compute,sendMsg,mergeMsg)
   result.vertices.collect

  }
  val initialMsg = (Long.MaxValue,0)

  def compute(id: VertexId, value: (Int,Int,Int), message: (Long,Int)): (Int,Int,Int) = {


    val r = Random
    val neighbours = value._3 - message._2
    if(value._2==1){
      return (value._1,value._2,neighbours)
    }
    // resolve conflict of marked nodes, select the one with smaller id
    if(value._2==0){
      if(message._1 > id){
        return (value._1+1,1,neighbours)
      }
    }
    if(neighbours == 0){
      return (value._1+1,1,neighbours)
    }
    if( r.nextFloat() <= 1./(2*neighbours)){
      return (value._1+1,0,neighbours)
    }
     return (value._1+1,-1,neighbours)
  }

  def sendMsg(triplet: EdgeTriplet[(Int,Int,Int), Int]):Iterator[(Long,(Long,Int))] = {
    if(triplet.dstAttr._2 == 1){
      return Iterator.empty
    }
    if (triplet.srcAttr._2 == 0) {
      return Iterator((triplet.dstId, (triplet.srcId, 0)))
    }
    if (triplet.srcAttr._2 == 1) {
      return Iterator((triplet.dstId, (Long.MaxValue, 1)))
    }
    if (triplet.srcAttr._2 == -1){
      return Iterator((triplet.dstId, (Long.MaxValue, 0)))
    }
    Iterator.empty
  }

    def mergeMsg(msg1:(Long,Int) , msg2:(Long,Int)):(Long,Int) = {
      (min(msg1._1,msg2._1),msg1._2 + msg2._2)
    }


}

