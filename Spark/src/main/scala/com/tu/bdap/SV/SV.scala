package com.tu.bdap.SV

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.SparkConf
import org.apache.spark.graphx.VertexRDD
import org.apache.spark.graphx.EdgeRDD

/**
  * Created by jan on 26.08.17.
  */
object SV {
  
  val DATASET_USA     = 1
  val DATASET_TWITTER = 2
  
  /**
   * Main
   */
  def main(args: Array[String]) = {
    
    
    var graph = loadGraph(args(0), args(1).toInt)
     
    
    
     
  }
  
  /**
   *  Load the graph
   */
  def loadGraph(path: String, datasetID: Int) : Graph[(Long, Boolean), Int] = {
    
     //Start the Spark context
    val conf = new SparkConf()
      .setAppName("HashMin")
      .setMaster("local")

    val sc = new SparkContext(conf)

    
    val file = datasetID match {
      
        case DATASET_USA => 
          // Load the graph
          sc.textFile("/home/johannes/Downloads/test.txt")
            .filter { x => x.startsWith("a") }
            .map { line =>
              val fields = line.split(" ")
              Edge(fields(1).toLong, fields(2).toLong, 0)
            }.flatMap(edge => Seq(Edge(edge.srcId,edge.dstId,0),Edge(edge.dstId,edge.srcId,0)))
        
        case DATASET_TWITTER =>
          // Load the graph
          sc.textFile("/home/johannes/Downloads/test.txt")
            .map { line =>
              val fields = line.split(" ")
              Edge(fields(1).toLong, fields(2).toLong, 0)
            }.flatMap(edge => Seq(Edge(edge.srcId,edge.dstId,0),Edge(edge.dstId,edge.srcId,0)))
        
      }
      
     var graph: Graph[(Long, Boolean), Int]= Graph.fromEdges(file, (1L, false)).mapVertices((id , value)=> {
        (id, false)
     })
    
    return graph
    
  }//End: loadGraph()

}
