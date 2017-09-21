package com.tu.bdap.utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD

//This class loads Graph-DataSets from HDFS or local disk storage
object DataSetLoader {

  def loadUSA(sc: SparkContext, path: String) : RDD[Edge[Long]] = {
    sc.textFile(path)
      .filter { x => x.startsWith("a") }
      .map { line =>
        val fields = line.split(" ")
        Edge(fields(1).toLong, fields(2).toLong, fields(3).toLong)

      }
  }

  def loadTwitter(sc: SparkContext, path: String)  : RDD[Edge[Long]] =  {
    sc.textFile(path)
      .filter { x => Character.isDigit(x.charAt(0)) }
      .map { line =>
        val fields = line.split(" ")
        Edge(fields(0).toLong, fields(1).toLong, 1)
      };
  }
  
   def loadFriendster(sc: SparkContext, path: String)  : RDD[Edge[Long]] =  {
    sc.textFile(path)
      .filter { x => Character.isDigit(x.charAt(0)) }
      .map { line =>
        val fields = line.split(" ")
        Edge(fields(0).toLong, fields(1).toLong, 1)
      }
  }

   def loadLivejournal(sc: SparkContext, path: String)  : RDD[Edge[Long]] =  {
    sc.textFile(path)
     .filter { x => Character.isDigit(x.charAt(0)) }
      .map { line =>
        val fields = line.split(" ")
        Edge(fields(0).toLong, -fields(1).toLong, 0)
      }
  }
}