package com.tu.bdap.utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD

/**
  * This class loads Graph-DataSets from HDFS or local disk storage
  */
object DataSetLoader {

  /** Load USA dataset
    *
    * @param sc spark context
    * @param path path to dataset
    * @return edge dataset read from file
    */
  def loadUSA(sc: SparkContext, path: String) : RDD[Edge[Long]] = {
    sc.textFile(path)
      .filter { x => x.startsWith("a") }
      .map { line =>
        val fields = line.split(" ")
        Edge(fields(1).toLong, fields(2).toLong, fields(3).toLong)

      }
  }

  /** Load Twitter dataset
    *
    * @param sc spark context
    * @param path path to dataset
    * @return edge dataset read from file
    */
  def loadTwitter(sc: SparkContext, path: String)  : RDD[Edge[Long]] =  {
    sc.textFile(path)
      .filter { x => Character.isDigit(x.charAt(0)) }
      .map { line =>
        val fields = line.split(" ")
        Edge(fields(0).toLong, fields(1).toLong, 1)
      };
  }

   /** Load Friendster dataset
    *
    * @param sc spark context
    * @param path path to dataset
    * @return edge dataset read from file
    */
   def loadFriendster(sc: SparkContext, path: String)  : RDD[Edge[Long]] =  {
    sc.textFile(path)
      .filter { x => Character.isDigit(x.charAt(0)) }
      .map { line =>
        val fields = line.split(" ")
        Edge(fields(0).toLong, fields(1).toLong, 1)
      }
  }

   /** Load LiveJournal dataset
    *
    * @param sc spark context
    * @param path path to dataset
    * @return edge dataset read from file
    */
   def loadLivejournal(sc: SparkContext, path: String)  : RDD[Edge[Long]] =  {
    sc.textFile(path)
     .filter { x => Character.isDigit(x.charAt(0)) }
      .map { line =>
        val fields = line.split(" ")
        Edge(fields(0).toLong, -fields(1).toLong, 0)
      }
  }
}