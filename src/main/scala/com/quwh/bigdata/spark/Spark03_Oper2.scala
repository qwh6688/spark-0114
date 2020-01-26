package com.quwh.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/*
mapPartitions
 */
object Spark03_Oper2 {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
//    listRDD.reduce()
    /* val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(dataset => {
       dataset.map(date => date * 2)
     })*/
    //    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(_.map(_*2))
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(x => x.map(_ * 2))
    mapPartitionsRDD.collect().foreach(println)


  }

}
