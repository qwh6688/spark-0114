package com.quwh.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 */
object Spark12_Oper11 {
  /**
    * sortBy
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val listRDD: RDD[Int] = sc.makeRDD(List(2, 3, 1, 5, 4, 8, 0, 7))
    val sortByRDD: RDD[Int] = listRDD.sortBy(x => x, true,2)
//    sortByRDD.collect().foreach(println)
    sortByRDD.saveAsTextFile("output")
    sc.stop()


  }
}
