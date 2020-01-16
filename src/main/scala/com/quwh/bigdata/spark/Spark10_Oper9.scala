package com.quwh.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 */
object Spark10_Oper9 {
  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,1,5,2,9,6,1))
    val distinctRDD: RDD[Int] = listRDD.distinct(2)
//    distinctRDD.collect().foreach(println)
    //打印到两个分区里
    distinctRDD.saveAsTextFile("output")



  }
}
