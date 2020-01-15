package com.quwh.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
先写出语法是否正确, 再写相应的逻辑;
 */
object Spark05_Oper4 {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    // flatMap
    val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1,2),List(3,4)))
    val flatMapRDD: RDD[Int] = listRDD.flatMap(datas=>datas)//
//    val testRDD: RDD[Nothing] = listRDD.flatMap(x=>List())
    flatMapRDD.collect().foreach(println)



  }

}
