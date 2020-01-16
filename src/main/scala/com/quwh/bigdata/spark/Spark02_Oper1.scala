package com.quwh.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/*
map的使用
 */
object Spark02_Oper1 {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
//    listRDD.groupBy()
    //val mapRDD: RDD[Int] = listRDD.map(x=>x*2)
    val mapRDD: RDD[Int] = listRDD.map(_*2)
    mapRDD.collect().foreach(println)


  }

}
