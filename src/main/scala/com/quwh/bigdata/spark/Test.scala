package com.quwh.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    val mapRDD: RDD[String] = listRDD.map(x=>"X"+x)
    mapRDD.collect().foreach(println)
  }

}
