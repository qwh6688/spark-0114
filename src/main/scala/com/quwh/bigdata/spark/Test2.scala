package com.quwh.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test2 {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val listRDD: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5,6,7,8),3)
    val arrayRDD: Array[Array[Int]] = listRDD.glom.collect()
    arrayRDD.foreach(x=>println(x.mkString(",")))
    sc.stop()
  }

}
