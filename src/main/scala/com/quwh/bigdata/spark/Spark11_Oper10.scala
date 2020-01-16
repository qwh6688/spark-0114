package com.quwh.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 */
object Spark11_Oper10 {
  /**
    * coalesce算子
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)
    println("缩减前的分区： " + listRDD.partitions.size)
    val coalesceRDD: RDD[Int] = listRDD.coalesce(3)
    println("缩减后的分区： " + coalesceRDD.partitions.size)
    coalesceRDD.saveAsTextFile("output")


  }
}
