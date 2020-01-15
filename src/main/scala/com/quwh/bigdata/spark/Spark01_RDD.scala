package com.quwh.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    //1) 内存中创建makeRDD
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3))
    //parallelize
    val arrayRDD: RDD[String] = sc.parallelize(Array("1","2","3","曲文辉"))
//    listRDD.collect().foreach(println)
    arrayRDD.collect().foreach(println)

    //2) 外部存储中创建
    val fileRDD: RDD[String] = sc.textFile("input/")


  }

}
