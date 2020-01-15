package com.quwh.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/*
先写出语法是否正确, 再写相应的逻辑;
 */
object Spark04_Oper3 {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)
    // 当传入的参数 有多个时, 用模式匹配
    /*
    需求：创建一个RDD，使每个元素跟所在分区形成一个元组组成一个新的RDD
     */
    val tupleRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      case (num, datas) => {
        datas.map((_, "分区: " + num))
      }
    }
    tupleRDD.collect().foreach(println)


  }

}
