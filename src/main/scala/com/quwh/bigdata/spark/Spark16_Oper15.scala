package com.quwh.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 */
object Spark16_Oper15 {
  /**
    * 创建一个pairRDD，根据key计算每种key的均值。（先计算每个key出现的次数以及可以对应值的总和，再相除得到结果）
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val input = sc.parallelize(Array(("a", 88), ("b", 95), ("a", 91), ("b", 93), ("a", 95), ("b", 98)), 2)
    //    (88, 1)
    //    val combine = input.combineByKey((_, 1), (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val combineByKeyRDD: RDD[(String, (Int, Int))] = input.combineByKey((_, 1), (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1), (x:(Int,Int),y:(Int,Int))=> (x._1+y._1,x._2+y._2))
    //rdd里面存的是数据集合
    val collectRDD: Array[(String, (Int, Int))] = combineByKeyRDD.collect
    //map里面要有计算，用 {}
    val resultRDD: Array[(String, Double)] = collectRDD.map {
      case (key, value) => (key, value._1 / value._2.toDouble)
    }
    resultRDD.foreach(println)
    sc.stop()

  }
}
