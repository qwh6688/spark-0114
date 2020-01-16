package com.quwh.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]
 */
object Spark06_Oper5 {
  // glom
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val listRDD: RDD[Int] = sc.makeRDD(1 to 8, 3)
    val glomRDD: RDD[Array[Int]] = listRDD.glom()
    //    glomRDD.collect().foreach(println)
    /*  glomRDD.collect().foreach(
        array => {
          array.foreach(println)
        }
      )*/
    /*    glomRDD.collect().foreach(array => {
          for (elem <- array) {
            println(elem)
          }
        })

      */
    //array.mkString 打印数组,中间用, 号隔开
    glomRDD.collect().foreach(array => println(array.mkString("$")))
  }
}
