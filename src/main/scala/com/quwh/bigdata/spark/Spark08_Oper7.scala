package com.quwh.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
groupBy
作用：分组，按照传入函数的返回值进行分组。将相同的key对应的值放入一个迭代器。
 */
object Spark08_Oper7 {
  /**
    * filter
    * 2. 需求：创建一个RDD，按照元素模以2的值进行分组。
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 3)
    val filterRDD: RDD[Int] = listRDD.filter(i=>i%2==0)
    filterRDD.collect().foreach(println)



  }
}
