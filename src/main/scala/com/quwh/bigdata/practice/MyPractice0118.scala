package com.quwh.bigdata.practice

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object MyPractice0118 {
  /*
  1. 数据结构：时间戳，省份，城市，用户，广告，中间字段使用空格分割。
  1516609143867 6 7 64 16
  1516609143869 9 4 75 18
2. 需求：统计出每一个省份广告被点击次数的TOP3

 省份，广告， 点击次数
 Province，广告， 1，
   */
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Paratice")
    val sc = new SparkContext(conf)
    val line: RDD[String] = sc.textFile("D:\\develop\\ideaWorkspace\\spark-0114\\input\\agent.log")
    val ProvinceADToOne: RDD[((String, String), Int)] = line.map { x =>
      val fields: Array[String] = x.split(" ")
      ((fields(1), fields(4)), 1)
    }
    val ProvinceADToSum: RDD[((String, String), Int)] = ProvinceADToOne.reduceByKey(_ + _)
//    ProvinceADToSum.sortBy(_._2,true) sortBy按照某个字段进行排序

    //sortWith 里面需要传进一个 比较函数；对于 **集合内 排序使用
    val ProvinceToADSum: RDD[(String, (String, Int))] = ProvinceADToSum.map(x =>
      (x._1._1, (x._1._2, x._2)))


    val ProvinceGroup: RDD[(String, Iterable[(String, Int)])] = ProvinceToADSum.groupByKey()
    val provinceAdTop3: RDD[(String, List[(String, Int)])] = ProvinceGroup.mapValues { x =>
      x.toList.sortWith {
        (a, b) => a._2 > b._2
      }.take(3)
    }
    //再按照省份进行排序
    val provinceOrderedAdTop3: RDD[(String, List[(String, Int)])] = provinceAdTop3.sortBy(_._1,true)
    provinceOrderedAdTop3.collect().foreach(println)

    /* val wordsRDD1: RDD[String] = line.flatMap(_.split(" "))
     wordsRDD1.collect.foreach(println)
     println("==============================")
     val wordsRDD2: RDD[Array[String]] = line.map(_.split(" "))
     wordsRDD2.collect.foreach(x =>
       println(x.mkString(","))
     )*/
    sc.stop()


  }

}
