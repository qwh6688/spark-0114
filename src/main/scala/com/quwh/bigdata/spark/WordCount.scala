package com.quwh.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(config)
    val lines: RDD[String] = sc.textFile("D:\\idea workspace\\spark-0114\\input\\word.txt")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne: RDD[(String, Int)] = words.map((_,1))
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
    val result: Array[(String, Int)] = wordToSum.collect()
    //println(result)
    result.foreach(println)



  }
}
