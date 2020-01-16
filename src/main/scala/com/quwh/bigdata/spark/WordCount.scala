package com.quwh.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(config)
    val lines: RDD[String] = sc.textFile("D:\\develop\\ideaWorkspace\\spark-0114\\input\\word.txt")
    /*
    第一种方法
     */
    /* val words: RDD[String] = lines.flatMap(_.split(" "))
     val wordToOne: RDD[(String, Int)] = words.map((_,1))
     val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)
     val result: Array[(String, Int)] = wordToSum.collect()
     //println(result)
     result.foreach(println)*/
    // 第二种方法
    //groupbyKey 看清结构才能知道相应的操作
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))
    /*  val groupByKey: RDD[(String, Iterable[Int])] = wordToOne.groupByKey()
     val sumRDD: RDD[(String, Int)] = groupByKey.map(tuple=>(tuple._1,tuple._2.sum))
     sumRDD.collect().foreach(println)*/


    // 第三种方法
    //aggregateByKey
    val aggregateByKeyRDD: RDD[(String, Int)] = wordToOne.aggregateByKey(0)(_+_,_+_)
    aggregateByKeyRDD.collect().foreach(println)
    sc.stop()

  }
}
