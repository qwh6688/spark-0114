package com.quwh.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 */
object Spark14_Oper12 {
  /**
    * sortBy
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val wordsRDD = sc.makeRDD(Array("one", "two", "two", "three", "three", "three"))
    val tupleRDD: RDD[(String, Int)] = wordsRDD.map((_,1))
    val groupByKeyRDD: RDD[(String, Iterable[Int])] = tupleRDD.groupByKey()
//    tupleRDD.aggregateByKey()
    //通过groupByKey 完成单词计数
    val wordSum: RDD[(String, Int)] = groupByKeyRDD.map(tuple=>(tuple._1,tuple._2.sum))
    wordSum.collect().foreach(println)



  }
}
