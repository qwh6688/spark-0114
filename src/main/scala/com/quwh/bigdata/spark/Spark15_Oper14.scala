package com.quwh.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 */
object Spark15_Oper14 {
  /**
    * aggregateByKey，分区内聚合，分区间聚合；
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    // 创建一个pairRDD，取出每个分区相同key对应值的最大值，然后相加
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val rdd: RDD[(String, Int)] = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    val arrayRDD: RDD[Array[(String, Int)]] = rdd.glom()
    val tempRDD: Array[Array[(String, Int)]] = arrayRDD.collect()
    tempRDD.foreach( x=>{
      println(x.mkString(","))
    })
    
    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(math.max(_, _), _ + _)
    aggRDD.collect().foreach(println)
    sc.stop()

  }
}
