package com.quwh.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/*
 */
object Spark13_Oper12 {
  /**
    * key-Value 类型的算子
    * partitionBy 重新分区
    * 自定义分区器
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    //分区规则
    val partByRDD: RDD[(String, Int)] = listRDD.partitionBy(new MyPartitioner(3))
    partByRDD.saveAsTextFile("output")
    sc.stop()


  }
}

//生命分区器
//继承抽象类 Partitioner
class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = {
    partitions
  }

  override def getPartition(key: Any): Int = key match {
    case "a" => 1
    case _ => 2
  }
}
