package com.quwh.bigdata.sql

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.{SparkConf, sql}

/*
强类型用户自定义聚合函数：通过继承Aggregator来实现强类型自定义聚合函数
*/
object SparkSQL05_UDAF_Class {
  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    val spark: SparkSession = sql.SparkSession.builder().config(sparkConfig).getOrCreate()
    val udaf = new MyAgeAvgClassFunction
    //这总强类型的数据结构，不能用sql语句进行查询，需要换种方法
    //及那个聚合函数转换为查询列
    val avgCol: TypedColumn[UserBean, Double] = udaf.toColumn.name("avgAge")
    val jsonDF: DataFrame = spark.read.json("input/user.json")
    import spark.implicits._
    val userDS: Dataset[UserBean] = jsonDF.as[UserBean]
    userDS.select(avgCol).show()

    //TODO 释放资源
    spark.stop()
  }

}

case class UserBean(name: String, age: Long)

case class AvgBuffer(var sum: Long, var count: Int) // 默认是val
class MyAgeAvgClassFunction extends Aggregator[UserBean, AvgBuffer, Double] {
  //初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }

  //把缓冲区中的数据和输入的数据处理一下
  override def reduce(b: AvgBuffer, a: UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
    //    AvgBuffer(b.sum, b.count) //测试一下
  }

  //缓存区的合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }


  //计算
  override def finish(reduction: AvgBuffer): Double = {
    reduction.sum.toDouble / reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}