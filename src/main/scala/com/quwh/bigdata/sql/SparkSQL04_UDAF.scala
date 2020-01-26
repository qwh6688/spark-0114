package com.quwh.bigdata.sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, sql}

/*
弱类型用户自定义聚合函数：通过继承UserDefinedAggregateFunction来实现用户自定义聚合函数。
 */
object SparkSQL04_UDAF {
  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    val spark: SparkSession = sql.SparkSession.builder().config(sparkConfig).getOrCreate()
    import spark.implicits._

    // 创建聚合函数对象
    val myAgeAvgFunction = new MyAgeAvgFunction
    //注册聚合函数
    spark.udf.register("avgAge",myAgeAvgFunction )
    //使用聚合函数
    val jsonDF: DataFrame = spark.read.json("input/user.json") // 外部文件创建DataFrame
    jsonDF.createOrReplaceTempView("user")
    spark.sql("select avgAge(age) from user").show()


    //TODO 释放资源
    spark.stop()
  }

}

class MyAgeAvgFunction extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    new StructType().add("age", LongType)
  }

  /*
  计算时的数据结构
   */
  override def bufferSchema: StructType = {
    new StructType().add("sum", LongType).add("count", LongType)
  }

  //函数返回时的数据类型
  override def dataType: DataType = {
    DoubleType
  }

  // 函数是否稳定
  override def deterministic: Boolean = true

  // 计算之前的缓冲区的初始化  sum，count 初始值是什么
  //buffer是个数组，只考虑结构不考虑类型，有顺序
  //没有名称，只有顺序，只有结构
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //根据查询结果更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //sum
    buffer(0) = buffer.getLong(0) + input.getLong(0)
    //count

    buffer(1) = buffer.getLong(1) + 1
  }

  //将多个节点的缓冲区合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //sum
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    //count
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)

  }

  //计算
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}
