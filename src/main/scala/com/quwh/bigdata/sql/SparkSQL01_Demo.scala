package com.quwh.bigdata.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, sql}


object SparkSQL01_Demo {
  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    val spark: SparkSession = sql.SparkSession.builder().config(sparkConfig).getOrCreate()
    val jsonDF: DataFrame = spark.read.json("input/user.json") // 外部文件创建DataFrame

    //将DataFrame转换成一张表
    jsonDF.createOrReplaceTempView("user")
    spark.sql("select name,age from user").show()




    //    jsonDF.show()


    spark.stop()
  }

}
