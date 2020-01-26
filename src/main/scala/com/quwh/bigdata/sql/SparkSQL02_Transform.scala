package com.quwh.bigdata.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, sql}

object SparkSQL02_Transform {
  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    val spark: SparkSession = sql.SparkSession.builder().config(sparkConfig).getOrCreate()

    val listRDD: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "huitailang", 6), (2, "xiyangyang", 4), (3, "meiyangyang", 8)))
    import spark.implicits._ //spark不是包名，二十sparkSession对象
    //RDD ==>DF
    val df: DataFrame = listRDD.toDF("id", "name", "age")
    // DF==>DS
    val ds: Dataset[User] = df.as[User]
    //    ds.show()

    //DS==>DF
    val df1: DataFrame = ds.toDF()

    //DF==>RDD
    //df只关心结构，
    val rdd1: RDD[Row] = df1.rdd
    rdd1.foreach {
      case row => {
        println(row.getInt(0) + ", " + row.getString(1) + ", " + row.getInt(2))
      }
    }



    //展示
    // df.show()


    //释放资源
    spark.stop()
  }

}

case class User(id: Int, name: String, age: Int)
