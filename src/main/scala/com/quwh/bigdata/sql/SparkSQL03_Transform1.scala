package com.quwh.bigdata.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, sql}

object SparkSQL03_Transform1 {
  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setMaster("local[*]").setAppName("SparkSQL01_Demo")
    val spark: SparkSession = sql.SparkSession.builder().config(sparkConfig).getOrCreate()
    val listRDD: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "huitailang", 6), (2, "xiyangyang", 4), (3, "meiyangyang", 8)))

    //spark不是包名，二十sparkSession对象
    import spark.implicits._
    //RDD ==>DS
    val useRDD: RDD[User] = listRDD.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val userDS: Dataset[User] = useRDD.toDS()
    //展示
    //    userDS.show

    //DS=>RDD
    val rdd2: RDD[User] = userDS.rdd
    rdd2.foreach(println)

    //释放资源
    spark.stop()
  }

}

