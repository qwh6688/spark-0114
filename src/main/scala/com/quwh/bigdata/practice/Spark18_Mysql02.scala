package com.quwh.bigdata.practice

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}


/*
检查点
 */

object Spark18_Mysql02 {
  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    sc.setCheckpointDir("ck")
    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val mapRDD: RDD[(Int, Int)] = listRDD.map((_, 1))
    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/rdd"
    val userName = "root"
    val passWd = "123456"
    //创建JdbcRDD
    /*
    先语法对，然后再写参数；
     */
    val sql = "select * from `user` where `id`>=? and id <=?;"
    //查询数据
    /* val rdd: JdbcRDD[Unit] = new JdbcRDD(
       sc,
       () => {
         Class.forName(driver)
         DriverManager.getConnection(url, userName, passWd)
       },
       sql,
       1,
       3,
       2,
       (rs) => {
         println(rs.getString(2) + ", " + rs.getInt(3))
       }
     )
     rdd.collect()*/

    val dataRDD = sc.makeRDD(List(("张三", 20), ("李四", 55), ("王五", 44)), 2)
    /*    dataRDD.foreach {
          case (name, age) => {
            Class.forName(driver)
            val connection: Connection = DriverManager.getConnection(url, userName, passWd)
            val sql = "insert into user(name,age) values (?,?)"
            val statement: PreparedStatement = connection.prepareStatement(sql)
            statement.setString(1, name)
            statement.setInt(2, age)
            statement.executeUpdate()
            statement.close()
            connection.close()
          }
        }*/

    //    修改
    dataRDD.foreachPartition(
      datas => {
        Class.forName(driver)
        val connection: Connection = DriverManager.getConnection(url, userName, passWd)
        datas.foreach {
          case (name, age) => {
            val sql = "insert into user(name,age) values (?,?)"
            val statement: PreparedStatement = connection.prepareStatement(sql)
            statement.setString(1, name)
            statement.setInt(2, age)
            statement.executeUpdate()
            statement.close()
          }
        }
        connection.close()
      })
    sc.stop()
    }

}
