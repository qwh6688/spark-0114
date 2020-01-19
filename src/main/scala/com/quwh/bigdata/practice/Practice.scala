package com.quwh.bigdata.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
1. 数据结构：时间戳，省份，城市，用户，广告，中间字段使用空格分割。
样本如下：
1516609143867 6 7 64 16
1516609143869 9 4 75 18
1516609143869 1 7 87 12

2. 需求：统计出每一个省份广告被点击次数的TOP3
 */
object Practice {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Paratice")
    val sc = new SparkContext(conf)
    val line: RDD[String] = sc.textFile("D:\\develop\\ideaWorkspace\\spark-0114\\input\\agent.log")
    // 转换结构 （6，16）=》（（6，16），1）
    //    val strings: Array[String] = sourceData.collect() 看结构
    val provinceAvToOne: RDD[((String, String), Int)] = line.map { x =>
      val fields: Array[String] = x.split(" ")
      ((fields(1), fields(4)), 1)
    }
    //4.计算每个省中每个广告被点击的总数：((Province,AD),sum)
    val provinceAvToSum: RDD[((String, String), Int)] = provinceAvToOne.reduceByKey(_ + _)
    //转换结构  将省份作为key，广告 + 点击数为value：(Province,(AD,sum))
    val provinceToAdSum: RDD[(String, (String, Int))] = provinceAvToSum.map {
      x => (x._1._1, (x._1._2, x._2))
    }
    //6.将同一个省份的所有广告进行聚合(Province,List((AD1,sum1),(AD2,sum2)...))
    val provinceGroup: RDD[(String, Iterable[(String, Int)])] = provinceToAdSum.groupByKey()
    //改下其他的算子试一下
    //7.对同一个省份所有广告的集合进行排序并取前3条，排序规则为广告点击总数
    val provinceAdTop3: RDD[(String, List[(String, Int)])] = provinceGroup.mapValues {
      x =>
//            x.toList.sortWith((x, y) => x._2 > y._2).take(3)
        x.toList.sortBy(x => x._2).reverse.take(3)
    }
    provinceAdTop3
    //8.将数据拉取到Driver端并打印
    provinceAdTop3.collect().foreach(println)
    sc.stop()





    /*
      结果
      (4,List((12,25), (2,22), (16,22)))
    */





    //    provinceGroup.collect().foreach
    //    (4,CompactBuffer((12,25), (25,11), (27,13), (13,21), (8,21), (1,20), (6,15), (7,7), (3,15), (22,18), (10,10), (19,10), (9,19), (23,18), (18,17), (11,11), (28,16), (26,10), (2,22), (29,13), (17,14), (16,22), (5,14), (21,17), (0,14), (15,17), (24,18), (4,17), (20,11), (14,18)))


    //    provinceAvToSum.collect().foreach(println)
    /*
    ((7,25),21)
    ((7,17),13)
    ((2,1),16)
     */

  }

}
