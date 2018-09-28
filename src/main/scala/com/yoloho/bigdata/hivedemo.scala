package com.yoloho.bigdata

import java.io.{FileNotFoundException, IOException}
import java.util.{Date, Properties}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 保存hive数据到本地mysql
  **/
object hivedemo {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\service\\hadoop-2.7.4-window") //加载hadoop组件
    val hivedemo1: SparkConf = new SparkConf().setMaster("local[4]").setAppName("hivedemo")


    val sc: SparkContext = new SparkContext(hivedemo1)
    sc.setLogLevel("WARN")
    try {
    val hc: HiveContext = new HiveContext(sc)
    println("开始" + new Date().toLocaleString)
     val frame: DataFrame = hc.sql("select * from bdm.dayima_search_log limit 10 ")
    //val frame: DataFrame = hc.sql("select * from fdm.dayima_user limit 10")
    frame.cache()
    val url = "jdbc:mysql://192.168.124.96:3306/spark"
    //val tablename="dayima_user0921" select *  from   dayima_user where nick like '236299301';
    val tablename = "dayima_userlimita1q1"
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "dayima")
    println("测试" + new Date().toLocaleString)

      frame.write.mode("append").jdbc(url, tablename, properties)
    frame.show()
      println("结束时间" + new Date().toLocaleString)
    }
    catch {
      case e: FileNotFoundException => println("Missing file exception")
      case ex: IOException => println("IO Exception")
      case ee: ArithmeticException => println(ee)
      case eee: Throwable => println("found a unknown exception" + eee)
      case ef: NumberFormatException => println(ef)
      case ec: Exception => println(ec)
      case e: IllegalArgumentException => println("illegal arg. exception")
      case e: IllegalStateException => println("illegal state exception")
    }


  }
}
