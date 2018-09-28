package com.yoloho.bigdata

import java.io.{FileNotFoundException, IOException}
import java.util.{Date, Properties}

import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 保存每天搜索的数据到hive
  */
object saveIS {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\service\\hadoop-2.7.4-window") //加载hadoop组件
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local[4]")
      .set("spark.executor.memory", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")
    try {
      println("开始时间"+new Date().toLocaleString)
      val hiveContext = new HiveContext(sc)
      //val frame: DataFrame = hiveContext.sql("select * from fdm.dayima_user limit 100")
     // val frame: DataFrame = hiveContext.sql(" SELECT  * FROM  bdm.dayima_search_log WHERE uid  = '85070343'")
      val frame: DataFrame = hiveContext.sql("SELECT  * FROM  bdm.dayima_search_log WHERE search_time BETWEEN '2018-09-27' AND '2018-09-28' order by  search_time ")

      //val frame: DataFrame = hiveContext.sql(" SELECT * FROM fdm.dayima_user WHERE regcountry LIKE '%北京%' limit 200 ")
      val url = "jdbc:mysql://192.168.124.96:3306/spark"
      //val tablename="dayima_user0921"--2580we
      val tablename = "dayima_userlimit1sea"
      val properties = new Properties()
      properties.setProperty("user", "root")
      properties.setProperty("password", "dayima")

      frame.cache()
      frame.printSchema()
      println("测试"+new Date().toLocaleString)
      frame.foreach(rdd=>{
        println(rdd)
      })
      val rd1: DataFrameWriter[Row] = frame.write.mode("append")

      println("准备开始存储"+new Date().toLocaleString)
      //rd1.save("e://demo22")
      rd1.jdbc(url,tablename,properties)
      println("结束时间"+new Date().toLocaleString)
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
    finally {
      sc.stop()
    }


  }
}
