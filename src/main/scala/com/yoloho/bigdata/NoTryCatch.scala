package com.yoloho.bigdata

import java.util.{Date, Properties}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object NoTryCatch {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\service\\hadoop-2.7.4-window") //加载hadoop组件
    val scf = new SparkConf().setMaster("local[4]").setAppName("")
    val sc: SparkContext = new SparkContext(scf)
    sc.setLogLevel("WARN")
    println("开始时间" + new Date().toLocaleString)
    val hc: HiveContext = new HiveContext(sc)
    val frame: DataFrame = hc.sql("select * from fdm.dayima_user limit 10")
    frame.cache()
    frame.foreach(rdd=>{
      println(rdd)
    })
    val url = "jdbc:mysql://192.168.124.96:3306/spark"
    val tablename = "dayima_userlimit10"
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "dayima")
    frame.write.mode("append").jdbc(url, tablename, properties)
    println("结束时间" + new Date().toLocaleString)
    sc.stop()

  }
}
