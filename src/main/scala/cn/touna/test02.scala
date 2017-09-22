package cn.touna

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA. 
  * User: lijie
  * Email:lijiewj51137@touna.cn 
  * Date: 2017/7/24 
  * Time: 14:58  
  */
object test02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LocalTake").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("src/main/files/userlocal/*.log").map(x => {
      val split = x.split(",")
      val time = if (split(3) == "1") -split(1).toLong else split(1).toLong
      ((split(0), split(2)), time)
    })

    val rdd2 = rdd1.reduceByKey(_ + _).map(x => {
      (x._1._2, (x._1._1, x._2))
    })

    //    println(rdd2.collect().toBuffer)

    val rdd3 = sc.textFile("src/main/files/userlocal/*.txt").map(x => {
      val split = x.split(",")
      (split(0), (split(1), split(2)))
    })

    //    println(rdd3.collect().toBuffer)

    val rdd4 = rdd2.join(rdd3).groupByKey.mapValues(x => {
      x.toList.sortBy(_._2).take(3)
    })

    println(rdd4.collect().toBuffer)

  }
}
