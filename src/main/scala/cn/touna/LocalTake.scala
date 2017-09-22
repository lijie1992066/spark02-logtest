package cn.touna

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA. 
  * User: lijie
  * Email:lijiewj51137@touna.cn 
  * Date: 2017/7/20 
  * Time: 11:41  
  */
object LocalTake {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LocalTake").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("src/main/files/userlocal/*.log").map(
      x => {
        val tmp = x.split(",")
        val mobile = tmp(0)
        val local = tmp(2)
        val locType = tmp(3)
        val time = if (locType == "1") -tmp(1).toLong else tmp(1).toLong
        ((mobile, local), time)
      }
    )
    val rdd2 = rdd1.aggregateByKey(0.toLong)((a: Long, b: Long) => {
      a + b
    }, (c: Long, d: Long) => {
      c + d
    }).map(x => {
      (x._1._2, (x._1._1, x._2))
    })

    val rdd01 = sc.textFile("src/main/files/userlocal/*.txt").map(x => {
      val tmp = x.split(",")
      (tmp(0), (tmp(1), tmp(2)))
    })

        println(rdd2.join(rdd01).collect().toBuffer)

    val rdd3 = rdd2.join(rdd01).map(x => {
      (x._2._1._1, x._1, x._2._1._2, x._2._2._1, x._2._2._2)
    })

    val rdd4 = rdd3.groupBy(_._1).mapValues(x => {
      x.toList.sortBy(_._3).take(3)
    })


    //        rdd4.saveAsTextFile("c://out")
    println(rdd4.collect().toBuffer)
    sc.stop()

  }
}


