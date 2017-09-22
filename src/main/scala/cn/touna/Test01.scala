package cn.touna

import java.net.URI

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created with IntelliJ IDEA. 
  * User: lijie
  * Email:lijiewj51137@touna.cn 
  * Date: 2017/7/24 
  * Time: 14:13  
  */
object Test01 {
  val PATH = "src/main/files/usercount/test.log"
  val ARR = Array("java.test.cn", "php.test.cn", "net.test.cn")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("url")
    val sc = new SparkContext(conf)
    val rdd01 = sc.textFile(PATH).map(x => {
      val split = x.split("\t")
      (split(1), 1)
    }).aggregateByKey(0)(_ + _, _ + _).map(x => {
      val host = new URI(x._1).getHost
      (host, x._1, x._2)
    })


        for (a <- ARR) {
          val pr = rdd01.filter(x => {
            x._1 == a
          }).sortBy(_._3, false).take(3).toBuffer
          println(pr)
        }


  }
}


class MyPartitioner(val num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = key match {
    case "java.test.cn" => 0
    case "net.test.cn" => 1
    case _ => 2
  }

}