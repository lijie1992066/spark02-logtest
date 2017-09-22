package cn.touna

import java.net.URI

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created with IntelliJ IDEA. 
  * User: lijie
  * Email:lijiewj51137@touna.cn 
  * Date: 2017/7/20 
  * Time: 10:48  
  */
class URLtake {
}

object URLtake {

  val PATH = "src/main/files/usercount/test.log"
  val ARR = Array("java.test.cn", "php.test.cn", "net.test.cn")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("url")
    val sc = new SparkContext(conf)
    var result = new ListBuffer[Array[Tuple3[String, String, Int]]]
    val rdd1 = sc.textFile(PATH).flatMap(_.split("\t")).map((_, 1)).combineByKey(x => x, (a: Int, b: Int) => a + b, (c: Int, d: Int) => c + d)
    val rdd2 = rdd1.map(x => {
      (new URI(x._1).getHost, x._1, x._2)
    })
    for (a <- ARR) {
      val rddTmp = rdd2.filter(x => {
        x._1 == a
      }).sortBy(_._3, false) take (3)
      result.+=(rddTmp)
    }

    for (a <- result) {
      for (b <- a) {
        print(b._1 + "," + b._2 + "," + b._3)
        println()
      }
    }

    //    val r1 = sc.textFile(PATH).flatMap(x => {
    //      x.split("\t")
    //    }).map((_, 1)).groupByKey.mapValues(_.sum)
    //    val r2 = sc.textFile(PATH).flatMap(x => {
    //      x.split("\t")
    //    }).map((_, 1)).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2))
    //    println(r1)
    //    println(r2)

  }
}


class IteblogPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val domain = new java.net.URL(key.toString).getHost()
    val code = (domain.hashCode % numPartitions)
    if (code < 0) {
      code + numPartitions
    } else {
      code
    }
  }

  override def equals(other: Any): Boolean = other match {
    case iteblog: IteblogPartitioner =>
      iteblog.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}