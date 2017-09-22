package cn.touna

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created with IntelliJ IDEA. 
  * User: lijie
  * Email:lijiewj51137@touna.cn 
  * Date: 2017/7/24 
  * Time: 11:14  
  */


object MyTest {

  val func = (index: Int, iter: Iterator[(Tuple2[String, Int])]) => {
    iter.toList.map(x => "[partID:" + index + ", val: " + x._1 + "]").iterator
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd01 = sc.parallelize(Array("a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m"), 6).map((_, 1))
    val rdd02 = rdd01.partitionBy(new MyPartitioner(3)).mapPartitionsWithIndex(func)
    println(rdd02.collect.toBuffer)
    sc.stop()

  }
}

//class MyPartitioner(val count: Int) extends Partitioner {
//  override def numPartitions: Int = {
//    count
//  }
//
//  override def getPartition(key: Any): Int = key match {
//    //    if (key.toString == "a") 0
//    //    if (key.toString == "b") 1
//    //    if (key.toString == "c") 2
//    //    else 3
//
//    case "a" => 0
//    case "b" => 1
//    case _ => 2
//  }


//}
