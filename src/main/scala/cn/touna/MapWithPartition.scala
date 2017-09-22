package cn.touna

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created with IntelliJ IDEA. 
  * User: lijie
  * Email:lijiewj51137@touna.cn 
  * Date: 2017/7/21 
  * Time: 11:25  
  */
object MapWithPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MapWithPartition")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 1)
    println(rdd1.foreachPartition(x => println(x.reduce(_ + _))))
//    val func = (a: Int, b: Iterator[(Int)]) => {
//      b.toList.map(x => {
//        (a,x)
//      }).toIterator
//    }


//    val r1 = rdd1.repartition(5)
//   val r1 =  rdd1.coalesce(5,false)
//    println(rdd1.getNumPartitions)
//    println(r1.getNumPartitions)
//    println(r1.getNumPartitions)

  }
}
