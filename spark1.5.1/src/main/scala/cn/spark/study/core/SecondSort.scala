package cn.spark.study.core

import scala.tools.nsc.doc.model.Val
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SecondSort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
        .setAppName("SecondSort")
        .setMaster("local")
    val sc = new SparkContext(conf)
    
    val lines = sc.textFile("C://Users//Administrator//Desktop//sort.txt", 1)
    val pairs = lines.map { line => (
        new SecondSortKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt),
        line)}
    val sortedPairs = pairs.sortByKey()
    val soredLines = sortedPairs.map(sortedPair => sortedPair._2)
    
    soredLines.foreach{ line => println(line)}
    
    
  }
  
}