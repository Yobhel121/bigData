package com.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object WordCount3 {

  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf)

    wordCount1(sc)
    wordCount2(sc)
    wordCount3(sc)
    wordCount4(sc)
    wordCount5(sc)
    wordCount6(sc)
    wordCount7(sc)
    wordCount8(sc)
    wordCount9(sc)

    sc.stop()
  }

  // groupBy
  def wordCount1(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordCount: RDD[(String, Int)] = group.mapValues(list => list.size)
    wordCount.collect().foreach(println)
  }

  // groupByKey
  def wordCount2(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val group = wordOne.groupByKey()
    val wordCount: RDD[(String, Int)] = group.mapValues(list => list.size)
    wordCount.collect().foreach(println)
  }

  // reduceByKey
  def wordCount3(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount = wordOne.reduceByKey(_ + _)
    wordCount.collect().foreach(println)
  }

  // aggregateByKey
  def wordCount4(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount = wordOne.aggregateByKey(0)(_ + _, _ + _)
    wordCount.collect().foreach(println)
  }

  // foldByKey
  def wordCount5(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne = words.map((_, 1))
    val wordCount = wordOne.foldByKey(0)(_ + _)
    wordCount.collect().foreach(println)
  }

  // combineByKey
  def wordCount6(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount = wordOne.combineByKey(
      v => v,
      (x: Int, y) => x + y,
      (x: Int, y: Int) => x + y
    )
    wordCount.collect().foreach(println)
  }

  // countByKey
  def wordCount7(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: collection.Map[String, Long] = wordOne.countByKey()
    wordCount.foreach(println)
  }

  // countByValue
  def wordCount8(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordCount: collection.Map[String, Long] = words.countByValue()
    wordCount.foreach(println)
  }

  // reduce, aggregate, fold
  def wordCount9(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    val words = rdd.flatMap(_.split(" "))

    // 【（word, count）,(word, count)】
    // word => Map[(word,1)]
    val mapWord = words.map(
      word => {
        mutable.Map[String, Long]((word, 1))
      }
    )

    val wordCount = mapWord.reduce(
      (map1, map2) => {
        map2.foreach {
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
          }
        }
        map1
      }
    )
    println(wordCount)
  }


}
