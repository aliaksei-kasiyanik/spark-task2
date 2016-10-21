package edu.gatech.cse6242

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
 

object Task2 {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Task2"))

    sc.textFile(args(0)).map(line => (line.split("\\s+")(1), line.split("\\s+")(2))).reduceByKey(_ + _).map(f => f._1+"\t"+f._2).saveAsTextFile("hdfs://localhost:8020" + args(1))
  }
}
