
/** ************************************************
 * Note: In Maven dependency                      *
 * Spark and Spark SQL must have the same version *
 * This code is created using 2.11 version 1.6.0  *
 * **************************************************/

import java.io._
import scala.io.Source
import scala.math.random
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SQLContext}
import au.com.bytecode.opencsv.CSVParser

object SparkCore extends App {

  override def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("local")
    val sc = new SparkContext(conf)


    /** *************************************************
     * *
     * IF YOU ARE USING TERMINAL MODE NOT AN IDE      * 
     * START HERE.    					      *
     * *
     * **************************************************/


    sc.setLogLevel("WARN")

    // Exploring Scala and Spark

    def addInt(a: Int = 5, b: Int = 7): Int = {
      var sum: Int = 0
      sum = a + b
      sum
    }

    def ProductInt(a: Int = 5, b: Int = 7): Int = a * b

    println("Returned Sum  and Product are : " + addInt(10) + " and " + ProductInt(10));


    //Exploring Lists
    val myOne = List(List("cat", "mat", "bat"),
      List("hat", "mat", "rat"),
      List("cat", "mat", "sat"),
      List("cat", "fat", "bat"),
      List("eat", "fat", "cat"),
      List("hat", "fat", "pat"))
    myOne.foreach(println)
    val myTwo = sc.parallelize(myOne)
    myTwo.foreach(println)
    val myThree = myTwo.map(x => (x(1), x(2)))
    myThree.foreach(println)
    println("---------------------")
    val myFour = myThree.sortBy(_._1, false).sortBy(_._2)
    myFour.foreach(println)


    // Write, Read, wordCount
    val writer = new PrintWriter(new File("result.txt"))
    writer.write("Hello World")

    val tf = sc.textFile("dataFile")


    val countsOne = tf
      .flatMap(line => line.split("\\s+"))
      .map(x => (x.toLowerCase(), 1))
      .reduceByKey(_ + _)
      .cache()
    val countsTwo = countsOne.sortByKey(true)
    countsTwo.foreach(println)

    val res = countsTwo.collect()
    for (n <- res) writer.println(n.toString())
    writer.close()


    // Exploring arrays and integers
    val data1 = Array(7, 8, 2, 10, 4, 10, 9, 4, 1, 6)
    val rdd1 = sc.parallelize(data1)
    val max1 = rdd1.max()
    val rdd2 = rdd1.filter(_ != max1)
    val max2 = rdd2.max()

    def getCount(in: Array[Int], start: Int, stop: Int): Int = {
      val cnt = in.map(x => if (x >= start && x <= stop)
        1
      else
        0
      ).reduce(_ + _)
      cnt
    }


    println("\nNumber of items between 4 and 7 is " + getCount(data1, 4, 7))
    println("\nSecond largest value is " + max2)

    // processing access log files using Spark
    def isNumeric(input: String): Boolean = input.forall(_.isDigit)

    val accessLog = sc.textFile("access_log").cache
    val logRecord = accessLog
      .filter(line => (line.split("\\s+").size > 9 && isNumeric(line.split("\\s+")(9))))
      .map(line => (line.split("\\s+")(0), line.split("\\s+")(9)))
      .filter(x => x._2 != "0").cache()

    def getIP(in: String): String = {
      in.split(" ")(0)
    }

    def getSite(in: String): String = {
      in.split(" ")(6)
    }

    def getSize(in: String): String = {
      val x = in.split(" ");
      if (x.count(_ => true) > 9)
        x(9)
      else
        "0"
    }

    val data = accessLog
    val cleanData = data.map(x => (getIP(x), getSize(x)))
      .filter(x => x._2 != "-").filter(x => x._2 != "")
      .filter(x => x._2 != "0").cache()
    val cleanDataInt = cleanData.map(x => (x._1, x._2.toInt))

    val cleanDataIntGroup = cleanDataInt.groupByKey()
    val cleanDataIntGroupMap = cleanDataIntGroup
      .map(x => (x._1, (x._2.count(_ => true), x._2.reduce(_ + _))))
    val average = cleanDataIntGroupMap.map(x => (x._1, x._2._2 / x._2._1))
    println("\n\n\tPRINTING AVERAGES (just 10 rows)")
    average.sortBy(_._1).top(10).foreach(println)
    //average.saveAsTextFile("file:///home/cloudera/Spark/AVG")


  }
}