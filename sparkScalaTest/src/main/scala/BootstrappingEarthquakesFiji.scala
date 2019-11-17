import java.io._

import scala.io.Source
import scala.math.random
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.uncommons.maths.statistics.DataSet

import scala.collection.mutable

case class Quakes(station: String, mag: String)

object BootstrappingEarthquakesFiji extends App {
  override def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("local")
    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    // Exploring SparkSQL
    // Initialize an SQLContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext._
    import sqlContext.implicits._

    // Load a cvs file
    val csv = sc.textFile("quakes.csv")
    //    csv.foreach(println)
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    //    println("headerAndRows=" + headerAndRows)
    //    headerAndRows.foreach(println)
    val header = headerAndRows.first
    //    println("header=" + header)
    //    header.foreach(println)
    val mtcdata = headerAndRows.filter(_ (0) != header(0))
    //    println("mtcdata=" + mtcdata)
    //    mtcdata.foreach(println)
    //Step 2. Select a categorical variable and a numeric variable and form the key-value pair and
    //create a pairRDD called “population”.
    val mtQuakes = mtcdata
      .map(p => Quakes(p(5), p(4)))
      .toDF
    //    mtQuakes.printSchema
    // Run SQL queries from the Spark DataFrame
    mtQuakes.registerTempTable("quakes")
    val stationQuakes = sqlContext.sql("SELECT station, sum(mag) / count(mag), IF(stddev(mag)='NaN', 0, stddev(mag)) FROM quakes WHERE 1=1 group by station order by station ")
    // Step 3. Compute the mean mpg and variance for each category and display
    println("origin data:---------------------")
    stationQuakes.show()
    // Step 4. Create the sample for bootstrapping. All you need to do is take 25% of the population without replacement.
    val sample = mtQuakes.sample(false, 0.25).map(p => Quakes(p(0).toString, p(1).toString)).toDF
    sample.cache()
    sample.registerTempTable("sample")
    val sampleOut = sqlContext.sql("SELECT station, sum(mag) / count(mag), IF(stddev(mag)='NaN', 0, stddev(mag)) FROM sample WHERE 1=1 group by station order by station ")
    println("sample take 25% of the population without replacement:---------------------")
    sampleOut.show()
    // Step 5. Do 1000 times
    var a = 0
    val resampleTimes = 10
    import scala.collection.mutable.HashMap
    val reAvg = new HashMap[String, Double].withDefault(k => 0)
    val reStddev = new HashMap[String, Double].withDefault(k => 0)
    while (a < resampleTimes) {
      //5a. Create a “resampledData”. All you need to do is take 100% of the sample with replacement.
      val resampledData = sample.sample(true, 1.00).map(p => Quakes(p(0).toString, p(1).toString)).toDF
      //      resampledData.printSchema
      //5b. Compute the mean mpg and variance for each c
      resampledData.registerTempTable("resampledData")
      val resampledDataOut = sqlContext.sql("SELECT station, sum(mag) / count(mag), IF(stddev(mag)='NaN', 0, stddev(mag)) FROM resampledData WHERE 1=1 group by station order by station ")
      println("resampled Data take 100% of the sample with replacement:---------------------")
      resampledDataOut.show()

      //5c. Keep adding the values in some running sum
      resampledDataOut.collect().foreach(line => {
        val station = line.getAs[String]("station")
        val avg = line.getAs[Double]("_c1")
        val stddev = line.getAs[Double]("_c2")
        reAvg(station) = reAvg(station) + avg
        reStddev(station) = reStddev(station) + stddev
      })
      println("loop num:" + a)
      a = a + 1
    }
    // Step 6. Divide each quantity by 1000 to get the average and display the result
    println("|Station\t|avg\t\t|stddev|")
    println("+--------------+------------------+-------------------+")
    reAvg.keys.foreach { i =>
      println("", i, "\t\t", reAvg(i) / resampleTimes, "\t\t", reStddev(i) / resampleTimes)
    }
    //    reAvg.keys.foreach { i =>
    //      print("Rank = " + i)
    //      println(" Mean Value = " + (reAvg(i) / resampleTimes))
    //    }
    //    println("----------------------------------------")
    //    reStddev.keys.foreach { i =>
    //      print("Key = " + i)
    //      println(" Variance Value = " + (reStddev(i) / resampleTimes))
    //    }
  }
}
