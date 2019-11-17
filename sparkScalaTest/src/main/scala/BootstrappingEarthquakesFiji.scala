import java.io._

import scala.io.Source
import scala.math.random
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

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

    val mtQuakes = mtcdata
      .map(p => Quakes(p(5), p(4)))
      .toDF
    mtQuakes.printSchema
    // Run SQL queries from the Spark DataFrame
    mtQuakes.registerTempTable("quakes")
    val stationQuakes = sqlContext.sql("SELECT station, sum(mag) / count(mag), stddev(mag) FROM quakes WHERE 1=1 group by station order by station ")
    stationQuakes.show()
    val sample = mtQuakes.sample(false, 0.25).map(p => Quakes(p(0).toString, p(1).toString)).toDF
    sample.cache()
    sample.printSchema
    sample.registerTempTable("sample")
    val sampleOut = sqlContext.sql("SELECT station, sum(mag) / count(mag), stddev(mag) FROM sample WHERE 1=1 group by station order by station ")
    sampleOut.show()
    // Step 5. Do 1000 times
    var a = 0
    //    sql("CREATE TABLE IF NOT EXISTS sumsData(station String, avg Double, stddev Double)")
    //    val sumSchema=StructType(List(StructField("station",StringType,true),StructField("avg",DoubleType,true),StructField("stddev",DoubleType,true)))
    val array = Array("", 0.0, 0.0)
    val sums = sc.parallelize(array)
    //    val sums = sc.emptyRDD[(String, Double, Double)]
    //    val sumsRowRDD=sums.map(u=>Row(u(0),u(1),u(2)))
    //    val sumsDf = sqlContext.createDataFrame(sumsRowRDD, sumSchema)
    //    sumsDf.registerTempTable("sumsData")
    while (a < 1) {
      //5a. Create a “resampledData”. All you need to do is take 100% of the sample with replacement.
      val resampledData = sample.sample(true, 1.00).map(p => Quakes(p(0).toString, p(1).toString)).toDF
      resampledData.printSchema
      //5b. Compute the mean mpg and variance for each c
      resampledData.registerTempTable("resampledData")
      val resampledDataOut = sqlContext.sql("SELECT station, sum(mag) / count(mag), stddev(mag) FROM resampledData WHERE 1=1 group by station order by station ")
      resampledDataOut.show()
      resampledDataOut.rdd.foreach(println)
      //5c. Keep adding the values in some running sum
      sums.union(resampledDataOut.rdd.map(p => (p(0).toString, p(1).toString.toDouble, p(2).toString.toDouble))).distinct()
      println("sums: start")
      sums.foreach(println)
      println("loop:" + a)
      //      resampledDataOut.write.insertInto("sumsData")
      a = a + 1
    }
    val sumSchema = StructType(List(StructField("station", StringType, true), StructField("avg", DoubleType, true), StructField("stddev", DoubleType, true)))
    val sumsRowRDD = sums.map(u => Row(u))
    val sumsDf = sqlContext.createDataFrame(sumsRowRDD, sumSchema)
    sumsDf.registerTempTable("sumsData")
    val sumsOut = sqlContext.sql("SELECT _c0, sum(_c1) / count(_c1), , sum(_c2) / count(_c2)  FROM sumsData WHERE 1=1 group by _c1 order by _c1 ")
    sumsOut.show()
    sumsOut.rdd.foreach(println)
    //    sums.foreach(println)
    //    // Create a Spark DataFrame
    //    val mtcars = mtcdata
    //      .map(p => Cars(p(0), p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11)))
    //      .toDF
    //    mtcars.printSchema
    //    mtcars.select("mpg").show(5)
    //    mtcars.filter(mtcars("mpg") < 18).show()
    //    // Aggregate data after grouping by columns
    //    import org.apache.spark.sql.functions._
    //    mtcars.groupBy("cyl").agg(avg("wt")).show()
    //    mtcars.groupBy("cyl").agg(count("wt")).sort($"count(wt)".desc).show()
    //    // Operate on columns
    //    mtcars.withColumn("wtTon", mtcars("wt") * 0.45).select(("car"),("wt"),("wtTon")).show(6)
    //    // Run SQL queries from the Spark DataFrame
    //    mtcars.registerTempTable("cars")
    //    val highgearcars = sqlContext.sql("SELECT car, gear FROM cars WHERE gear >= 5")
    //    highgearcars.show()
    //
    //    // DataFrame from Scala objects
    //    val info = List(("mike", 24), ("joe", 34), ("jack", 55))
    //    val infoRDD = sc.parallelize(info)
    //    val people = infoRDD.map(r => Person(r._1, r._2)).toDF()
    //    people.registerTempTable("people")
    //    val subDF = sqlContext.sql("select * from people where age > 30")
    //    subDF.show()
  }
}
