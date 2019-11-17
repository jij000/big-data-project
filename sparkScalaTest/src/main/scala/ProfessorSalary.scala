import java.io._
import scala.io.Source
import scala.math.random
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SQLContext}
import au.com.bytecode.opencsv.CSVParser

case class Professor(id: String, rank: String, discipline: String, yrs_since_phd: String, yrs_service: String, 
    sex: String,salary: String)
    
object ProfessorSalary extends App{
  override def  main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("local[*]");
    //val conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("local")
    val sc = new SparkContext(conf);
    sc.setLogLevel("WARN")
    
    val sqlContext= new org.apache.spark.sql.SQLContext(sc);
    import sqlContext._;
    import sqlContext.implicits._;
    
    val salariesCSV = sc.textFile("Salaries.csv").cache;
    val headerAndRows = salariesCSV.map(line => line.split(",").map(_.trim));
    val header = headerAndRows.first;
    val mtcdata = headerAndRows.filter(_(0) != header(0));
    val mtcars = mtcdata.map(p => Professor(p(0), p(1), p(2), p(3), p(4), p(5), p(6))).toDF;
    mtcars.printSchema;
    mtcars.select("salary").show(5);
    //mtcars.filter(mtcars("salary") < 102580).show();
    import org.apache.spark.sql.functions._
    val orginalData = mtcars.groupBy("rank").agg(avg("salary").as("avg"),var_pop("salary").as("vari"));
    val sampledata = mtcars.sample(false, 0.25);
    //sampledata.show();
    val meanProf = sampledata.groupBy("rank").agg(avg("salary").as("avg"),var_pop("salary").as("vari"));
    //val varianceProf = sampledata.groupBy("rank").agg(var_pop("salary")).collect();
    //
    
    //print(meanProf(0).get(0));
    
    import scala.collection.mutable.HashMap
    val reMean = new HashMap[String, Double].withDefault(k => 0)
    val reVariance = new HashMap[String, Double].withDefault(k => 0)
    val resampleTimes = 100
    for( a <- 1 to resampleTimes){
      val resampledData = sampledata.sample(true,1);
      val reProf = resampledData.groupBy("rank").agg(avg("salary").as("avg"),var_pop("salary").as("vari")).collect();
      for (b <- 0 to 2)
      {
        reMean(reProf(b).getString(0)) = reMean(reProf(b).getString(0)) + reProf(b).getDouble(1)
        reVariance(reProf(b).getString(0)) = reVariance(reProf(b).getString(0)) + reProf(b).getDouble(2)
      }
    }
    println("Original:");
    orginalData.show();
    println("Sample:");
    meanProf.show();
    println("ReSample:")
    reMean.keys.foreach{ i =>  
                           print( "Rank = " + i )
                           println(" Mean Value = " + (reMean(i)/resampleTimes) )}
    println("----------------------------------------")
    reVariance.keys.foreach{ i =>  
                           print( "Key = " + i )
                           println(" Variance Value = " + (reVariance(i)/resampleTimes) )}
                          
                           
    
    
    
    //mtcars.groupBy("rank").agg(var_pop("salary")).show();
    
    
    
  }
}