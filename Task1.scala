package SparkAssignment47

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType,NumericType,IntegerType} // Import dependencies
import scala.io.Source


object Task1 {
  
  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g") //Configuration
    val sc=new SparkContext(conf) // Create Spark Context with Configuration
    val spark = SparkSession //Create Spark Session
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    
    //1. What are the total number of gold medal winners every year
    
    val SportsData = sc.textFile("Sports_data.txt") // Read input Data 
    //Create the definition for the Schema for the Text File
    val schemaString = "firstname:string,lastname:string,sports:string,medal_type:string,age:string,year:string,country:string"
    val schema = StructType(schemaString.split(",").map(x=>  //Identify and apply the Datatype for the fields
                  StructField(x.split(":")(0),if(x.split(":")(1).equals("string"))StringType else IntegerType, true)))
    val rowRDD = SportsData.map(_.split(",")).map(r=>Row(r(0),r(1),r(2),r(3),r(4),r(5),r(6))) // Create RDD from the SportsData read from text file
    val SportsDataDF = spark.createDataFrame(rowRDD, schema) //Create a Dataframe
    SportsDataDF.createOrReplaceTempView("SportsData") //Create Temp View
    // Write SQL command directly for the Temp view created
    val resultDF = spark.sql("SELECT year, COUNT(*) FROM SportsData WHERE medal_type = 'gold' GROUP BY year") 
    resultDF.show() //Show the output
    
    val resultDF2 = spark.sql("SELECT sports, COUNT(*) FROM SportsData WHERE medal_type = 'silver' and country ='USA' GROUP BY sports")
    resultDF2.show()
  }
  
}