package SparkAssignment47

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.udf //udf dependencies
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType,NumericType,IntegerType} // Import dependencies
import scala.io.Source
object Task2 {
  def main(args: Array[String]): Unit = {
    val conf:SparkConf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory","1g") //Configuration
    val sc=new SparkContext(conf) // Create Spark Context with Configuration
    val spark = SparkSession //Create Spark Session
    .builder()
    .appName("Spark SQL basic example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
    
 //1. Change firstname, lastname columns into
 //   Mr.first_two_letters_of_firstname<space>lastname
    
    val SportsData = sc.textFile("Sports_data.txt") // Read input Data 
    //Create the definition for the Schema for the Text File
    val schemaString = "firstname:string,lastname:string,sports:string,medal_type:string,age:string,year:string,country:string"
    val schema = StructType(schemaString.split(",").map(x=>  //Identify and apply the Datatype for the fields
                  StructField(x.split(":")(0),if(x.split(":")(1).equals("string"))StringType else IntegerType, true)))
    val rowRDD = SportsData.map(_.split(",")).map(r=>Row(r(0),r(1),r(2),r(3),r(4),r(5),r(6))) // Create RDD from the SportsData read from text file
    val SportsDataDF = spark.createDataFrame(rowRDD, schema) //Create a Dataframe
    SportsDataDF.createOrReplaceTempView("SportsData")
    //Create and udf for concatenating two string
    val Name = udf((firstname:String, lastname:String)=> "Mr.".concat(firstname.substring(0,2)).concat(" ")concat(lastname))
    spark.udf.register("Full_Name", Name) //Register the udf as Full_Name
    val fname = spark.sql("SELECT Full_Name(firstname, lastname) FROM SportsData").show() // User Registered Udf in SQL
    
    
    //Add a new column called ranking using udfs on dataframe, where : 
    //gold medalist, with age >= 32 are ranked as pro 
    //gold medalists, with age <= 31 are ranked amateur 
    //silver medalist, with age >= 32 are ranked as expert 
    //silver medalists, with age <= 31 are ranked rookie

    val Ranking = udf((medal:String,age:Int) => (medal,age) match
        {
      case (medal, age) if medal == "gold" && age >=32 => "Pro"
      case (medal, age) if medal == "gold" && age <=32 => "amateur"
      case (medal, age) if medal == "silver" && age >=32 => "expert"
      case (medal, age) if medal == "silver" && age <=32 => "rookie"
      
        })
    
    spark.udf.register("Ranks", Ranking)
    val RankingRDD = SportsDataDF.withColumn("Ranks",Ranking(SportsDataDF.col("medal_type"), SportsDataDF.col("age")))
    RankingRDD.show()
    
    
    
    
    
    
    
    
     
}
  
}