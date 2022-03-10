package com.scala-bill-calculator

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Source
import org.apache.spark.sql.types.IntegerType

import org.apache.spark.sql.functions._

object ValidateBillLog {
  
  // Each line from Bill log has this schema
  final case class PhoneCall(duration: String, phone: String)
  
  //In this function I validate the Bill Log
  def validateLog(S: String) : String = {
  var result = "ERROR: Validation error"
 // New SparkSession
    val spark = SparkSession
      .builder
      .appName("PhoneBillCalculation")
      .master("local[*]")//local standalone it must be change for paralellize in through cluster
      .getOrCreate()
    
   // Distribute S variable through the nodes
   val distData = spark.sparkContext.parallelize(List(S))
             
   // Parse S string into RDD with duration, phone columns using PhoneCall case class
   val parsedBillRDD = distData.flatMap(row =>row.split("\n")).map(line => PhoneCall(line.split(",")(0), line .split(",")(1)))
   
   // Convert RDD into a DataSet to use sparkSQL in order to optimize transformations
   import spark.implicits._
   val phoneBillDS = parsedBillRDD.toDS()
   
   //check N is in range [1...100]
   var N = phoneBillDS.count()
   
   // N range ok
   if(N>0&&N<=100){     
    //check phone format
    val phoneBillPhoneFormattedDS=phoneBillDS.withColumn("_tmp", split($"phone", "\\-")).select(
     $"_tmp".getItem(0).cast(IntegerType).as("phone_1"),
     length($"_tmp".getItem(0).cast(IntegerType)).as("length_1"),
     $"_tmp".getItem(1).as("phone_2"),
     length($"_tmp".getItem(1)).as("length_2"),
     $"_tmp".getItem(2).as("phone_3"),
     length($"_tmp".getItem(2)).as("length_3"),
    )
     
    val phoneErrorDS = phoneBillPhoneFormattedDS.where((col("length_1")!==3)||(col("length_2")!==3)||(col("length_3")!==3))
    // Phone format ok
    if(phoneErrorDS.count()==0){
      //check duration format
      val phoneBillDurationFormattedDS=phoneBillDS.withColumn("_tmp", split($"duration", "\\:")).select(
       $"_tmp".getItem(0).as("duration_1"),
       length($"_tmp".getItem(0)).as("length_1"),
       $"_tmp".getItem(1).as("duration_2"),
       length($"_tmp".getItem(1)).as("length_2"),
       $"_tmp".getItem(2).as("duration_3"),
       length($"_tmp".getItem(2)).as("length_3"),
      )
             
      val durationErrorDS = phoneBillDurationFormattedDS.where((col("length_1")!==2)||(col("length_2")!==2)||(col("length_3")!==2))
      
      // Duration format ok
      if(durationErrorDS.count()==0){
        result = "Validation ok"
      }
      else{
        // Duration format error
        result = "ERROR: Phone numbers must have this format hh:mm:ss;" 
      }
    }
    else{
      // Phone format error
      result = "ERROR: Phone numbers must have this format nnn-nnn-nnn and can't start by zero;" 
    }    
   }
   else{
     // N range error
     result = "ERROR: Bill log N value must be in [1..100] range;" 
   }   

   result
}

 /** Main function where the action happens */
  def main(args: Array[String]) {
 
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // mutable variable phone bill logs
    var S : String = "00:01:07,400-234-090\n00:05:01,701-080-080\n00:05:00,400-234-090"
        
    // Call to validate function and display result
    println(validateLog(S))      
  }  
}