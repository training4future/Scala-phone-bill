package com.scala-bill-calculator

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import scala.io.Source
import org.apache.spark.sql.types.IntegerType

import org.apache.spark.sql.functions._

//Scala Object 
object PhoneBillCalculation {
  
  // Each line from Bill log has this schema 
  final case class PhoneCall(duration: String, phone: String)
    
  //In this function I calculate the Bill and return cents
  def calculateBill(S: String):Int = {
    //initialize vars
    var result= 0
    val oneMinute = 60
    val fiveMinutes = 5*oneMinute
    val oneHour = 60*oneMinute 
    val cost1 = 3
    val cost2 = 150
    
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
        
   // Split duration string into hours minutes seconds integers and add it to dataset
   import org.apache.spark.sql.functions.split
    
   // Format duration
   val phoneBillDurationFormattedDS=phoneBillDS.withColumn("_tmp", split($"duration", "\\:")).select(
     $"_tmp".getItem(0).cast(IntegerType).as("hours"),
     $"_tmp".getItem(1).cast(IntegerType).as("minutes"),
     $"_tmp".getItem(2).cast(IntegerType).as("seconds"),
     $"phone".as("phone")
   )
   
   // Format phone number
   val phoneBillPhoneFormattedDS = phoneBillDurationFormattedDS.withColumn("phone",regexp_replace(col("phone"), "\\-", "")).select(
     $"hours".as("hours"),
     $"minutes".as("minutes"),
     $"seconds".as("seconds"),
     ($"hours"*oneHour+$"minutes"*oneMinute+$"seconds").as("total_duration"),
     $"phone".cast(IntegerType).as("phone")
   )
   
   // Sum duration by phone number and orderby total duration and phone number first result will be the phone number that has the longest total duration
   val durationByPhone = phoneBillPhoneFormattedDS.groupBy("phone").sum("total_duration").orderBy(desc("sum(total_duration)"),desc("phone"))
   
   // Get top duration phone number 
   var longestDurationNumber = durationByPhone.select($"phone").first()(0)
   
   val finalPhoneBill = phoneBillPhoneFormattedDS.filter(phoneBillPhoneFormattedDS("phone")!==longestDurationNumber)
   
   //calculate bill from calls that are shorter than 5 minutes avoiding longestDurationNumber and sum it to lessThan5Bill var
   val lessThan5Bill = finalPhoneBill.where(col("total_duration")<fiveMinutes).agg(sum("total_duration").as("less_than_five")).select($"less_than_five".cast(IntegerType)*cost1).first().get(0).asInstanceOf[Int]
   
   //calculate bill from calls that are longer or equal than 5 minutes avoiding longestDurationNumber and sum it to atLeast5BillDS var    
   val atLeast5BillDS = finalPhoneBill.where(col("total_duration")>=fiveMinutes).select(
       $"hours".as("hours"),
       $"minutes".as("minutes"),
       $"seconds".as("seconds"),
       $"phone".as("phone"),
       when($"seconds">0,($"minutes"+1)*cost2).otherwise(($"minutes")*cost2).as("cost")
   )

   val atLeast5Bill=atLeast5BillDS.agg(sum("cost").cast(IntegerType)).first().get(0).asInstanceOf[Int]
     
   // result is the sum of both costs
   result = lessThan5Bill + atLeast5Bill
   
   result
  }
     
  /** Main function where the action happens */
  def main(args: Array[String]) {
 
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // mutable variable phone bill logs
    var S : String = "00:01:07,400-234-090\n00:05:01,701-080-080\n00:05:00,400-234-090"
        
    // Call to calculate function and display result
    println("Phone Bill is " + calculateBill(S) + " cents")
    
  }  
}