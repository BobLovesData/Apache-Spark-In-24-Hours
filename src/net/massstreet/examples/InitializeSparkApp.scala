package net.massstreet

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.log4j._

object InitializeSparkApp {
  
  
  
  /** Convert input data to (customerID, amountSpent) tuples */
  def extractCustomerPricePairs(line: String) = {
    val fields = line.split(",",-1) //-1 includes empty columns
    (fields(6).toString, fields(5).toFloat)
  }
 
  
     def main(args: Array[String]){
       
       Logger.getLogger("org").setLevel(Level.ERROR)
       
       val sc = new SparkContext("local[*]","First App")
       
       val data = sc.textFile("data/uber_data.csv")
       
       val mappedInput = data.map(extractCustomerPricePairs)
       
       val totalMilesByPurpose = mappedInput.reduceByKey((x,y) => (x + y))
       
       totalMilesByPurpose.foreach(println)
       
     }
  
}