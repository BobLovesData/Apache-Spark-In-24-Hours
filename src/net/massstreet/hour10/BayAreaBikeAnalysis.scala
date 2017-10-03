//Return the average number of bikes available by the hour for one week
//February 22 to February 28, for stations located in the San Jose area only.
//This is done with Dataframes and Spark SQL

package net.massstreet.hour10

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import java.text._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object BayAreaBikeAnalysis {
  

  
  def main(args: Array[String]) {
    
     Logger.getLogger("org").setLevel(Level.ERROR)
       
    // Use new SparkSession interface in Spark 2.0      
    val spark = SparkSession
    .builder()
    .appName("BayAreaBikeAnalysis")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .getOrCreate()
  
    //Load files into dataframes
    import spark.implicits._
    val statuses = spark.read.format("CSV").option("header","true").load("Data/status.csv")
    val stations = spark.read.format("CSV").option("header","true").load("Data/station.csv")  
    
    //Filter for one week don't have to cast date
    val statusesFilteredByDate = statuses.select("*").filter($"time".between("2014/02/22", "2014/02/28"))
    
    //Filter stations for San Jose
    //Note the crazy inconsistent syntax here.
    //There is more than one way to do this.
    val stationsFilteredByCity = stations.where($"city" === "San Jose")
    
    
    //Join Dataframes
    val stationsJoinedWithStatus = stationsFilteredByCity.join(statusesFilteredByDate, stationsFilteredByCity.col("id") === statusesFilteredByDate.col("station_id"))
    
    stationsJoinedWithStatus.select("name","bikes_available","time").createOrReplaceTempView("bikes")
    
    
    val stationsJoinedWithTimeColumn = stationsJoinedWithStatus.withColumn("time_only", functions.split(stationsJoinedWithStatus.col("time"), " ").getItem(1))
    
    val stationsJoinedWithHourColumn = stationsJoinedWithTimeColumn.withColumn("hour", functions.split(stationsJoinedWithTimeColumn.col("time_only"), ":").getItem(0))
  

    val aveargePerHourByStation =  stationsJoinedWithHourColumn.groupBy($"name",$"hour").agg(avg("bikes_available"))
    
    val topTenAveragesByStation = aveargePerHourByStation.sort(desc("avg(bikes_available)"))
    
    aveargePerHourByStation.show(100)
    topTenAveragesByStation.show(100)
    
    spark.stop()
  }   
    
      
      
}