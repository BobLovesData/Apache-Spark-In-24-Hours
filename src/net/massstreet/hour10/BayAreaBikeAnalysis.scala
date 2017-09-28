//Return the average number of bikes available by the hour for one week
//February 22 to February 28, for stations located in the San Jose area only.

package net.massstreet.hour10

import org.apache.spark.SparkContext._
import org.apache.spark._

object BayAreaBikeAnalysis {
  
  case class Station(ID:Int, name:String, lat:Double, long:Double, dockCount:Int, city:String, installationDate:String)
  case class Status(station_id:Int, bikesAvailable:Int, docksAvailable:Int, time:String)
  
  def extractStations(line: String): Station = {
    val fields = line.split(",",-1)
    val station:Station = Station(fields(0).toInt, fields(1), fields(2).toDouble, fields(3).toDouble, fields(4).toInt, fields(5), fields(6))
    return station
  }
  
  def extractStatus(line: String): Status = {
    val fields = line.split(",",-1)
    val status:Status = Status(fields(0).toInt, fields(1).toInt, fields(2).toInt, fields(3))
    return status
  }
  
  def main(args: Array[String]){
    
  }