package net.massstreet.hour12

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.io.Source._


object AverageWordLength {
  
 

  
  
  def main(args: Array[String]) {
     
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val stopWordsURI = "https://s3.amazonaws.com/sty-spark/stopwords/stop-word-list.csv"
   
    val sc = new SparkContext("local[*]","Average-Word-Length")
    
    val input = fromURL(stopWordsURI).mkString
    
    val stopWords = input.split(",")
   
    
    val stopWordsBroadcast = sc.broadcast(stopWords)
    
    
    val wordCount = sc.longAccumulator("Word Count")
    
    val totalLength = sc.longAccumulator("Total Length")
    
    
    wordCount.add(1)
    
   
  }
    
  
     
     
    
  
}