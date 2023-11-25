package scalatest

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import java.util.Date

object scalaobject {
  def main(args: Array[String]) {
    
    val logFile = "C:/Users/JohannAlbrecht/Desktop/uberr" // Should be some file on your system
    
    val conf = new SparkConf().setAppName("Scala").setMaster("local")
    val sc = new SparkContext(conf)
    
    val dataset = sc.textFile(logFile)
    System.out.println("Scala Running...")    
    
    
    
    val header = dataset.first()
    val format = new java.text.SimpleDateFormat("MM/dd/yyyy")
    var days =Array("Sun","Mon","Tue","Wed","Thu","Fri","Sat")
    val eliminate = dataset.filter(line => line != header)
    val split = eliminate.map(line => line.split(",")).map { x => (x(0),format.parse(x(1)),x(3)) }
    val combine = split.map(x => (x._1+" "+days(x._2.getDay),x._3.toInt))
    val arrange = combine.reduceByKey(_+_).map(item => item.swap).sortByKey(false).collect.foreach(println)
    System.out.println("Trips Counted")
    
    
  }
}