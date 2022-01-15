package ejanikSparkScalaExercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object AmountSpentByCustomer {

  def parseLine(line : String): (Int, Float) = {
    val fields = line.split(",")
    val customerIds = fields(0).toInt
    val orderPrice = fields(2).toFloat
    (customerIds, orderPrice)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "AmountSpentByCustomer")
    val lines = sc.textFile("data/customer-orders.csv")
    //entering the local cluster
    val rdd = lines.map(parseLine)
    val totalsByCustomer = rdd.reduceByKey((x,y) => x+y)
    val totalsSortedById = totalsByCustomer.sortByKey()
    val results = totalsSortedById.collect()
    //back to the driver program
    //results.foreach(println) for some reason gives floats of different precisions
    for(result <- results){
      val customerId = result._1
      val totalAmountSpent = f"${result._2}%.2f"
      println(s"Customer $customerId spent $totalAmountSpent dollars")
    }

  }
}
