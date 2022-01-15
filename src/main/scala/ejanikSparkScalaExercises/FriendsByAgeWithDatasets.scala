package ejanikSparkScalaExercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object FriendsByAgeWithDatasets {

  case class Person(ID: Int, name: String, age: Int, friends: Int)

  def mapper(lines: String): Person = {
    val fields = lines.split(',')
    val person:Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    person
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("FriendsByAgeWithDatasets")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    /* Issue: the commented lines below cause error:  java.lang.NumberFormatException: For input string: "id"
      when .show() is used (as well as prntln(head))

    val lines = spark.sparkContext.textFile("data/fakefriends.csv")
    val people = lines.map(mapper).toDS.cache()
    people.printSchema() // this works all right
    people.show() // java.lang.NumberFormatException: For input string: "id"
                     ^ also, happens to the very first field in person class, omitting "id" doesn't help
     */

    //using different approach, comment the val below to check if needed
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]


    //useful thing found in spark datasets documentation
    val friendsByAge = people.groupBy("age").agg(Map(
      "friends" -> "avg"
    ))
    friendsByAge.show()
    spark.stop()
  }

}
