package ejanikSparkScalaExercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

import scala.util.{Failure, Success, Try}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object SparkFinalProjectV2 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .appName("SparkFinalProjectV2")
      .master("local[*]")
      .getOrCreate()

    val (userDirDF: DataFrame, messageDirDF: DataFrame, messageDF: DataFrame, retweetDF: DataFrame) = getData(spark)
    val topUsers: DataFrame = getTopUsers(spark, userDirDF, messageDirDF, messageDF, retweetDF)
    topUsers.show()


    spark.stop()

  }

  private def getData(spark: SparkSession): Any = {
    Try(tryGetTwitterDataFrames(spark)) match {
      case s: Success[_] =>
        s.value
      case e: Failure[_] =>
        println(e)
        spark.stop()
        sys.exit()
    }
  }

  def getTopUsers(spark: SparkSession, userDirDF: DataFrame, messageDirDF: DataFrame, messageDF: DataFrame, retweetDF: DataFrame): DataFrame = {

    val firstWaveRetweetsDF: DataFrame = getFirstWaveRetweets(spark, retweetDF, messageDF)
    //firstWaveRetweetsDF.show()

    val secondWaveRetweetsDF: DataFrame = getSecondWaveRetweets(spark, retweetDF, firstWaveRetweetsDF)
    //secondWaveRetweetsDF.show()

    val secondWaveRetweetsFormattedDF: DataFrame = getSecondWaveRetweetsFormatted(firstWaveRetweetsDF, secondWaveRetweetsDF)
    //secondWaveRetweetsFormattedDF.show()

    val retweetsCount: DataFrame = getRetweetsCount(secondWaveRetweetsFormattedDF)
    //retweetsCount.show()

    val formattedOutput: DataFrame = getFormattedOutput(userDirDF, messageDirDF, retweetsCount)

    //formattedOutput.printSchema()
    formattedOutput
  }

  private def getFormattedOutput(userDirDF: DataFrame, messageDirDF: DataFrame, retweetsCount: DataFrame): DataFrame = {
    retweetsCount
      .join(userDirDF, "USER_ID")
      .join(messageDirDF, "MESSAGE_ID")
      .select("USER_ID", "FIRST_NAME", "LAST_NAME", "MESSAGE_ID", "TEXT", "TOTAL_NUMBER_RETWEETS")
      .sort(desc("TOTAL_NUMBER_RETWEETS"))
      .withColumn("TOTAL_NUMBER_RETWEETS", col("TOTAL_NUMBER_RETWEETS").cast(IntegerType))
  }

  private def getRetweetsCount(secondWaveRetweetsFormattedDF: DataFrame): DataFrame = {
    secondWaveRetweetsFormattedDF
      .groupBy("MESSAGE_ID", "USER_ID")
      .agg(
        count("MESSAGE_ID").as("TOTAL_NUMBER_RETWEETS")
      )
      .select("USER_ID", "MESSAGE_ID", "TOTAL_NUMBER_RETWEETS")
  }

  private def getSecondWaveRetweetsFormatted(firstWaveRetweetsDF: DataFrame, secondWaveRetweetsDF: DataFrame): DataFrame = {
    firstWaveRetweetsDF
      .union(secondWaveRetweetsDF
        .drop("USER_ID", "FIRST_WAVE_SUBSCRIBER_ID", "FIRST_WAVE_MESSAGE_ID")
        .withColumnRenamed("FIRST_WAVE_USER_ID", "USER_ID"))
  }

  private def getSecondWaveRetweets(spark: SparkSession, retweetDF: DataFrame, firstWaveRetweetsDF: DataFrame): DataFrame = {
    import spark.implicits._
    firstWaveRetweetsDF
      .withColumnRenamed("USER_ID", "FIRST_WAVE_USER_ID")
      .withColumnRenamed("SUBSCRIBER_ID", "FIRST_WAVE_SUBSCRIBER_ID")
      .withColumnRenamed("MESSAGE_ID", "FIRST_WAVE_MESSAGE_ID")
      .join(retweetDF)
      .where($"FIRST_WAVE_SUBSCRIBER_ID" === $"USER_ID" && $"FIRST_WAVE_MESSAGE_ID" === $"MESSAGE_ID")
  }

  private def getFirstWaveRetweets(spark: SparkSession, retweet: DataFrame, messageDF: DataFrame): DataFrame = {
    import spark.implicits._
    messageDF
      .join(retweet
        .withColumnRenamed("MESSAGE_ID", "RETWEETS_MESSAGE_ID"),
        "USER_ID")
      .where($"MESSAGE_ID" === $"RETWEETS_MESSAGE_ID")
      .select("USER_ID", "SUBSCRIBER_ID", "MESSAGE_ID")
  }

  private def tryGetTwitterDataFrames(spark: SparkSession): (DataFrame, DataFrame, DataFrame, DataFrame) = {
    val readAvro = spark.read.format("avro")
    try {
      val userDirDF: DataFrame = readAvro.load("data/avro-data/UserDir.avro")
      val messageDirDF: DataFrame = readAvro.load("data/avro-data/MessageDir.avro")
      val messageDF: DataFrame = readAvro.load("data/avro-data/Message.avro")
      val retweetDF: DataFrame = readAvro.load("data/avro-data/Retweet.avro")
      (userDirDF, messageDirDF, messageDF, retweetDF)
    } catch {
      case ex: AnalysisException =>
        //println("The path is incorrect or the avro files are missing")
        throw ex
      case unknownEx: Exception =>
        //println(s"Unknown exception: $unknownEx,occured when reading avro files")
        throw unknownEx
    }
  }
}
