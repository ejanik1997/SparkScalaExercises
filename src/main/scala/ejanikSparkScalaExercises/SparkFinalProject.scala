package ejanikSparkScalaExercises

import org.apache.spark.sql.types.{FloatType, LongType, StringType, StructType}
import org.apache.log4j._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions._


object SparkFinalProject {

  private case class WarehousePositions(positionID: Long, warehouse: String, product: String, eventTime: Timestamp)

  private case class Amounts(positionID: Long, amount: Float, eventTime: Timestamp)

  private val warehouseSchemaStructType = new StructType()
    .add("positionID", LongType, nullable = true)
    .add("warehouse", StringType, nullable = true)
    .add("product", StringType, nullable = true)
    .add("eventTime", LongType, nullable = true) // TimestampType returns null if used here

  private val amountsSchemaStructType = new StructType()
    .add("positionID", LongType, nullable = true)
    .add("amount", FloatType, nullable = true) //Both suggested DecimalType and NumericType return null
    .add("eventTime", LongType, nullable = true)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("SparkFinalProject")
      .master("local[*]")
      .getOrCreate()

    getWarehouseOverview(spark)

    spark.stop()
  }

  private def getWarehouseOverview(spark: SparkSession): Unit = {
    val warehousesDS: Dataset[WarehousePositions] = getWarehouseDS(spark)

    val amountsDS: Dataset[Amounts] = getAmountsDS(spark)

    val currentEventTimesDF: DataFrame = getCurrentEventTimesDF(warehousesDS, amountsDS)

    val currentAmountsDF: DataFrame = getCurrentAmounts(spark, amountsDS, currentEventTimesDF)

    val amountsMaxMinAvgDF: DataFrame = getAmountsMaxMinAvg(warehousesDS, amountsDS)

    showFormattedCurrentAmounts(currentAmountsDF)

    showFormattedAmountsMaxMinAvg(amountsMaxMinAvgDF)
  }

  private def showFormattedAmountsMaxMinAvg(amountsMaxMinAvgDF: DataFrame): Unit = {
    amountsMaxMinAvgDF
      .select("warehouse", "product", "maxAmount", "minAmount", "avgAmount")
      .sort("warehouse", "product")
      .show()
  }

  private def showFormattedCurrentAmounts(currentAmountsDF: DataFrame): Unit = {
    currentAmountsDF
      .select("a.positionID", "warehouse", "product", "amount")
      .show()
  }

  private def getAmountsMaxMinAvg(warehousesDS: Dataset[WarehousePositions], amountsDS: Dataset[Amounts]): DataFrame = {
    warehousesDS
      .join(amountsDS, "positionID")
      .groupBy("positionID", "warehouse", "product")
      .agg(
        max("amount").as("maxAmount"),
        min("amount").as("minAmount"),
        avg("amount").as("avgAmount")
      )
  }

  private def getCurrentAmounts(spark: SparkSession, amountsDS: Dataset[Amounts], currentEventTimesDF: DataFrame): DataFrame = {
    import spark.implicits._
    amountsDS.as("a")
      .join(currentEventTimesDF.as("cet"),
        $"a.eventTime" <=> $"cet.max(amountEventTime)" &&
          $"cet.positionID" <=> $"a.positionID",
        "inner")
      .sort("a.positionID")
  }

  private def getCurrentEventTimesDF(warehousesDS: Dataset[WarehousePositions], amounts: Dataset[Amounts]): DataFrame = {
    warehousesDS
      .select("positionID", "warehouse", "product")
      .join(amounts
        .withColumnRenamed("eventTime", "amountEventTime")
        .groupBy("positionID")
        .max("amountEventTime"), "positionID")
      .sort("positionID")
  }

  private def getAmountsDS(spark: SparkSession): Dataset[Amounts] = {
    import spark.implicits._
    spark
      .read
      .schema(amountsSchemaStructType)
      .option("sep", ", ")
      .csv("data/SparkFinalProject_ListOfAmounts.csv")
      .as[Amounts]
  }

  private def getWarehouseDS(spark: SparkSession): Dataset[WarehousePositions] = {
    import spark.implicits._
    spark
      .read
      .schema(warehouseSchemaStructType)
      .option("sep", ", ")
      .csv("data/SparkFinalProject_WarehousePositions.csv")
      .as[WarehousePositions]
  }
}
