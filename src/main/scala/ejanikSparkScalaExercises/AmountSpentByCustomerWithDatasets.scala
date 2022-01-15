package ejanikSparkScalaExercises
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AmountSpentByCustomerWithDatasets {

  case class Customer(ID: Int, someColumn: Int, orderPrice: Float)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("FriendsByAgeWithDatasets")
      .master("local[*]")
      .getOrCreate()

    val customerSchema = new StructType()
      .add("ID", IntegerType, nullable = true)
      .add("someColumn", IntegerType, nullable = true)
      .add("orderPrice", FloatType, nullable = true)

    import spark.implicits._

    val dataSet = spark.read
      .schema(customerSchema)
      .csv("data/customer-orders.csv")
      .as[Customer]

    val tempDS = dataSet.select($"ID", $"orderPrice")
    val amountSpentByCustomer = tempDS.groupBy($"ID").sum(
      "orderPrice"
    ).sort("ID")

    val res = amountSpentByCustomer.collect()

    for(result <- res) {
      val cus = result(0)
      val ord = result(1).asInstanceOf[Double]
      val formattedOrd = f"$ord%.2f dollars"
      println(s"Customer of ID $cus has spent $formattedOrd")
    }

  }

}
