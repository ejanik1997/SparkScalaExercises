package ejanikSparkScalaExercises


import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object LeastPopularSuperheroes {

  case class SuperHeroNames(id: Int, name: String)
  case class SuperHero(value: String)

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("MostPopularSuperhero")
      .master("local[*]")
      .getOrCreate()

    val superHeroNamesSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    import spark.implicits._
    val names = spark.read
      .schema(superHeroNamesSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNames]

    val lines = spark.read
      .text("data/Marvel-graph.txt")
      .as[SuperHero]

    val connections = lines
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))
      .sort("connections")
      .limit(10) //returns a dataframe with initial 10 rows, instead of returning it as an array like head(10) does

    /* Bit of context: the point of the exercise was to limit the connections dataframe before joining it with names.
    *  The proposed way was to get minConnectionCount, then filter the connections with it and then join it with names,
    *  but I've found the limit() method in spark documentation, as I couldn't figure the suggested solution out.
    *  The previous commit was suboptimal, but now I've reached the optimal effect with less code.
    *
    * The difference in solutions is: I get first x rows, while the suggested one gets all the heroes of min appearances.
    * */

    val leastPopularWithNames = names
      .join(connections, "id")

    for(res <- leastPopularWithNames){
      println(s"${res(1)} is in bottom 10 popular superheroes with ${res(2)} co-appearances.")
    }

    spark.stop()
  }
}

