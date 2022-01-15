import ejanikSparkScalaExercises.SparkFinalProjectV2
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class SparkFinalProjectV2Test extends FunSuite with BeforeAndAfterEach with DatasetComparer {
  private val master = "local"
  private val appName = "ReadFileTest"
  private val showTopUsersSchema = List(
    StructField("USER_ID", IntegerType, nullable = true),
    StructField("FIRST_NAME", StringType, nullable = true),
    StructField("LAST_NAME", StringType, nullable = true),
    StructField("MESSAGE_ID", IntegerType, nullable = true),
    StructField("TEXT", StringType, nullable = true),
    StructField("TOTAL_NUMBER_RETWEETS", IntegerType, nullable = true))

  var spark: SparkSession = _

  override def beforeEach(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark = new sql.SparkSession.Builder().appName(appName).master(master).getOrCreate()
  }

  override def afterEach(): Unit = {
    spark.stop()
  }

  def getTestData(testNumber: String): (DataFrame, DataFrame, DataFrame, DataFrame) = {
    val readAvro = spark.read.format("avro")
    val userDirDF: DataFrame = readAvro.load(s"data/avro-data-tests/showTopUsers_$testNumber/UserDir.avro")
    val messageDirDF: DataFrame = readAvro.load(s"data/avro-data-tests/showTopUsers_$testNumber/MessageDir.avro")
    val messageDF: DataFrame = readAvro.load(s"data/avro-data-tests/showTopUsers_$testNumber/Message.avro")
    val retweetDF: DataFrame = readAvro.load(s"data/avro-data-tests/showTopUsers_$testNumber/Retweet.avro")

    (userDirDF, messageDirDF, messageDF, retweetDF)
  }


  test("check for two first wave retweets and two second wave retweets") {
    /*
      OP                 First wave                Second wave
      User 1 posts 11 -> user 2 retweets 1's 11 -> user 4 retweets 2's 11
                                                 -> user 5 retweets 2's 11
                      -> user 3 retweets 1's 11
     */
    val testNumber = "1"
    val (userDirDF, messageDirDF, messageDF, retweetDF) = getTestData(testNumber)
    val someData = Seq(
      Row(1, "Robert", "Smith", 11, "First user1 tweet", 4)
    )
    val expected = spark.createDataFrame(spark.sparkContext.parallelize(someData), StructType(showTopUsersSchema))
    val result = SparkFinalProjectV2.getTopUsers(spark, userDirDF, messageDirDF, messageDF, retweetDF)
    assertSmallDatasetEquality(result, expected, ignoreNullable = true, orderedComparison = false, ignoreColumnNames = true)
  }

  test("check if ignores third+ wave retweets, only counting first and second") {
    /*
      OP                 First wave                Second wave
      User 1 posts 11 -> user 2 retweets 1's 11 -> user 3 retweets 2's 11 -> user 4 retweets 3's 11 -> user 5 retweets 4's 11
     */
    val testNumber = "2"
    val (userDirDF, messageDirDF, messageDF, retweetDF) = getTestData(testNumber)
    val someData = Seq(
      Row(1, "Robert", "Smith", 11, "First user1 tweet", 2)
    )
    val expected = spark.createDataFrame(spark.sparkContext.parallelize(someData), StructType(showTopUsersSchema))
    val result = SparkFinalProjectV2.getTopUsers(spark, userDirDF, messageDirDF, messageDF, retweetDF)

    assertSmallDatasetEquality(result, expected, ignoreNullable = true, orderedComparison = false, ignoreColumnNames = true)
  }

  test("check if manages user with multiple messages retweets correctly") {
    /*
      OP                 First wave                Second wave
      User 1 posts 11 -> user 2 retweets 1's 11 -> user 3 retweets 2's 11
                                                -> user 4 retweets 2's 11

      User 1 posts 14 -> user 2 retweets 1's 14
                      -> user 3 retweets 1's 14 -> user 4 retweets 3's 14
                                                -> user 5 retweets 3's 14
     */
    val testNumber = "3"
    val (userDirDF, messageDirDF, messageDF, retweetDF) = getTestData(testNumber)
    val someData = Seq(
      Row(1, "Robert", "Smith", 11, "First user1 tweet", 3),
      Row(1, "Robert", "Smith", 14, "Second user1 tweet", 4)
    )
    val expected = spark.createDataFrame(spark.sparkContext.parallelize(someData), StructType(showTopUsersSchema))
    val result = SparkFinalProjectV2.getTopUsers(spark, userDirDF, messageDirDF, messageDF, retweetDF)

    assertSmallDatasetEquality(result, expected, ignoreNullable = true, orderedComparison = false, ignoreColumnNames = true)
  }

  test("check if manages multiple users with multiple retweets correctly while omitting third+ wave retweets") {
    /*
      OP                 First wave                Second wave
      User 1 posts 11 -> user 2 retweets 1's 11 -> user 3 retweets 2's 11 -> user 4 retweets 3's 11

      User 1 posts 14 -> user 2 retweets 1's 14 -> user 3 retweets 2's 14 -> user 4 retweets 3's 14 > user 5 retweets 4's 14


      User 3 posts 13 -> user 1 retweets 3's 13
                      -> user 2 retweets 3's 13 -> user 4 retweets 2's 14 -> user 5 retweets 4's 14

      User 3 posts 15 -> user 5 retweets 3's 15 -> user 4 retweets 5's 15 -> user 2 retweets 4's 15
     */

    val testNumber = "4"
    val (userDirDF, messageDirDF, messageDF, retweetDF) = getTestData(testNumber)
    val someData = Seq(
      Row(1, "Robert", "Smith", 11, "First user1 tweet", 2),
      Row(1, "Robert", "Smith", 14, "Second user1 tweet", 2),
      Row(3, "Alex", "Jones", 13, "First user3 tweet", 3),
      Row(3, "Alex", "Jones", 15, "Second user3 tweet", 2)
    )
    val expected = spark.createDataFrame(spark.sparkContext.parallelize(someData), StructType(showTopUsersSchema))
    val result = SparkFinalProjectV2.getTopUsers(spark, userDirDF, messageDirDF, messageDF, retweetDF)

    assertSmallDatasetEquality(result, expected, ignoreNullable = true, orderedComparison = false, ignoreColumnNames = true)
  }

}
