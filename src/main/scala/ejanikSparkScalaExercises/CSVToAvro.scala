package ejanikSparkScalaExercises

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.log4j._
import org.apache.spark.sql.{AnalysisException, SparkSession}

object CSVToAvro {

  case class UserDir(USER_ID: Int, FIRST_NAME: String, LAST_NAME: String)

  case class MessageDir(MESSAGE_ID: Int, TEXT: String)

  case class Message(USER_ID: Int, MESSAGE_ID: Int)

  case class Retweet(USER_ID: Int, SUBSCRIBER_ID: Int, MESSAGE_ID: Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("FromTextToAvro")
      .master("local[*]")
      .getOrCreate()

    val userDirSchema = new StructType()
      .add("USER_ID", IntegerType, nullable = true)
      .add("FIRST_NAME", StringType, nullable = true)
      .add("LAST_NAME", StringType, nullable = true)

    val messageDirSchema = new StructType()
      .add("MESSAGE_ID", IntegerType, nullable = true)
      .add("TEXT", StringType, nullable = true)

    val messageSchema = new StructType()
      .add("USER_ID", IntegerType, nullable = true)
      .add("MESSAGE_ID", IntegerType, nullable = true)

    val retweetSchema = new StructType()
      .add("USER_ID", IntegerType, nullable = true)
      .add("SUBSCRIBER_ID", IntegerType, nullable = true)
      .add("MESSAGE_ID", IntegerType, nullable = true)

    import spark.implicits._
    val userDirTable = spark.read
      .schema(userDirSchema)
      .option("sep", ",")
      .csv("data/v2_user_dir.csv")
      .as[UserDir]

    val messageDirTable = spark.read
      .schema(messageDirSchema)
      .option("sep", ",")
      .csv("data/v2_message_dir.csv")
      .as[MessageDir]

    val messageTable = spark.read
      .schema(messageSchema)
      .option("sep", ",")
      .csv("data/v2_message.csv")
      .as[Message]

    val retweetTable = spark.read
      .schema(retweetSchema)
      .option("sep", ",")
      .csv("data/v2_retweet.csv")
      .as[Retweet]

    //userDirTable.show()
    //messageDirTable.show()
    //messageTable.show()
    //retweetTable.show()

    val tables = List(userDirTable, messageDirTable, messageTable, retweetTable)
    val directories = List("UserDir", "MessageDir", "Message", "Retweet")
    val zipped = directories.zip(tables)

    for (element <- zipped) {
      try {
        element._2.write
          .format("avro")
          .save(s"data/avro-data/${element._1}.avro")
        println(s"Saved avro file at data/avro-data/${element._1}.avro")
      } catch {
        case e: AnalysisException => println(s"$e\nFile already exists at ${element._1}")
        case e: NoSuchElementException => println(s"$e\nNo such element exception, file not saved")
      }
    }
    spark.stop()
  }

}
