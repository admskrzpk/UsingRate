import org.apache.spark.sql.streaming.Trigger

import java.sql.Timestamp

object MyRate extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .master("local[2]")
    .getOrCreate()
  import spark.implicits._

  val data = spark
    .readStream
    .format("rate")
    .load
    .as[(Timestamp, Long)]

  import org.apache.spark.sql.ForeachWriter
  import java.io._

  val directory = args(0)
  var pw: PrintWriter = _

  val writer = new ForeachWriter[(Timestamp, Long)] {
    override def open(partitionId: Long, epochId: Long): Boolean = {
      println(s">>> open ($partitionId, $epochId)")
      try {
        pw = new PrintWriter(new File(directory + s"/batch_${partitionId}_$epochId"))
        true
      } catch {
        case _ => println("BLAD")
          false
      }
    }

    override def process(value: (Timestamp, Long)): Unit = {
      println(s">>> process ($value)")
      pw.write(s"timestamp: ${value._1} => value: ${value._2}\n")
    }

    override def close(errorOrNull: Throwable): Unit = {
      pw.close()
      println(s"$errorOrNull")
    }
  }

  val output: Unit = data
    .writeStream
    .foreach(writer)
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .option("checkpointLocation", args(1))
    .queryName("hello")
    .start
    .awaitTermination
}