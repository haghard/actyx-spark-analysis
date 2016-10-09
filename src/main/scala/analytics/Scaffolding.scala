package analytics

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import spray.json._

trait Scaffolding {
  /**

                                      +--------------------------+------------------+--------------------------+-----------------
                                      |2016-01-01:01:01:26:source|..-01:01:01:26:...|2016-01-01:01:01:29:source|..-01:01:01:29:..
              +-----------------------+--------------------------+------------------+--------------------------+-----------------
              |324234-34523:2016-01-01|                          |                  |                          |
              +-----------------------+--------------------------+------------------+--------------------------+-----------------


                         +------+------+------+--
                         |offset|offset|offset|
              +----------+------+------+------+--
              |readings:1| 135  | 245  |  442 |
              +----------+------+------+------+--

                         +------+------+------+--
                         |offset|offset|offset|
              +----------+------+------+------+--
              |readings:2| 132  | 275  |  472 |
              +----------+------+------+------+--

  */

  val keySpace = "aggregates"

  val maTable = "moving_average"
  val createMaTable = s"""CREATE TABLE IF NOT EXISTS ${keySpace}.${maTable} (
                | device_id varchar,
                | time_bucket varchar,
                | source varchar,
                | when timestamp,
                | startOffset bigint,
                | endOffset bigint,
                | value double,
                | PRIMARY KEY ((device_id, time_bucket), when)) WITH CLUSTERING ORDER BY (when DESC);""".stripMargin


  val offsetsTable = "kafka_offsets"
  val createOffsetsTable = s"""CREATE TABLE IF NOT EXISTS ${keySpace}.${offsetsTable} (
                | topic varchar,
                | partition int,
                | offset bigint,
                | PRIMARY KEY ((topic, partition)));""".stripMargin

  val InsertOffset = s"UPDATE ${keySpace}.${offsetsTable} SET offset = ? where topic = ? and partition = ?"
  val InsertReading = s"INSERT INTO ${keySpace}.${maTable}(device_id, time_bucket, source, when, startOffset, endOffset, value) values (?,?,?,?,?,?,?)"

  val chDir = "checkpoint-ma"

  val timeBucketFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  def murmur2Partitioner(kafkaNumPartitions: Int) = new org.apache.spark.Partitioner {

    import org.apache.kafka.common.utils.Utils

    private def toPositive(number: Int) = number & 0x7fffffff

    override val numPartitions = kafkaNumPartitions

    override def getPartition(key: Any): Int =
      toPositive(Utils.murmur2(key.asInstanceOf[String].getBytes)) % numPartitions

    override val toString = "murmur2"
  }

  case class Reading(deviceId: String, current: Double, currentAlert: Double, when: ZonedDateTime, name: String,
                     `type`: String, state: String)


  implicit object ReadingJsonReader extends JsonReader[Reading] with DefaultJsonProtocol {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z")
    override def read(json: JsValue): Reading =
      json.asJsObject.getFields("device_id", "name", "timestamp", "state", "current_alert", "type", "current") match {
        case Seq(JsString(id), JsString(name), JsString(timestamp), JsString(state), JsNumber(currentAlert), JsString(type0), JsNumber(current)) =>
          new Reading(id, current.toDouble, currentAlert.toDouble, ZonedDateTime.parse(timestamp, formatter), name, type0, state)
        case _ => deserializationError("Reading deserializationError")
      }
  }
}
