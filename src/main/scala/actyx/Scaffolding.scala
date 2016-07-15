package actyx

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import com.datastax.spark.connector.SomeColumns
import spray.json._

trait Scaffolding {
  val keySpace = "actyx"
  val maTable = "moving_average"
  val offsetsTable = "kafka_offsets"

  val sourceName = "spark-streaming-job"

  val chDir = "checkpoint-ma"

  val timeBucketFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val columns = SomeColumns("device_id", "time_bucket", "source", "when", "value")

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
