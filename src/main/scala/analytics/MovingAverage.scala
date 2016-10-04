package analytics

import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import java.time.{ZoneOffset, ZonedDateTime}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import spray.json._
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}


/**

  bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.11:1.6.1,datastax:spark-cassandra-connector:1.6.0-s_2.11 \
   --master spark://192.168.0.182:7077 \
   --conf spark.cassandra.connection.host=192.168.0.182,192.168.0.38 \
   --conf spark.cassandra.output.consistency.level=LOCAL_QUORUM \
   --total-executor-cores 4 \
   --executor-memory 1024MB \
   --class analytics.MovingAverage \
   ../moving-average.jar \
   client1 readings 4 192.168.0.182:2181 192.168.0.38:9092 15 4

 */
object MovingAverage extends Scaffolding {

  def setupSsc(conf: SparkConf, clientId: String, topic: String,
               kafkaNumPartitions: Int, zookeper: String, broker: String,
               streamingIntervalSec: Int, windowSize: Int,
               cc: CassandraConnector,
               offsets: Map[TopicAndPartition, Long]): StreamingContext = {

    //to store offsets in external db
    val kafkaParams: Map[String, String] = Map[String, String]("metadata.broker.list" -> broker, "zookeeper.connect" -> zookeper)

    //to store offsets in zookeeper db and periodically commits offset in the zookeper
    val kafkaParams0: Map[String, String] = Map[String, String](
      "client.id" -> clientId,
      "group.id" -> "spark-cluster",
      "metadata.broker.list" -> broker,
      "zookeeper.connect" -> zookeper,
      "auto.offset.reset" -> "largest", //smallest
      "zookeeper.session.timeout.ms" -> "1000",
      "zookeeper.sync.time.ms" -> "250",
      "auto.commit.interval.ms" -> "1000")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(streamingIntervalSec))
    ssc.checkpoint(chDir)

    //We store offsets in cassandra
    val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, (Double, Long))](
      ssc, kafkaParams, offsets,
      (mmd: MessageAndMetadata[String, String]) => {
        val obj = mmd.message().parseJson.convertTo[Reading]
        (obj.deviceId, (obj.current, 1l))
      }
    )

    var offsetRanges = Array[OffsetRange]()
    val window = directKafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      println("Offset ranges on the driver:" + offsetRanges.mkString("\n"))
      println(s"Number of kafka partitions before windowing: ${offsetRanges.size}")
      println(s"Number of spark partitions before windowing: ${rdd.partitions.size}")
      rdd
    }.reduceByKeyAndWindow(
      reduceFunc = { (a: (Double, Long), b: (Double, Long)) ⇒ (a._1 + b._1, a._2 + b._2) },
      invReduceFunc = { (acc: (Double, Long), evicted: (Double, Long)) ⇒ (acc._1 - evicted._1, acc._2 - evicted._2) },
      windowDuration = Seconds(streamingIntervalSec * windowSize), slideDuration = Seconds(streamingIntervalSec),
      partitioner = murmur2Partitioner(kafkaNumPartitions),
      filterFunc = null)

    //window.mapPartitions { it =>}

    window.foreachRDD { rdd =>
      println(s"Partitioner: ${rdd.partitioner}")
      println(s"Number of spark partitions after windowing: ${rdd.partitions.size}")

      rdd.foreachPartition { iter =>
        //Runs on a worker
        val pId = TaskContext.get.partitionId
        //println("read offset ranges on the executor\n" + offsetRanges.mkString("\n"))
        val range = offsetRanges(pId)
        val host = java.net.InetAddress.getLocalHost

        //If you want finer grained control you must store offsets somewhere (Cassandra)

        //At least once
        //Stage 1 - save results
        val now = ZonedDateTime.now(ZoneOffset.UTC)
        iter.foreach { r =>
          val value: java.lang.Double = if(r._2._2 > 0l) r._2._1 / r._2._2 else 0.0
          cc.withSessionDo {
            _.execute(s"INSERT INTO ${keySpace}.${maTable}(device_id, time_bucket, source, when, value) values (?,?,?,?,?)",
              r._1, (timeBucketFormatter format now), s"$sourceName-${host}-$pId", java.util.Date.from(now.toInstant), value)
          }
        }

        //Stage 2 - save the most recent offset
        cc.withSessionDo { s =>
          s.execute(s"UPDATE ${keySpace}.${offsetsTable} SET offset = ? where topic = ? and partition = ?",
            range.untilOffset: java.lang.Long, topic, pId: java.lang.Integer)
        }
      }
    }

    ssc
  }

  /**
   * If the job fails and restarts at something other than the highest offset,
   * the first window after restart will include all messages received while your job was down,
   * not just N seconds worth of messages. It's a mess.
   * We accept that our first window will be wrong.
   *
   */
  def main(args: Array[String]): Unit = {
    if (args.length != 7) {
      System.err.println(s"Found: $args with length ${args.length} Expected: <clientId> <topic> <numOfPartitions> <zookeper> <broker> <streamingInterval> <windowSize>")
      System.exit(-1)
    } else {
      val clientId = args(0)
      val topic = args(1)
      val numOfPartitions = args(2).toInt
      val zookeper = args(3)
      val kafkaBroker = args(4)
      val streamingIntervalSec = args(5).toInt
      val windowSize = args(6).toInt
      val parallelism = args(2).toInt

      val conf = new SparkConf().setAppName(sourceName)
        .set("spark.cleaner.ttl", "3600")
        .set("spark.default.parallelism", parallelism.toString)
        .set("spark.streaming.backpressure.enabled", "true")
        .set("spark.streaming.backpressure.pid.minRate", "10000")
        .set("spark.streaming.kafka.maxRatePerPartition", "10000")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

      println(conf.toDebugString)
      println(s"Client: $clientId - Topic: $topic - NumOfPartitions:$numOfPartitions - Zookeper: $zookeper - Broker: $kafkaBroker")

      val cc = new CassandraConnector(CassandraConnectorConf(conf))

      cc.withSessionDo { session =>
        session.execute(
          s"""
            |CREATE KEYSPACE IF NOT EXISTS $keySpace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };
          """.stripMargin)

        /**
         *
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
        session.execute(
          s"""CREATE TABLE IF NOT EXISTS ${keySpace}.${maTable} (
              | device_id varchar,
              | time_bucket varchar,
              | source varchar,
              | when timestamp,
              | startOffset bigint,
              | value double,
              | PRIMARY KEY ((device_id, time_bucket), when)) WITH CLUSTERING ORDER BY (when DESC);""".stripMargin)

        session.execute(
          s"""CREATE TABLE IF NOT EXISTS ${keySpace}.${offsetsTable} (
              | topic varchar,
              | partition int,
              | static offset bigint,
              | PRIMARY KEY ((topic, partition)));""".stripMargin)

        (0 until numOfPartitions).foreach { n =>
          session.execute(s"INSERT INTO ${keySpace}.${offsetsTable}(topic, partition, offset) values(?,?,?) IF NOT EXISTS;",
            topic, n: java.lang.Integer, 0l: java.lang.Long)
        }
      }

      val offsets =
        (0 until numOfPartitions).foldLeft(Map[TopicAndPartition, Long]()) { (acc, i) =>
          val offset = Option(
            cc.withSessionDo { session =>
              session.execute(s"SELECT offset FROM ${keySpace}.${offsetsTable} where topic = ? and partition = ?", topic, i: java.lang.Integer)
            }.one()).map(_.getLong("offset")).getOrElse(0l)
          acc + (TopicAndPartition(topic, i) -> offset)
        }

      val ctx = setupSsc(conf, clientId, topic, numOfPartitions, zookeper, kafkaBroker, streamingIntervalSec, windowSize, cc, offsets)
      ctx.start()
      ctx.awaitTermination
    }
  }
}