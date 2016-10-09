package analytics

import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.datastax.driver.core.BatchStatement.Type

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

import java.time.{ZoneOffset, ZonedDateTime}

import com.datastax.driver.core.exceptions.WriteTimeoutException
import com.datastax.driver.core.{ BatchStatement, ConsistencyLevel, WriteType}
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import spray.json._


/**

 * scp target/scala-2.11/moving-average.jar haghard@192.168.0.182:/home/haghard/Projects

 * bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.11:1.6.1,datastax:spark-cassandra-connector:1.6.0-s_2.11 \
 * --master spark://192.168.0.182:7077 \
 * --conf spark.cassandra.connection.host=192.168.0.182,192.168.0.38 \
 * --conf spark.cassandra.output.consistency.level=LOCAL_QUORUM \
 * --total-executor-cores 4 \
 * --executor-memory 1024MB \
 * --class analytics.MovingAverage \
 * ../moving-average.jar \
 * client1 readings 4 192.168.0.148:2181 192.168.0.148:9092 35 4 192.168.0.182 8125

 */
object MovingAverage extends Scaffolding {

  case class CommitFailed(msg: String, cause: WriteTimeoutException) extends Exception(msg)

  def setupSsc(conf: SparkConf, clientId: String, topic: String, kafkaNumPartitions: Int, zookeper: String, broker: String,
    streamingIntervalSec: Int, windowSize: Int, graphiteHost: String, graphitePort: Int, /*g: GraphiteUDP,*/
    cc: CassandraConnector, offsets: Map[TopicAndPartition, Long]): StreamingContext = {

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

    window.foreachRDD { rdd =>
      println(s"Partitioner: ${rdd.partitioner}")
      println(s"Number of spark partitions after windowing: ${rdd.partitions.size}")
      rdd.foreachPartition { iter =>
        //Runs on a worker
        /*val graphite = new GraphiteUDP {
          override val address = new InetSocketAddress(InetAddress.getByName(graphiteHost), graphitePort)
        }*/

        val pId = TaskContext.get.partitionId
        //println("read offset ranges on the executor\n" + offsetRanges.mkString("\n"))
        val range = offsetRanges(pId)
        val host = java.net.InetAddress.getLocalHost


        //http://christopher-batey.blogspot.ru/2015/03/cassandra-anti-pattern-cassandra-logged.html
        //I want finer grained control, so I must store offsets anywhere else (Cassandra)
        //I can decide to make sure that both statements succeed
        //(Atomicity)Logged batches are used to ensure that all the statements will eventually succeed.

        //Bad: Batch span 2 tables and several partitions to ensure that all the statements will eventually succeed (we need atomicity)
        val stmt = new BatchStatement(Type.LOGGED)
        stmt.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
        stmt.setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL)

        cc.withSessionDo { s =>
          //def graphiteReporter(metricRegistry: MetricRegistry, pId: Int, host: InetAddress): Unit /*GraphiteReporter*/ = {
          val graphite = new Graphite(new InetSocketAddress(graphiteHost, graphitePort))
          val reporter = GraphiteReporter.forRegistry(s.getCluster.getMetrics.getRegistry)
            .prefixedWith(s"moving-avg-$pId")
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .filter(MetricFilter.ALL)
            .build(graphite)
          reporter.start(20, TimeUnit.SECONDS)
          //}

          //graphiteReporter(s.getCluster.getMetrics.getRegistry, pId, host)
        }

        cc.withSessionDo { s =>
          val now = ZonedDateTime.now(ZoneOffset.UTC)
          //graphiteReporter(s.getCluster.getMetrics.getRegistry, pId, host)

          val offsetStmt = s.prepare(InsertOffset)
          val readingStatement = s.prepare(InsertReading)

          var batch = stmt.add(offsetStmt.bind(range.untilOffset: java.lang.Long, topic, pId: java.lang.Integer))
          var buffer = Vector[String]()
          iter.foreach { case (id, (sum, count)) =>
            val value: java.lang.Double = if (count > 0l) sum / count else 0.0
            buffer = buffer :+ s"${pId}_${host}:1|c"
            batch = batch.add(readingStatement.bind(id, (timeBucketFormatter format now), s"${maTable}-${host}-$pId",
              java.util.Date.from(now.toInstant), range.fromOffset: java.lang.Long, range.untilOffset: java.lang.Long, value))
          }

          //This write consist of 2 operations
          //1.Writing to the batch log
          //2.Applying the actual statements

          executeWithRetry(5) {
            s.execute(batch)
            //buffer.foreach(graphite.send(_))
          }
        }
      }
    }
    ssc
  }

  def executeWithRetry[T](n: Int)(f: ⇒ T) = retryCassandraWrite(n)(f)

    @tailrec def retryCassandraWrite[T](n: Int)(action: ⇒ T): T =
      Try(action) match {
        case Success(x) ⇒ x
        case Failure(e) ⇒
          if(e.isInstanceOf[WriteTimeoutException] && n > 1) {
            val ex = e.asInstanceOf[WriteTimeoutException]
            if (ex.getWriteType().equals(WriteType.BATCH_LOG)) {
              //Couldn't commit batch so we can retry
              retryCassandraWrite(n - 1)(action)
            } else if (ex.getWriteType().equals(WriteType.BATCH)) {
              //This means it made it to the batch log so that they will get replayed eventually.
              null.asInstanceOf[T]
            } else throw new RuntimeException("Unexpected write type: " + ex.getWriteType())
        } else throw e
      }

  /**
   * If the job fails and restarts at something other than the highest offset,
   * the first window after restart will include all messages received while your job was down,
   * not just N seconds worth of messages. It's a mess.
   * We accept that our first window will be wrong.
   *
   */
  def main(args: Array[String]): Unit = {
    if (args.length != 9) {
      System.err.println(s"Found: $args with length ${args.length} Expected: <clientId> <topic> <numOfPartitions> <zookeper> <broker> " +
        s"<streamingInterval> <windowSize> <graphiteHost> <graphitePort>")
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

      val graphiteHost = args(7)
      val graphitePort = args(8).toInt

      val conf = new SparkConf().setAppName(maTable)
        .set("spark.cleaner.ttl", "3600")
        .set("spark.default.parallelism", parallelism.toString)
        .set("spark.streaming.backpressure.enabled", "true")
        .set("spark.streaming.backpressure.pid.minRate", "10000")
        .set("spark.streaming.kafka.maxRatePerPartition", "10000")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

      println(conf.toDebugString)
      println(s"Client: $clientId - Topic: $topic - NumOfPartitions:$numOfPartitions - Zookeper: $zookeper - Broker: $kafkaBroker - Graphite:${graphiteHost}:${graphitePort}")

      val cc = new CassandraConnector(CassandraConnectorConf(conf))

      cc.withSessionDo { session =>
        session.execute(s"""
           |CREATE KEYSPACE IF NOT EXISTS $keySpace WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };
        """.stripMargin)

        session execute createMaTable

        session execute createOffsetsTable

        (0 until numOfPartitions).foreach { n =>
          session.execute(s"INSERT INTO ${keySpace}.${offsetsTable}(topic, partition, offset) values(?,?,?) IF NOT EXISTS;",
            topic, n: java.lang.Integer, 0l: java.lang.Long)
        }
      }

      val offsets = (0 until numOfPartitions).foldLeft(Map[TopicAndPartition, Long]()) { (acc, i) =>
        val offset = Option(
          cc.withSessionDo { session =>
            session.execute(s"SELECT offset FROM ${keySpace}.${offsetsTable} where topic = ? and partition = ?", topic, i: java.lang.Integer)
          }.one).map(_.getLong("offset")).getOrElse(0l)
        acc + (TopicAndPartition(topic, i) -> offset)
      }


      val ctx = setupSsc(conf, clientId, topic, numOfPartitions, zookeper, kafkaBroker, streamingIntervalSec,
        windowSize, graphiteHost, graphitePort, cc, offsets)
      ctx.start
      ctx.awaitTermination
    }
  }
}