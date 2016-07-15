# actyx-spark-analysis

A implementation of moving average alg for spark. This job reads data from kafka's topics, evaluate a moving average aggregate and writes into cassandra. The main downside thought, is that can be use this job only if a difference between arrival and processing time of an event very small. 


#How to build

> sbt assembly

It will create target/scala-2.11/moving-average.jar

#How to run on spark cluster

To run this spark streaming job you need to have Kafka, Cassandra and Spark cluster

> bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.11:1.6.1,datastax:spark-cassandra-connector:1.6.0-s_2.11 \
  --master spark://192.168.0.182:7077 \  
  --conf spark.cassandra.connection.host=192.168.0.182,192.168.0.38 \  
  --conf spark.cassandra.output.consistency.level=LOCAL_QUORUM \  
  --total-executor-cores 4 \  
  --executor-memory 1024MB \  
  --class actyx.MovingAverage \  
  ../moving-average.jar \  
  client1 readings 4 192.168.0.182:2181 192.168.0.38:9092 15 4

## Parameters 

192.168.0.182,192.168.0.38 - cassandra connection points

client1 - client.id for zookeeper

readings - kafka topic name

4 - the number of kafka topic partitions  

192.168.0.182:2181 - zookeper address

192.168.0.38:9092 - kafka broker address 

15 - slideDuration in seconds (spark streaming terminology)
 
4 - number of slideDuration in the single windowDuration (spark streaming terminology) 

