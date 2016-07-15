# actyx-spark-analysis


#How to run on spark cluster

bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.11:1.6.1,datastax:spark-cassandra-connector:1.6.0-s_2.11 \

  --master spark://192.168.0.182:7077 \
  
  --conf spark.cassandra.connection.host=192.168.0.182,192.168.0.38 \
  
  --conf spark.cassandra.output.consistency.level=LOCAL_QUORUM \
  
  --total-executor-cores 4 \
  
  --executor-memory 1024MB \
  
  --class actyx.MovingAverage \
  
  ../moving-average.jar \
  
  client1 readings 4 192.168.0.182:2181 192.168.0.38:9092 15 4

## Parameters 

client1 - client.id for zookeeper

readings - kafka topic name

4 - number of partitions  

192.168.0.182:2181 - zookeper address

192.168.0.38:9092 - kafka broker address 

15 - slideDuration in seconds (spark terminology)
 
4 - number of slideDuration in the single windowDuration (spark terminology) 

