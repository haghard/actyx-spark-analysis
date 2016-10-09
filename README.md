# kafka-spark-moving-average

An implementation of moving average alg for spark. This spark-streaming job reads data from kafka topic, evaluates moving average aggregates and writes them into cassandra. The main downside being is that we can use this job only if a difference between an arrival and a processing time of an event very small(Within a single DC). 

#How to build

> sbt assembly

It will create target/scala-2.11/moving-average.jar

#How to run on Spark standalone cluster

To run this spark streaming job you need to have Kafka, Cassandra and Spark cluster

> bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.11:1.6.1,datastax:spark-cassandra-connector:1.6.0-s_2.11 \
  --master spark://192.168.0.182:7077 \  
  --conf spark.cassandra.connection.host=192.168.0.182,192.168.0.38 \  
  --conf spark.cassandra.output.consistency.level=LOCAL_QUORUM \  
  --total-executor-cores 4 \  
  --executor-memory 1024MB \  
  --class analytics.MovingAverage \  
  ../moving-average.jar \  
  client1 readings 4 192.168.0.38:2181 192.168.0.38:9092 15 4

## Parameters 

192.168.0.182,192.168.0.38 - cassandra connection points

client1 - client.id for zookeeper

readings - kafka topic name

4 - the number of kafka topic partitions  

192.168.0.182:2181 - zookeper address

192.168.0.38:9092 - kafka broker address 

15 - slideDuration in seconds (spark streaming terminology)
 
4 - number of slideDuration in the single windowDuration (spark-streaming terminology) 


### Links ###
  https://www.youtube.com/watch?v=fXnNEq1v3VA
  
  http://koeninger.github.io/kafka-exactly-once/#1
  
  http://koeninger.github.io/spark-cassandra-example/#1
  
  https://github.com/koeninger/kafka-exactly-once
  
  https://www.youtube.com/watch?v=p5U9rTFYq0c&list=PLm-EPIkBI3YoiA-02vufoEj4CgYvIQgIk&index=23


### Spark standalone cluster ###

sbin/start-master.sh -h 192.168.0.182

sbin/start-slave.sh spark://192.168.0.182:7077 -h 192.168.0.38 --memory 1100M

sbin/start-slave.sh spark://192.168.0.182:7077 -h 192.168.0.148 --memory 1100M

sbin/start-slave.sh spark://192.168.0.182:7077 -h 192.168.0.57 --memory 1100M


### How to run grafana + graphite inside a docker container ###   

``` 
    docker run -d -p 80:80 -p 8125:8125/udp -p 8126:8126 kamon/grafana_graphite
```

### How to set up grafana_graphite ### 

http://stackoverflow.com/questions/32459582/how-to-set-up-statsd-along-with-grafana-graphite-as-backend-for-kamon

### Run kafka with Docker compose  ###

Create file docker-compose.yml
```
  version: '2'
  services:
    zookeeper:
      image: wurstmeister/zookeeper:3.4.6
      ports:
        - "2181:2181"
    kafka:
      image: wurstmeister/kafka:0.10.0.1
      ports:
        - "9092:9092"
      environment:
        KAFKA_ADVERTISED_HOST_NAME: 192.168.0.38
        KAFKA_CREATE_TOPICS: "readings:4:1" // not nes
        KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181        
      volumes:
        - /var/run/docker.sock:/var/run/docker.sock
```
and run `docker-compose up -d`


To create topic manually

`bin/kafka-topics.sh --create --zookeeper 192.168.0.148:2181 --replication-factor 1 --partitions 4 --topic readings`


### Query ###
 
```
  select * from aggregates.moving_average where device_id = 'e7335741-e93a-4a30-b430-7cf48b8d4afd' and time_bucket = '2016-10-09';
``

### Cassandra LWT ###

  https://www.youtube.com/watch?v=wcxQM3ZN20c&index=42&list=PLm-EPIkBI3YoiA-02vufoEj4CgYvIQgIk

  https://github.com/chbatey/cassandra-lwts-summit.git


### Add more brokers ### 
docker-compose scale kafka=3

### Remove all composed services ###
docker-compose stop && docker-compose rm -f


### Run 2 node cassandra cluster ###

docker run -d -e CASSANDRA_BROADCAST_ADDRESS=192.168.0.182 -e CASSANDRA_SEEDS=192.168.0.182,192.168.0.148 -e CASSANDRA_CLUSTER_NAME="haghard_cluster" -e CASSANDRA_HOME="/var/lib/cassandra" -e CASSANDRA_START_RPC="true" -e CASSANDRA_RACK="wr0" -e CASSANDRA_DC="west" -e CASSANDRA_ENDPOINT_SNITCH="GossipingPropertyFileSnitch" -p 7000:7000 -p 7001:7001 -p 9042:9042 -p 9160:9160 -p 7199:7199 -v /home/haghard/Projects/cassandra-db-3.7:/var/lib/cassandra cassandra:3.7

docker run -d -e CASSANDRA_BROADCAST_ADDRESS=192.168.0.38 -e CASSANDRA_SEEDS=192.168.0.182,192.168.0.148 -e CASSANDRA_CLUSTER_NAME="haghard_cluster" -e CASSANDRA_HOME="/var/lib/cassandra" -e CASSANDRA_START_RPC="true" -e CASSANDRA_RACK="wr1" -e CASSANDRA_DC="west" -e CASSANDRA_ENDPOINT_SNITCH="GossipingPropertyFileSnitch" -p 7000:7000 -p 9042:9042 -p 9160:9160 -p 7199:7199 -v /home/haghard/Projects/cassandra-db-3.7:/var/lib/cassandra cassandra:3.7


