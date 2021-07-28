# Kafka Crash Course

Apache Kafka is a distributed publish-subscribe messaging system.

## Commands
- <a href="https://github.com/AlizaminJ/kafka/blob/master/basic-kafka-commands.txt"> Basic kafka commands</a>
- <a href="https://github.com/AlizaminJ/kafka/blob/master/kafka-cluster-with-multiple-brokers.txt">Kafka cluster</a>
- <a href="https://github.com/AlizaminJ/kafka/blob/master/consumer-groups.txt">Consumer groups</a>


## Overall Architecture

![kafka architecture core](./assets/kafka-architecture-core.png?raw=true "kafka architecture core")  

![kafka architecture](./assets/kafka-architecture.png?raw=true "kafka architecture") 

![kafka architecture for ml](./assets/kafka-architecture-for-ml.png?raw=true "kafka architecture for ML")  


## Requirements
- Java installed Ubuntu machine. If you do not have an Ubuntu machine, you may try <a href="https://aws.amazon.com/lightsail/">AWS Lightsail</a> or <a href="https://www.digitalocean.com/products/droplets/"> Droplets on Digital Ocean as a VPS solution (Kafka is a server side system!)</a>
- Choose 4G RAM VPS not to encounter memory issues

### Intro & Installation
- Connect using your own SSH client to VPS with your .pem-file (you have to download it from Lightsail) after having created it:
```
ssh -i YOUR_PEM_FILE.pem YOUR_USER@VPS_IP_ADDESS
```
- Install java runtime environment and development kit:
```
sudo apt update && sudo apt -y upgrade
sudo apt -y install openjdk-11-jre-headless
sudo apt -y install openjdk-11-jdk
```
- Install binary version kafka on https://kafka.apache.org/downloads: Get the installation link (it suggests the closest server, in my case "https://apache.uib.no/kafka/2.5.0/kafka_2.12-2.5.0.tgz", create and download the file into "Downloads" directory, and finally install it into a new "kafka" directory: 
```
curl https://apache.uib.no/kafka/2.5.0/kafka_2.12-2.5.0.tgz -o Downloads/kafka.tgz
mkdir kafka
cd kafka
tar -xvzf ~/Downloads/kafka.tgz --strip 1
```
- Check the structure in kafka folder:
  - "bin" - all scripts
  - "config" - ".properties" files for configuration
  - "libs" - installed libraries
  - "logs" - logs files
  - "site-docs" - documentation
- After installation, if you try to start the server you will get error, because kafka needs to connect to zookeeper which coordinates brokers:
```
bin/kafka-server-start.sh config/server.properties
```
![ZooKeeper](./assets/zookeper-coordination.png?raw=true "ZooKeeper")   
For fallback, you may create zookeeper cluster (ensemble) if there are too many broker clusters

- Zookeeper maintains list of active brokers, manages configuration of the topics and partitions and elects controller. So start zookeper (default port localhost: 2181), then start the server/broker (default port localhost: 9092; NB! If you want to run multiple brokers in a single computer you have to run each broker in a distinct port) in a new terminal while zookeper is running:
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
- Kafka saves logs in "logs" folder but messages are stored in /tmp/kafka-logs which are automatically created by kafka server (check "logs/server.properties" for log.dirs=/tmp/kafka-logs). To check running brokers in kafka-logs:  
```
cat meta.properties

# result something like this
broker.id=0
version=0
cluster.id=4kUvEDRXSBi-u9LwNOX_7g
```
### Topics
- Create a topic (remember, you may specify either any of kafka/broker/bootstrap server or zookeeper, try ```bin/kafka-topics.sh --create``` to see):
```
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic YOUR_TOPIC_NAME_1
```
- List existing topics:
```
bin/kafka-topics.sh --list --zookeeper localhost:2181
```
- To read a topic's details:
```
bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic YOUR_TOPIC_NAME_1

# Result. Here zero stands for broker_id which starts at zero
 Topic: YOUR_TOPIC_NAME_1   Partition: 0    Leader: 0       Replicas: 0     Isr: 0
```   
- Inside every topic, messages can be distributed among several partitions. As you may read in "config/server.properites", the default number of log partitions per topic is 1 (for production environments, it is recommended to have 3: 1 leader and 2 replicas). More partitions allow greater parallelism for consumption, but this will also result in more files across the brokers. Each partition should have at least one leader.  

![Partitions](./assets/kafka-partitions.png?raw=true "Partitions")  

![Partitions structure](./assets/kafka-partitions-structure.png?raw=true "Partitions structure")  

![Partition leader](./assets/kafka-partition-leader.png?raw=true "Partition leader")  

ReplicationFactor says how many time each message is replicated in a cluster (redundancy). For example, if you have 3 servers, and ReplicationFactor=3, then every single message will be replicated in each server once.  
- But what controls the leader-follower relationship (and much more)? The brain of Apache Kafka is controller. A big part of what the controller does is to maintain the consistency of the replicas and determine which replica can be used to serve the clients, especially during individual broker failure.  

![Controller](./assets/kafka-controller.png?raw=true "Controller") 


### Producer & Consumer
- Start consumer (which consumes messages) in a new terminal:
```
# add "--from-beginning" to the end to get all messages if consumer started 
# after producer having sent messages already

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic YOUR_TOPIC_NAME_1 
```
- Start producer (which sends messages) by the following in a new terminal and write your first messages in console:  
```
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic YOUR_TOPIC_NAME_1

# result, and your inout
> item1
> item2 
> ...etc
```
- Kafka stores messages even if they were already consumed by one of the consumers. The same messages can be read multiple times by different consumers. Multiple consumers and multiple producers can exchange messages via single centralized storage point - kafka cluster. Producers and consumers do not know each other.
The structure of the message:
  - Timestamp
  - Offset number (which is unique across 
  ion)
  - Key (optional)
  - Value (sequence of bytes) --> You can exchange anything like objects, strings or number   
It is up to the producer to choose the partition.  

##  Overall Process
### Producer   
![Producer](./assets/kafka-infrastructure-producer.png?raw=true "producer")   
### Consumer
![Consumer](./assets/kafka-infrastructure-consumer.png?raw=true "consumer")  


## Further Reading & Sources
- <a href="https://sookocheff.com/post/kafka/kafka-in-a-nutshell/">Kafka in a Nutshell</a>
- <a href="http://cloudurable.com/blog/kafka-architecture/index.html">Kafka Architecture</a>
- <a href="https://www.confluent.io/blog/using-apache-kafka-drive-cutting-edge-machine-learning/">Using Apache Kafka to Drive Cutting-Edge Machine Learning</a>
- <a href="https://www.cloudkarafka.com/blog/2016-11-30-part1-kafka-for-beginners-what-is-apache-kafka.html">Part 1: Apache Kafka for beginners - What is Apache Kafka?</a>
- <a href="https://www.youtube.com/watch?v=CU44hKLMg7k&list=PLWkguCWKqN9P6pYm70oCGsa11Fnrf2MEX"> Kafke Beginner Course </a>
- <a href="https://insidebigdata.com/2018/04/19/developing-deeper-understanding-apache-kafka-architecture-part-2-write-read-scalability/">Developing a Deeper Understanding of Apache Kafka Architecture</a>
- <a href=""> </a>


