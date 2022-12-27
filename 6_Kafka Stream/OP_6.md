# project hints: ad click dataset criteo, common crawl, graph datasets --> spark graphs

Basic terms
    Kafka Message
        Key
        Value
        Timestamp
    Topic in Kafka
        Producer pushes messages to a topic
        Consumer consumes messages from a topic
    Kafka Broker
        Kafka broker => Physical machine on which Kafka is running
        Kafka Cluster => Multiple Kafka broker’s => Multiple machines working together
Logs
    Data segments present in your disk
    Stores messages in a order fashion: Assigns sequence id to each message before storing in logs

Partitioning in Kafka (Scalability): Topic Partition are a unit of parallelism, partition can only be worked by one consumer in a consumer group at a time 

Replication in Kafka (Fault tolerance): Leader and Replica(s) messages are replicated across different brokers [*Replication Factor]

Configurations:
    -retention.ms: amount of time logs will stay before get deleted
    -cleanup.policy: [delete|compact] the messages from topic (batch mode in background)
    -partition: scalability count
    -replication: number of times a partition will be replicated

    Consumer config:
        offset: consumer already read  # since consumer follow up since last place it read before
        consumer.group.id: identifier
        auto.offset.reset: [earliest|latest] first time read message from first (exist) or last message (now-on)

    Producer config:
        acks: [0|1|all] 0   : doesn't wait for leader or replica broker to write the message on disk :: log messages / monitor messages (speed better)
                        1   : waits for leader broker to write message on disk :: (message could die with broker)
                        all : waits for leader and all replica to write the message on disk :: safest

docker-compose.yml: https://developer.confluent.io/quickstart/kafka-docker/

add required requirments for future python scripts

can create topic either from UI or : ./bin/kafka-topics.sh --create --topic demo_1 --bootstrap-server localhost:9092 --partitions 2

# 
Check example of first production to kafka: producer.py
with the Consumer to contact topic in kafka: consumer.py
multi-consumers are with same group_id

# Avro and Schema Registry:

Why Schema needed: ex: php send java serialized object or php object or internal representation :: Hard to deseralize the message.
AVRO: Data Serialization system
    Schema stored seperatly from the records (schema registery)
    Adv.: smaller file size (vs JSON), schema evolution, Avro clients provide auto. validation against schema
Deserialize with any compatible schema in the schema registery as a consumer.
###Schemas not compatible: produce new topic for new schema intake, translate in downstream from new to old schema till all services migrate to the new topic, then decomission converter.

added to docker-compose.yml as schema-registery
# In publishing messages to kafka using avro; need avro schema: "FILE.avsc" :: hashed key schema as need to determine which partition the data will go into
Protobuf supported by Kafka


### Kafka Stream:
    Client library for building stream application
    Data from Kafka to Kafka ###[THE LIMITATION OF KAFKA]
    Stream application
        Fault tolerant
        Scalable
    Event processing with milliseconds latency 
    Provides a convenient DSL

    Processing and analyzing data stored in Kafka
    Builds upon important stream processing concepts:
        event time/processing time, 
        windowing support
        management of state

Kafka stream in short
    Millisecond delay
    Balance the processing load as new instances of your app are added or existing ones crash
    Maintain local state for tables
    Recover from failures
Stream vs State 

Kafka stream features
    Aggregates: count, groupby
    Stateful processing (Stored internally in Kafka topic)
    Joins
        KStream with KStream
        KStream with KTable
        KTable with KTable
    Windows
        Time based
        Session based

[ Spark, Flink ... Storm vs Kafka Streams ... Consumer]

example using Faust lib. python: 
#### Python Faust coming from RobinHood
* [Faust Doc](https://faust.readthedocs.io/en/latest/index.html)
* [KStream vs Faust](https://faust.readthedocs.io/en/latest/playbooks/vskafka.html)

#### JVM library "original from Confluent Kafka" 
* [Confluent Kafka Stream](https://kafka.apache.org/documentation/streams/)

Examples by producing json messages in folder "streams"

### Kafka Stream-Features: not only python related all are in JVM
Threading model, Joins, Global KTable (act as broadcast (reminder: small table join large one))

Interactive Queries (RPC), KSQL, Kafka Connect
