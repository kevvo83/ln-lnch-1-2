package AvroConsumer_v1

import java.time.Duration
import java.util.Arrays
import java.util.{Properties => jProps}
import java.util.{ArrayList => juArrayList}

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.LongDeserializer

import scala.jdk.CollectionConverters._

object AvroConsumer_v1 extends App {

  val props: jProps = new jProps()
  props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer])
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer])
  props.put("schema.registry.url", "http://localhost:8081")
  props.setProperty("group.id", "group1")
  props.setProperty("enable.auto.commit", "false")

  val topicname: String = "orders-topic"

  val consumer = new KafkaConsumer[String, String](props)
  consumer.subscribe(Arrays.asList[String](topicname))

  // Get List of Partition Data for the Topic in question
  val topicPartitionData: juArrayList[TopicPartition] = new juArrayList()
  val listPartitions = consumer.partitionsFor(topicname).asScala
  listPartitions.foreach( partInfo => {
    topicPartitionData.add(new TopicPartition(partInfo.topic(), partInfo.partition()))
  }
  )
  consumer.unsubscribe()

  // Manually assign Consumer to All Partitions of the Topic
  consumer.assign(topicPartitionData)

  var records:ConsumerRecords[String, String] =  consumer.poll(Duration.ofMillis(100))

  // Manually seek Consumer to the start of each Topic Partition
  consumer.seek(topicPartitionData.get(0), 0)
  consumer.seek(topicPartitionData.get(1), 0)
  consumer.seek(topicPartitionData.get(2), 0)


  // Now finally, read all the records in the Topic!
  while (true) {

    records = consumer.poll(Duration.ofMillis(100))

    records.asScala.foreach(record => {
      println(s"key=${record.key()}, value=${record.value()}, topic=${record.topic()}, " +
        s"partition=${record.partition()}, offset=${record.offset()}")
    })

  }

}
