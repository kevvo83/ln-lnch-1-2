package SimpleConsumer

import org.apache.kafka.clients._
import org.apache.kafka.clients.consumer.{KafkaConsumer, _}
import java.util.{Arrays, Properties => jProps, ArrayList => juArrayList}
import java.time.Duration
import java.util

import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.jdk.CollectionConverters._

object SimpleConsumer extends App {

  val props: jProps = new jProps()
  props.setProperty("bootstrap.servers", "localhost:9092")
  props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.setProperty("group.id", "group1")
  props.setProperty("enable.auto.commit", "false")

  val topicname: String = "my-topic"

  val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
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
