package SimpleBulkProducer

import java.util.{Properties => jProps}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}

/*
  OBJECTIVE: Produce simple string serialized message to topic, with a Partitioner
 */

object SimpleBulkProducerWithPartitioner extends App {

  val props: jProps = new jProps()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  System.setProperty("log4j.configuration",getClass.getResource("../log4j.properties").toString)
  final val logger: Logger = LoggerFactory.getLogger("SimpleProducer")

  val producer: KafkaProducer[String, String] = new KafkaProducer(props)

  List.range[Int](1, 51, 1).foreach[Unit]( x => {
    val rec: ProducerRecord[String, String] =
      new ProducerRecord[String, String]("my-topic", (x % 3), s"${x}_key"  , s"${x}_value")

    producer.send(rec)
  }
  )

  producer.close()

  // Connect 3 kafka-console-consumer.sh to the same Consumer Group label using -
  // kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my-topic --group console-consumer-77876

  // Then Run this method - it will be clear that each message will ONLY go to 1 partition

}
