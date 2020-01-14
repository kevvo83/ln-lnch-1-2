package SimpleProducer

import java.util.{Properties => jProps}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.slf4j.{LoggerFactory, Logger}


/*
  OBJECTIVE: Produce simple String-serialized message to topic, no partitioner
 */

object SimpleProducer extends App {

  val props: jProps = new jProps()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  System.setProperty("log4j.configuration",getClass.getResource("../log4j.properties").toString)
  final val logger: Logger = LoggerFactory.getLogger("SimpleProducer")

  val producer: KafkaProducer[String, String] = new KafkaProducer(props)

  // Topic Name needs to exist already - Otherwise what is the Producer doing? Are there timeouts, etc?
  var rec: ProducerRecord[String, String] = new ProducerRecord[String, String]("my-topic",
                                                                                "firstKey",
                                                                                "firstValue")
  producer.send(rec)

  rec = new ProducerRecord[String, String]("my-topic", "secondKey", "secondValue")
  producer.send(rec)

  producer.close()

}
