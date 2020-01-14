package AvroProducer_v1

import java.io.File
import java.net.URI
import java.time.Duration
import java.util.{Properties => jProps}

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.avro.{Protocol, Schema}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer._
import org.slf4j.{Logger, LoggerFactory}
import org.apache.avro.generic.{GenericData, GenericRecord}

import scala.util.Random

object AvroProducer_v1 extends App {

  val props: jProps = new jProps()
  props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
  props.put("schema.registry.url", "http://localhost:8081")

  System.setProperty("log4j.configuration",getClass.getResource("../log4j.properties").toString)
  final val logger: Logger = LoggerFactory.getLogger("AvroProducer_v1")

  // Parse that Schema!!!
  val schemaFile: URI = getClass.getResource("/AvroProducer_v1/Order_v.avsc").toURI
  val valueSchema: Schema = new Schema.Parser().parse(new File(schemaFile))

  val producer: KafkaProducer[Long, GenericRecord] = new KafkaProducer(props)

  // Put together the {Key:Value} pair!!!
  val value: GenericRecord = new GenericData.Record(valueSchema)
  value.put("ts", System.currentTimeMillis())
  value.put("product_id", 2333)
  value.put("quantity", 1)

  val key: Long = (new Random()).nextLong()

  // Build that record!!!
  val kr = new ProducerRecord[Long, GenericRecord]("orders-topic", null, key, value)

  // Produce that thang!!!
  producer.send(kr, new Callback() {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception != null) {
        logger.info(s"${exception.getCause}\n")
        exception.getStackTrace foreach (line => logger.info(s"\t\t${line}"))
        logger.info(s"${exception.getMessage}")
      }
      else logger.info(s"The new offset is ${metadata.offset()}, and is sent " +
        s"to partition ${metadata.partition()} - ${metadata.toString}")
    }
  })

  producer.flush()
  producer.close(Duration.ofMillis(100))

}
