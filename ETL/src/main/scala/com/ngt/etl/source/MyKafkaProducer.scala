package com.ngt.etl.source

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.io.BufferedSource

/**
 * @author ngt
 * @create 2020-12-19 21:44
 */
object MyKafkaProducer {
  def main(args: Array[String]): Unit = {
    writeToKafka("bitcoin-source0")
  }

  def writeToKafka(topic: String): Unit = {

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)
    //    val source: BufferedSource = io.Source.fromFile("data/bitcoin.csv")
    val source: BufferedSource = io.Source.fromFile("data/bitstampUSD.csv")

    for (line <- source.getLines()) {
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, line)
      producer.send(record)
//      TimeUnit.MILLISECONDS.sleep(10)
    }
    producer.close()
  }
}
