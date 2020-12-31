package com.ngt.etl.test

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties

/**
 * @author ngt
 * @create 2020-12-31 16:47
 */
object TestKafka {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //    val inputStream: DataStream[String] = env.readTextFile("TrafficData/metor_time_sort.csv")

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("group.id", "bitcoin")

    // 使用通配符 同时匹配多个Kafka主题
    val inputStream = env.addSource(new FlinkKafkaConsumer011[String](java.util.regex.Pattern.compile("bitcoin-source"), new SimpleStringSchema(), properties))

    inputStream.print()
    env.execute()
  }
}

