package com.ngt.etl.test

import com.ngt.etl.source.MyRedisSourceFun
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

import java.util.Properties

/**
 * @author ngt
 * @create 2020-12-20 1:17
 */
object RedisToKafka {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val prop = new Properties
    prop.load(ClassLoader.getSystemResourceAsStream("kafka.properties"))

    val dataStream: DataStream[String] = env.addSource[String](new MyRedisSourceFun)
      .map(x => x)
    dataStream.print()


    dataStream.addSink(
      new FlinkKafkaProducer011[String]("192.168.100.102:9092,192.168.100.103:9092,192.168.100.104:9092",
        "bitcoin-source", new SimpleStringSchema())
    )

    env.execute()
  }
}
