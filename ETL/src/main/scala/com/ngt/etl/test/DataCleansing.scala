package com.ngt.etl.test

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
 * @author ngt
 * @create 2020-12-19 21:58
 */
object DataCleansing {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val properties: Properties = new Properties()
    //    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    //    properties.setProperty("group.id", "consumer-group")
    //
    //    val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("bitcoin-source", new SimpleStringSchema(), properties))

    val inputStream = env.readTextFile("data/test.csv")

    val dataStream: DataStream[String] = inputStream
      .filter(data => {
        ! data.contains("NaN")
//        data(1) != "NaN"
      })

//      .map(arr => {
//        Bitcoin(arr(0).toLong, arr(1).toDouble, arr(2).toDouble,
//          arr(3).toDouble, arr(4).toDouble, arr(5).toDouble, arr(6).toDouble, arr(7).toDouble)
//      })


//    val nodes: util.HashSet[InetSocketAddress] = new util.HashSet()
//    nodes.add(new InetSocketAddress("192.168.31.8", 7001))
//    nodes.add(new InetSocketAddress("192.168.31.8", 7002))
//    nodes.add(new InetSocketAddress("192.168.31.8", 7003))
//    nodes.add(new InetSocketAddress("192.168.31.8", 7004))
//    nodes.add(new InetSocketAddress("192.168.31.8", 7005))
//    nodes.add(new InetSocketAddress("192.168.31.8", 7006))
//val jedisCluster: FlinkJedisClusterConfig = new FlinkJedisClusterConfig.Builder().setNodes(nodes).build()

//    val jedis = new FlinkJedisPoolConfig.Builder().setHost("192.168.31.8").setPort(6379).build()

//    dataStream.addSink(new RedisSink(jedis, new MyRedisSinkFun))
//    dataStream.addSink(new FlinkKafkaProducer011("hadoop102:9092", "sinktest", new SimpleStringSchema()))
    dataStream.addSink(new FlinkKafkaProducer011[String]("hadoop102:9092,hadoop103:9092,hadoop104:9092", "bitcoin-source", new SimpleStringSchema()))
    env.execute("kafka")

  }
}



//
//case class SensorReading(id: String, timestamp: Long, temperature: Double)
//
//case class MyRedisSinkFun() extends RedisMapper[Bitcoin] {
//  override def getCommandDescription: RedisCommandDescription = {
//    new RedisCommandDescription(RedisCommand.HSET, "Bitcoin")
//
//  }
//
//  override def getKeyFromData(data: Bitcoin): String = data.timestamp.toString
//
//  override def getValueFromData(data: Bitcoin): String = data.toString
//}