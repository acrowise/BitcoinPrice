package com.ngt.etl.raw

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties

/**
 * @author ngt
 * @create 2020-12-20 1:19
 *         数据清洗，去除原始文件中的无效数据，保存到本地
 */

// 时间戳(秒)，开盘价格，最高价，最低价，收盘价， 交易量， 交易价值(美元)， 权重交易价格
case class File2CSV_Bitcoin(timestamp: Long, openPrice: String, highPrice: String, lowPrice: String, closePrice: String,
                            currencyBTC: String, currencyValue: String, weightedPrice: String)

object DataCleansingToCSV {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("group.id", "bitcoin")
    properties.setProperty("flink.partition-discovery.interval-millis", "1000")


    // 使用通配符 同时匹配多个 Kafka主题
    val inputStream: DataStream[String] =
      env.addSource(new FlinkKafkaConsumer011[String](java.util.regex.Pattern.compile("bitcoin-source[0-9]"), new SimpleStringSchema(), properties))

    val dataStream = inputStream
      .filter(data => {
        !data.contains("NaN") && !data.contains("Timestamp")
      })
      .map(data => {
        val arr: Array[String] = data.split(",")
        File2CSV_Bitcoin(arr(0).toLong * 1000L, arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7))
      })

    dataStream.writeAsCsv("data/bitcoin.csv")
    env.execute("DataCleansingToCSV")
  }
}
