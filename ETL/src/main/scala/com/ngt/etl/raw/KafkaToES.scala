package com.ngt.etl.raw


import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.util
import java.util.Properties

/**
 * @author ngt
 * @create 2020-12-20 0:54
 */

// 时间戳(秒)，开盘价格，最高价，最低价，收盘价， 交易量， 交易价值(美元)， 权重交易价格
case class CSVToES_Bitcoin(timestamp: Long, openPrice: String, highPrice: String, lowPrice: String, closePrice: String,
                           currencyBTC: String, currencyValue: String, weightedPrice: String)

object KafkaToES {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(8)

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
    properties.setProperty("group.id", "bitcoin")
    properties.setProperty("flink.partition-discovery.interval-millis", "1000")

    // 使用通配符 同时匹配多个Kafka主题
    val inputStream: DataStream[String] =
      env.addSource(new FlinkKafkaConsumer011[String](java.util.regex.Pattern.compile("bitcoin-source[0-9]"), new SimpleStringSchema(), properties))

    //    val inputStream: DataStream[String] = env.readTextFile("data/bitcoin.csv")
    val dataStream: DataStream[CSVToES_Bitcoin] = inputStream
      .filter(data => {
        !data.contains("NaN") && !data.contains("Timestamp")
      })
      .map(data => {
        val arr = data.split(",")
        CSVToES_Bitcoin(arr(0).toLong * 1000L,
          arr(1), arr(2), arr(3), arr(4), arr(5), arr(6), arr(7))
      })


    val httpHosts = new java.util.ArrayList[HttpHost]
    //    httpHosts.add(new HttpHost("192.168.100.102", 9200, "http"))
    //    httpHosts.add(new HttpHost("192.168.100.103", 9200, "http"))
    //    httpHosts.add(new HttpHost("192.168.100.104", 9200, "http"))

    httpHosts.add(new HttpHost("192.168.31.20", 9200, "http"))

    val esSinkFunc = new ElasticsearchSinkFunction[CSVToES_Bitcoin] {
      override def process(element: CSVToES_Bitcoin, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
        // 包装写入es的数据
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("timestamp", element.timestamp.toString)
        dataSource.put("openPrice", element.openPrice)
        dataSource.put("highPrice", element.highPrice)
        dataSource.put("lowPrice", element.lowPrice)
        dataSource.put("closePrice", element.closePrice)
        dataSource.put("currencyBTC", element.currencyBTC)
        dataSource.put("currencyValue", element.currencyValue)
        dataSource.put("weightedPrice", element.weightedPrice)

        // 创建一个index request
        val indexRequest = Requests.indexRequest()
          // ES 中 索引名字注意不能使用大写字母
          .index("bitcoin")
          //          .`type`("readingdata")
          .source(dataSource)

        indexer.add(indexRequest)
      }
    }
    dataStream.addSink(new ElasticsearchSink.Builder[CSVToES_Bitcoin](httpHosts, esSinkFunc).build())
    env.execute("KafkaToES")
  }
}

