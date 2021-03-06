package com.ngt.etl.price

import com.ngt.etl.sink.{ElasticsearchBitcion, MyElasticsearchSink}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.flink.streaming.api.scala._

/**
 * @author ngt
 * @create 2020-12-21 2:29
 */

object TimePriceToElasticsearch {
  def main(args: Array[String]): Unit = {
    val dayPriceFile = "data_time_price/DayPrice.csv"
    val hourPriceFile = "data_time_price/HourPrice.csv"
    val minutePriceFile = "data_time_price/MinutePrice.csv"

    /*
    val dayIndexES: String = "price-day"
    val hourIndexES: String = "price-hour"
    val minuteIndexES: String = "price-minute"
     */

    // 执行前请先检查 index 值
    toES(hourPriceFile)
  }

  def toES(filePath: String) = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(8)
    val InputStream = env.readTextFile(filePath)
    val dataStram: DataStream[ElasticsearchBitcion] = InputStream
      .map(data => {
        val strings: Array[String] = data.split(",")
        ElasticsearchBitcion(strings(0), strings(1), strings(2), strings(3))
      })
    val sink: MyElasticsearchSink = new MyElasticsearchSink
    dataStram.addSink(new ElasticsearchSink.Builder[ElasticsearchBitcion](sink.httpHosts, sink.esSinkFunc).build())
    env.execute("TimePriceToElasticsearch")
  }
}
