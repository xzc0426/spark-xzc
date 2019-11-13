package com.xzc.realtime

import com.xzc.common.model.MyKafkaMessage
import com.xzc.common.util.{DateUtil, MykafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Req7AdvClickWindowApplication {

  def main(args: Array[String]): Unit = {

    // 需求七 ： 统计最近一分钟的广告点击的趋势

    // 准备配置对象
    val sparkConf = new SparkConf().setAppName("Req4BlackListApplication").setMaster("local[*]")

    // 构建上下文环境对象
    val ssc = new StreamingContext(sparkConf, Seconds(5))


    ssc.sparkContext.setCheckpointDir("cp")

    // 采集kafka中的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream("ads_log", ssc)

    val messageDStream: DStream[MyKafkaMessage] = kafkaDStream.map(record => {
      val message = record.value()
      val datas: Array[String] = message.split(" ")
      MyKafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
    })

    // TODO 1. 设定窗口操作
    val windowDStream: DStream[MyKafkaMessage] = messageDStream.window(Seconds(60), Seconds(10))

    // TODO 2. 将数据进行结构的转换
    val timeDStream: DStream[(String, Long)] = windowDStream.map(message => {
      var time: String = DateUtil.formatTime(message.timestamp.toLong, "yyyy-MM-dd HH:mm:ss")

      // 12:15 ==> 12:10
      // 23:59 ==> 23:50
      // 24:00 ==> 00:00
      // 2019-06-22 23:59:07 ==> 2019-06-22 23:59:00
      // 2019-06-23 00:00:01 ==> 2019-06-23 00:00:00
      time = time.substring(0, time.length - 1) + "0"

      (time, 1L)
    })

    // TODO 3. 将转换结构后的数据进行聚合
    val timeReduceDStream: DStream[(String, Long)] = timeDStream.reduceByKey(_ + _)

    // TODO 4. 将聚合后的数据按照时间进行排序
    val sortDStream: DStream[(String, Long)] = timeReduceDStream.transform(rdd => {
      rdd.sortByKey()
    })

    // TODO 5. 打印结果
    sortDStream.print()

    ssc.start()

    ssc.awaitTermination()

  }
}
