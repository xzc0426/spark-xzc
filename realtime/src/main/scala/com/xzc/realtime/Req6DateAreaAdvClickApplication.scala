package com.xzc.realtime

import com.xzc.common.model.MyKafkaMessage
import com.xzc.common.util.{DateUtil, MykafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods
import redis.clients.jedis.Jedis

object Req6DateAreaAdvClickApplication {

  def main(args: Array[String]): Unit = {

    // 需求六 ： 每天各地区 top3 热门广告

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

    // TODO 1. 将数据进行结构的转换
    val dateAdvUserToCountDStream: DStream[(String, Long)] = messageDStream.map(message => {
      val date: String = DateUtil.formatTime(message.timestamp.toLong, "yyyy-MM-dd")
      (date + "_" + message.area + "_" + message.city + "_" + message.adid, 1L)
    })

    // TODO 2. 将转换结构后的数据进行聚合 ： (date-area-city-adv, sum)
    val stateDStream: DStream[(String, Long)] = dateAdvUserToCountDStream.updateStateByKey[Long] {
      (seq: Seq[Long], buffer: Option[Long]) => {
        val sum = buffer.getOrElse(0L) + seq.sum
        Option(sum)
      }
    }

    // TODO 3. 将数据进行结构的转换 (date-area-city-adv, sum) =>  (date-area-adv, sum)
    val mapDStream: DStream[(String, Long)] = stateDStream.map {
      case (key, sum) => {
        val keys: Array[String] = key.split("_")
        (keys(0) + "_" + keys(1) + "_" + keys(3), sum)
      }
    }

    // TODO 4. 将转换结构后的数据进行聚合 :  (date-area-adv, totalSum)
    val totalDStream: DStream[(String, Long)] = mapDStream.reduceByKey(_ + _)

    // TODO 5. 将聚合后的数据进行结构的转换 ： (date-area-adv, totalSum) =>  (date-area, (adv, totalSum))
    val dateAreaToAdvSumDStream: DStream[(String, (String, Long))] = totalDStream.map {
      case (key, totalSum) => {
        val keys: Array[String] = key.split("_")

        (keys(0) + "_" + keys(1), (keys(2), totalSum))
      }
    }

    // TODO 6. 对转换结构后的数据进行分组
    val groupDStream: DStream[(String, Iterable[(String, Long)])] = dateAreaToAdvSumDStream.groupByKey()

    // TODO 7. 对广告数据进行排序，取前三名
    val resultDStream: DStream[(String, Map[String, Long])] = groupDStream.mapValues(datas => {
      datas.toList.sortWith {
        (left, right) => {
          left._2 > right._2
        }
      }.take(3).toMap
    })

    // TODO 8. 将聚合结果保存到redis中
    resultDStream.foreachRDD(rdd => {
      rdd.foreachPartition(datas => {

        val innerClient: Jedis = RedisUtil.getJedisClient

        datas.foreach(data => {

          val keys = data._1.split("_")
          val date = keys(0)
          val area = keys(1)
          val map = data._2

          // list => json
          // [{}, {}, {}]
          // {"xx":10}
          import org.json4s.JsonDSL._
          val listJson = JsonMethods.compact(JsonMethods.render(map))
          innerClient.hset("top3_ads_per_day:" + date, area, listJson)
        })

        innerClient.close()
      })
    })


    ssc.start()

    ssc.awaitTermination()

  }
}
