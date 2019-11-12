package com.xzc.realtime

import java.util

import com.xzc.common.util.{DateUtil, MykafkaUtil, RedisUtil}
import com.xzc.common.model.MyKafkaMessage
import com.xzc.common.util.{DateUtil, MykafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object Req5DateAreaCityAdvClickApplication {

    def main(args: Array[String]): Unit = {

        // 需求四 ： 广告黑名单实时统计

        // 准备配置对象
        val sparkConf = new SparkConf().setAppName("Req4BlackListApplication").setMaster("local[*]")

        // 构建上下文环境对象
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        //StreamingContext.getActiveOrCreate()

        ssc.sparkContext.setCheckpointDir("cp")

        // 采集kafka中的数据
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream("ads_log", ssc)

        val messageDStream: DStream[MyKafkaMessage] = kafkaDStream.map(record => {
            val message = record.value()
            val datas: Array[String] = message.split(" ")
            MyKafkaMessage(datas(0), datas(1), datas(2), datas(3), datas(4))
        })

        // TODO 1. 将数据进行结构的转换 ：(date-area-city-adv, 1)
        val dateAdvUserToCountDStream: DStream[(String, Long)] = messageDStream.map(message => {
            val date: String = DateUtil.formatTime(message.timestamp.toLong, "yyyy-MM-dd")
            (date + "_" + message.area + "_" + message.city + "_" + message.adid, 1L)
        })

        // TODO 2. 将转换结构后的数据进行聚合：(date-area-city-adv, sum)
        val dateAdvUserToSumDStream: DStream[(String, Long)] = dateAdvUserToCountDStream.reduceByKey(_+_)

        // TODO 3. 对聚合的结果进行阈值的判断
        dateAdvUserToSumDStream.foreachRDD(rdd=>{
            rdd.foreachPartition(datas=>{

                val innerClient: Jedis = RedisUtil.getJedisClient

                datas.foreach {
                    case (key, sum) => {
                        innerClient.hincrBy("date:area:city:ads", key, sum)
                    }
                }

                innerClient.close()
            })
        })

        ssc.start()

        ssc.awaitTermination()

    }
}
