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

object Req4BlackListApplication {

    def main(args: Array[String]): Unit = {

        // 需求四 ： 广告黑名单实时统计

        // 问题1 ： task中使用的第三方对象没有序列化（连接对象）
        //         在Executor节点创建连接
        // 问题2 ： 黑名单的数据只取了一次
        //         希望获取数据的操作可以周期的执行（transform）
        // 问题3 ： java序列化会出现无法反序列化（transient）的问题
        //         采用广播变量来传递序列化数据

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

        //messageDStream.print()

        // TODO 0. 对原始数据进行筛选（黑名单数据过滤）
        // 从redis中获取数据
        /*
        val client: Jedis = RedisUtil.getJedisClient
        val blackList: util.Set[String] = client.smembers("blacklist")
        client.close()

        val filterDStream: DStream[MyKafkaMessage] = messageDStream.filter(message => {
            !blackList.contains(message.userid)
        })
        */



        // Coding : Driver 1
        val transformDStream: DStream[MyKafkaMessage] = messageDStream.transform(rdd => {
            // Coding : Driver n
            val client: Jedis = RedisUtil.getJedisClient
            val blackList: util.Set[String] = client.smembers("blacklist")
            // Driver => object => Driver
            //blackList.contains("1")
            // 使用广播变量来实现序列化操作
            val blackListBroadcast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(blackList)

            client.close()

            rdd.filter(message => {
                // Coding : Executor m
                // Driver ==> Executor
                !blackListBroadcast.value.contains(message.userid)
            })
        })

        // TODO 1. 将数据进行结构的转换 ：(date-adv-user, 1)
        val dateAdvUserToCountDStream: DStream[(String, Long)] = transformDStream.map(message => {
            val date: String = DateUtil.formatTime(message.timestamp.toLong, "yyyy-MM-dd")
            (date + "_" + message.adid + "_" + message.userid, 1L)
        })

        // TODO 2. 将转换结构后的数据进行聚合：(date-adv-user, sum)
        val stateDStream: DStream[(String, Long)] = dateAdvUserToCountDStream.updateStateByKey[Long] {
            (seq:Seq[Long], buffer:Option[Long]) => {
                val sum = buffer.getOrElse(0L) + seq.sum
                Option(sum)
            }
        }

        // TODO 3. 对聚合的结果进行阈值的判断
        stateDStream.foreachRDD(rdd=>{
            rdd.foreach{
                case ( key, sum ) => {
                    if ( sum >= 100 ) {
                        // TODO 4. 如果超过阈值，那么需要拉入黑名单(redis)
                        val keys: Array[String] = key.split("_")
                        val innerClient: Jedis = RedisUtil.getJedisClient
                        innerClient.sadd("blacklist", keys(2))
                        innerClient.close()
                    }
                }
            }
        })

        ssc.start()

        ssc.awaitTermination()

    }
}
