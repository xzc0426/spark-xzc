package com.xzc.offline

import com.xzc.common.util.{ConfigUtil, DateUtil, StringUtil}
import com.xzc.common.model.UserVisitAction
import com.xzc.common.util.{ConfigUtil, DateUtil, StringUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

// 需求八 ： 统计每个页面平均停留时间
object Req8PageAvgAccessTimeApplication {

    def main(args: Array[String]): Unit = {

        // 准备上下文环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req2CategoryTop10SessionTop10Application")
        val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
        sparkSession.sparkContext.setCheckpointDir("cp")

        import sparkSession.implicits._

        sparkSession.sql("use " + ConfigUtil.getValueFromConfig("hive.database"))

        var sql = "select * from user_visit_action where 1 = 1 "

        val startDate = ConfigUtil.getValueFromCondition("startDate")
        val endDate = ConfigUtil.getValueFromCondition("endDate")

        if (StringUtil.isNotEmpty(startDate)) {
            sql = sql + " and date >='" + startDate + "'"
        }

        if (StringUtil.isNotEmpty(endDate)) {
            sql = sql + " and date <='" + endDate + "'"
        }

        val df: DataFrame = sparkSession.sql(sql)
        val ds: Dataset[UserVisitAction] = df.as[UserVisitAction]
        val userVisitActionRDD: RDD[UserVisitAction] = ds.rdd

        // TODO 1. 将数据使用session进行分组
        val sessionRDD: RDD[(String, Iterable[UserVisitAction])] = userVisitActionRDD.groupBy(action=>{action.session_id})

        // TODO 2. 将分组后的数据使用时间排序，保证按照页面的真正流转顺序
        val sessionToListRDD: RDD[(String, List[(Long, Long)])] = sessionRDD.mapValues(datas => {
            val actions: List[UserVisitAction] = datas.toList.sortWith {
                (left, right) => {
                    left.action_time < right.action_time
                }
            }
            // (A,timeA), (B,timeB), (C,timeC)
            val pageToTimeList: List[(Long, String)] = actions.map(action => {
                (action.page_id, action.action_time)
            })

            // TODO 3. 将页面流转的操作进行拉链处理 : ((A,timeA), (B,timeB)), ((B,timeB), (C,timeC))
            val page1ToPage2ZipList: List[((Long, String), (Long, String))] = pageToTimeList.zip(pageToTimeList.tail)

            // TODO 4. 对拉链后的数据进行计算：(A, (timeB-timeA)), (B, (timeC-timeB))
            page1ToPage2ZipList.map {
                case (page1, page2) => {

                    val page1Time = DateUtil.getTimestamp(page1._2, "yyyy-MM-dd HH:mm:ss")
                    val page2Time = DateUtil.getTimestamp(page2._2, "yyyy-MM-dd HH:mm:ss")

                    (page1._1, (page2Time - page1Time))
                }
            }
        })

        val listRDD: RDD[List[(Long, Long)]] = sessionToListRDD.map {
            case (session, list) => list
        }

        val pageToTimeRDD: RDD[(Long, Long)] = listRDD.flatMap(list=>list)
        // TODO 5. 对计算结果进行分组：(A, Iterator[ (timeB-timeA), (timeC-timeA) ])
        val pageGroupRDD: RDD[(Long, Iterable[Long])] = pageToTimeRDD.groupByKey()

        // TODO 6. 获取最终结果：(A, sum/size)
        pageGroupRDD.foreach{
            case ( pageid, datas ) => {
                println( pageid + "=" + (datas.sum / datas.size) )
            }
        }

        // 释放资源
        sparkSession.close()

    }
}