package com.xzc.common.util

import java.io.InputStream
import java.util
import java.util.{Properties, ResourceBundle}

import com.alibaba.fastjson.{JSON, JSONObject}

object ConfigUtil {

    val rb =  ResourceBundle.getBundle("config")
    val condRb =  ResourceBundle.getBundle("condition")

    def main(args: Array[String]): Unit = {
        println(getValueFromCondition("startDate"))
    }

    /**
      * 根据条件获取数据
      * @param cond
      * @return
      */
    def getValueFromCondition(cond:String): String = {
        val conds = condRb.getString("condition.params.json")
        // 转换JSON格式
        val json: JSONObject = JSON.parseObject(conds)
        json.getString(cond)
    }

    /**
      * 从指定的配置文件中查找指定key的数据
      * @param key
      * @return
      */
    def getValueFromConfig(key : String): String = {
        rb.getString(key)
    }

    /**
      * 从指定的配置文件中查找指定key的数据
      * @param key
      * @return
      */
    def getValue( key : String ): String = {
        val stream: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties")

        val properties = new Properties()
        properties.load(stream)

        properties.getProperty(key)
    }
}
