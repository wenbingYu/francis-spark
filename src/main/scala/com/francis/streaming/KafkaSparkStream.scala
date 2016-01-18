package com.francis.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by yuwenbing on 15/12/1.
 */


object KafkaSparkStream {


  def main(args: Array[String]) {


    val sc = new SparkContext(new SparkConf().setAppName("DirectKafkaWordCount"))

    val ssc = new StreamingContext(sc, Seconds(5))

    val brokers = "192.168.145.201:9092," +
      "192.168.145.202:9092," +
      "192.168.145.203:9092," +
      "192.168.145.204:9092," +
      "192.168.145.205:9092," +
      "192.168.145.206:9092," +
      "192.168.145.207:9092," +
      "192.168.145.208:9092," +
      "192.168.145.209:9092," +
      "192.168.145.210:9092";

    println("-----------------" + brokers)

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)


    val topicSet = Set("imp")


    val impDStreamRdd = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)

    val message = impDStreamRdd.map(_._2);

    message.print()

    ssc.start()
    ssc.awaitTermination()


  }

}
