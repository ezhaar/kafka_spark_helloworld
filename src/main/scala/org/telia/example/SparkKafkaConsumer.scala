/**
  * Created by izhar on 3/30/17.
  */

package org.telia.example

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._

object SparkKafkaConsumer {
  def main(args: Array[String]): Unit = {
//    val bootstrap_server = args(0).toString
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "kafka:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      // unique group id for consumer group
      "group.id" -> "g1",
      // where will the consumer start picking up msgs (earliest, latest)
      "auto.offset.reset" -> "earliest",
      // auto commit every 5 seconds or manual commit
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val conf = new SparkConf().setAppName("kafkaTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val words = stream.map(_.value()).flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }

}
