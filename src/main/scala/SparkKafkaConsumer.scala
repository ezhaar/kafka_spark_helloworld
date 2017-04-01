/**
  * Created by izhar on 3/30/17.
  */

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark._
import org.apache.spark.streaming._

object SparkKafkaConsumer {
  def main(args: Array[String]): Unit = {
    // is it possible to have more consumers than the number of partitions, in the same group?
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "172.18.0.3:9092, 172.18.0.4:9092, 172.18.0.5:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "g1",
      "auto.offset.reset" -> "none",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val conf = new SparkConf().setAppName("kafkaTest").setMaster("local")
    val ssc = new StreamingContext(conf, Seconds(1))
    val topics = Array("testP3")
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
