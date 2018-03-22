package com.zlst

import org.apache.log4j.LogManager
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import kafka.serializer.StringDecoder

/**
 * @author litaoxiao
 *
 */
object ConsumerMain extends Serializable {
  @transient lazy val log = LogManager.getRootLogger
  def functionToCreateContext(): StreamingContext = {
    System.setProperty("logfilename", "demo")
    val sparkConf = new SparkConf().setAppName("WordFreqConsumer")
      .setMaster("local[2]")
      .set("spark.local.dir", "e:/tmp")
      .set("spark.streaming.kafka.maxRatePerPartition", "10")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    // Create direct kafka stream with brokers and topics
    val topicsSet = "sparkTest".split(",").toSet
    val kafkaParams = scala.collection.immutable.Map[String, String]("metadata.broker.list" -> "localhost:9092", "auto.offset.reset" -> "smallest", "group.id" -> "2")
    val km = new KafkaManager(kafkaParams)
    val kafkaDirectStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    log.info(s"Initial Done***>>>")

   // kafkaDirectStream.cache
    //do something......
    val lines = kafkaDirectStream.map(_._2)  
    val words = lines.flatMap(_.split(" "))  
    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)  
    wordCounts.saveAsTextFiles("e:/spark");
    //更新zk中的offset
    kafkaDirectStream.foreachRDD(rdd => {
      if (!rdd.isEmpty)
        km.updateZKOffsets(rdd)
    })

    ssc
  }


  def main(args: Array[String]) {
    val ssc = functionToCreateContext()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}