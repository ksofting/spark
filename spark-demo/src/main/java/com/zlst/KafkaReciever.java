package com.zlst;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class KafkaReciever {

	public static void main(String[] args) throws Exception {
		String brokers = "localhost:9092";
        String topics = "sparkTest";
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("streaming word count").set("spark.local.dir", "e:/tmp")
        	      .set("spark.streaming.kafka.maxRatePerPartition", "10");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        //kafka相关参数，必要！缺了会报错
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers) ;
        kafkaParams.put("auto.offset.reset", "smallest");
        kafkaParams.put("group.id", "1");
        //Topic分区
        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(ssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
        //单词分解
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String,String>, String>(){
        	private static final long serialVersionUID = 1L;

        	@Override
        	public Iterator<String> call(Tuple2<String,String> tuple) throws Exception {
        	return Arrays.asList(tuple._2.split(" ")).iterator();
        	}
        });
        words.print();
        //为单词进行计数
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>(){
        	private static final long serialVersionUID = 1L;

        	@Override
        	public Tuple2<String, Integer> call(String word) throws Exception {
        	return new Tuple2<String, Integer>(word, 1);
        	}
        });
        pairs.print();
        //分组进行计数累加
        JavaPairDStream<String, Integer> wordcounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>(){
        	private static final long serialVersionUID = 1L;

        	@Override
        	public Integer call(Integer v1, Integer v2) throws Exception {
        	return v1 + v2;
        	}
        });
//        km.updateZK(lines.inputDStream());
        //将结果打印到控制台
        wordcounts.print();
        ssc.start();
        ssc.awaitTermination();
	}
}
