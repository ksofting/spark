package com.zlst;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.alibaba.fastjson.JSONObject;
import com.zlst.entity.LogInfo;
import com.zlst.enums.LogType;
import com.zlst.function.DataFormatFunction;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * @describe:容器日志分析
 * @author 170213
 * @date 2018-03-21
 * @see
 * @since 1.0
 */
public class ContainerLogAnalyse {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Throwable {

		String brokers = "localhost:9092";
		String topics = "analyse";
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("containerAnalyse")
				.set("spark.local.dir", "e:/tmp").set("spark.streaming.kafka.maxRatePerPartition", "10");
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN");
		JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(10));
		ssc.checkpoint("/checkpoint");
		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		// kafka相关参数，必要！缺了会报错
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);
		kafkaParams.put("auto.offset.reset", "smallest");
		kafkaParams.put("group.id", "1");

		JavaPairInputDStream<String, String> dStream = KafkaUtils.createDirectStream(ssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		// json转换
		JavaDStream<LogInfo> jsons = dStream.map(new Function<Tuple2<String, String>, LogInfo>() {

			private static final long serialVersionUID = -5411752989302468800L;

			@Override
			public LogInfo call(Tuple2<String, String> v1) throws Exception {

				return DataFormatFunction.call(v1);
			}
		});
		// 容器日志过滤筛选
		JavaDStream<LogInfo> containers = jsons.filter(new Function<LogInfo, Boolean>() {

			@Override
			public Boolean call(LogInfo v1) throws Exception {

				if (v1.getLogType() == LogType.CONTAINER) {
					return true;
				}
				return false;
			}
		});

		ssc.start();
		ssc.awaitTermination();
	}

}
