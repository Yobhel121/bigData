package cn.spark.study.streaming;

import java.util.*;

import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;

import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

/**
 * 基于Kafka receiver方式的实时wordcount程序
 *
 * @author Administrator
 */
public class KafkaReceiverWordCount {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("KafkaWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        String brokers = "192.168.2.107:9092,192.168.2.108:9092,192.168.2.109:9092";
        String topics = "TestTopic";

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put("group.id", "DefaultConsumerGroup");
//		kafkaParams.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> lines = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        JavaDStream<String> words = lines.flatMap(
                new FlatMapFunction<ConsumerRecord<String, String>, String>() {
                    @Override
                    public Iterator<String> call(ConsumerRecord<String, String> record) throws Exception {
                        return Arrays.asList(record.value().split(" ")).iterator();
                    }
                }
        );


//		// 使用KafkaUtils.createStream()方法，创建针对Kafka的输入数据流
//		Map<String, Integer> topicThreadMap = new HashMap<String, Integer>();
//		topicThreadMap.put("WordCount", 1);
//
//		JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(
//				jssc,
//				"192.168.2.107:2181,192.168.2.108:2181,192.168.2.109:2181",
//				"DefaultConsumerGroup",
//				topicThreadMap);
//
//		// 然后开发wordcount逻辑
//		JavaDStream<String> words = lines.flatMap(
//
//				new FlatMapFunction<Tuple2<String,String>, String>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Iterable<String> call(Tuple2<String, String> tuple)
//							throws Exception {
//						return Arrays.asList(tuple._2.split(" "));
//					}
//
//				});

        JavaPairDStream<String, Integer> pairs = words.mapToPair(

                new PairFunction<String, String, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(String word)
                            throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }

                });

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(

                new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }

                });

        wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }


}
