package cn.spark.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HivetoKafka {

  // "select 1 as id, '[1,2,3]' as shops" 115.238.44.226:39999 test
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage <hive sql> <kafka bootstrap servers> <topic>")
      sys.exit(1)
    }
    val sql = args(0)
    val kafkaServers = args(1)
    val topic = args(2)

    val conf = new SparkConf()
      //      .setMaster("local")
      .setAppName("HivetoKafka")
    val sc = SparkContext.getOrCreate(conf)
    val hc = new HiveContext(sc)

    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", kafkaServers)
        p.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        p.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        /*p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers)
         //增加延迟
         p.setProperty("linger.ms", "500")
         //这意味着leader需要等待所有备份都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据。这是最强的保证。，
         p.setProperty("acks", "all")
         //无限重试，直到你意识到出现了问题，设置大于0的值将使客户端重新发送任何数据，一旦这些数据发送失败。注意，这些重试与客户端接收到发送错误时的重试没有什么不同。允许重试将潜在的改变数据的顺序，如果这两个消息记录都是发送到同一个partition，则第一个消息失败第二个发送成功，则第二条消息会比第一条消息出现要早。
         p.setProperty("retries", Integer.MAX_VALUE+"")
         p.setProperty("reconnect.backoff.ms ", "20000");
         p.setProperty("retry.backoff.ms", "20000");
         //关闭unclean leader选举，即不允许非ISR中的副本被选举为leader，以避免数据丢失
         p.setProperty("unclean.leader.election.enable", "false");*/
        p
      }
      sc.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }
    val result: Dataset[String] = hc.sql(sql).toJSON
    result.foreachPartition(rdd => {
      rdd.foreach(record => {
        kafkaProducer.value.send(topic, record).get()
      })
    })

    kafkaProducer.value.producer.flush()
    kafkaProducer.value.producer.close()
    sc.stop()
  }
}
