import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.streams.StreamsConfig

import java.sql._
import java.util.{Properties, Collections}
import scala.collection.JavaConverters._

object SaveLongDistanceFlyers extends App {
  val dbConn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "19981209")

  val props: Properties = new Properties()
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "wordstore-consumer")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty("enable.auto.commit", "true")
  props.setProperty("auto.commit.interval.ms", "1000")

  val consumer = new KafkaConsumer[String, String](props)
  val topic = "long-distance-travellers"
  try{
    consumer.subscribe(java.util.Collections.singletonList(topic))
    while(true){
      val records = consumer.poll(10)
      for (record<-records.asScala){
        println("Topic: " + record.topic() +
            ",Key: " + record.key())
        var sql:String = "INSERT INTO longflyers (id)"+"Values(?)"
        val stm = dbConn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
        stm.setString(1, record.key())
        stm.executeUpdate()
      }
    }
  }catch {
    case e:Exception => e.printStackTrace()
  }
  finally {
    consumer.close()
  }

  dbConn.close()
}
