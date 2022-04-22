import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.streams.StreamsConfig

import java.sql._
import java.time.Duration
import java.util.{Properties}
import scala.collection.JavaConverters._

object SaveLongDistanceFlyers extends App {


  val props: Properties = new Properties()
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "beestore-consumer")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  props.setProperty("enable.auto.commit", "true")
  props.setProperty("auto.commit.interval.ms", "1000")

  val consumer = new KafkaConsumer[String, String](props)
  val topic = "long-distance-travellers"
  consumer.subscribe(List(topic).asJava)

  val dbConn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "19981209")
  var stm: PreparedStatement = dbConn.prepareStatement("INSERT INTO longflyers(id) values(?)")
  try {
    while (true) {
      val records = consumer.poll(Duration.ofMillis(10))
      for (record <- records.asScala) {
        println(record.value())
        stm.setString(1, record.key())
        stm.executeQuery()
      }
    }
  }
  catch {
    case e:Exception => e.printStackTrace()
  }
  finally {
    consumer.close()
    dbConn.close()
  }

}
