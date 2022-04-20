import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig

import java.time.Instant
import java.util.Properties

case class bee(Id: Int, x: Int, y: Int, timestamp: Long)

object GenerateBeeFlight extends App {
    val N = 10
    val W = 10
    val H = 10

    def genId(N: Int):Int = scala.util.Random.nextInt(N)
    def genX(W: Int):Int = scala.util.Random.nextInt(W)
    def genY(H: Int):Int = scala.util.Random.nextInt(H)
    def genTimestamp():Long = Instant.now.getEpochSecond

    val props: Properties = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put("acks", "all")
    props.put("linger.ms", 1)
    props.put("retries", 0)

    val producer: Producer[String, String] = new KafkaProducer[String, String](props)
    (1 to 100).foreach { i =>
        Thread.sleep(100)
        val pickBee = bee(Id = genId(N), x = genX(W), y = genX(H), timestamp = genTimestamp())
        println(s"${pickBee.Id}, ${pickBee.x},${pickBee.y},${pickBee.timestamp}")
        val msgg = new ProducerRecord[String, String]("events", pickBee.Id.toString, s"${pickBee.Id}, ${pickBee.x},${pickBee.y},${pickBee.timestamp}")
        producer.send(msgg)
    }
    producer.flush()
    producer.close()

}
