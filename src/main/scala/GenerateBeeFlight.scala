import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsConfig

import java.time.Instant
import java.util.{Properties, UUID}

case class bee(Id: String, x: Int, y: Int, timestamp: Long)

object GenerateBeeFlight extends App {
    //Can change the number of bees here
    val N = 10
    //Can change W here
    val W = 10
    //Can change H here
    val H = 10
    val props: Properties = new Properties()
    val producer: Producer[String, String] = new KafkaProducer[String, String](props)
    var idArray = List.empty[String]

    def genId(N: Int): Int = scala.util.Random.nextInt(N)

    def genX(W: Int): Int = scala.util.Random.nextInt(W)
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put("acks", "all")
    props.put("linger.ms", 1)
    props.put("retries", 0)

    def genY(H: Int): Int = scala.util.Random.nextInt(H)
    (1 to N).foreach(i => idArray = idArray :+ UUID.randomUUID().toString)

    def genTimestamp(): Long = Instant.now.getEpochSecond

    //Can change the number of messages
    while(true) {
        Thread.sleep(100)
        val pickBee = bee(Id = idArray(genId(N)), x = genX(W), y = genX(H), timestamp = genTimestamp())
        println(s"${pickBee.Id},${pickBee.x},${pickBee.y},${pickBee.timestamp}")
        val msgg = new ProducerRecord[String, String]("events", pickBee.Id, s"${pickBee.Id},${pickBee.x},${pickBee.y},${pickBee.timestamp}")
        producer.send(msgg)
    }
    producer.flush()
    producer.close()

}
