import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer, StringSerializer}
import org.apache.kafka.streams.kstream.{Named, TimeWindowedKStream, TimeWindows}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.{KStream, KTable, Materialized, Produced}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, ByteArrayWindowStore, StreamsBuilder}
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Properties
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class Beeresult(beelist:List[Int] = List.empty)

object Beeresult{
  implicit object BeeresultSerde extends Serde[Beeresult]{
    override def serializer(): Serializer[Beeresult] = (_:String, data:Beeresult)=>{
      data.beelist.mkString(",").getBytes
    }

    override def deserializer(): Deserializer[Beeresult] = (_:String, data: Array[Byte])=>{
      val pattern = new String(data, StandardCharsets.UTF_8)
      val res = Option(pattern).filter(_.nonEmpty).map(_.split(",").map(_.toInt).toList).getOrElse(List.empty[Int])
      Beeresult(beelist = res)
    }
  }
}

/*case class Keypair(keypair:List[Int] = List.empty)

object Keypair{
  implicit object KeypairSerde extends Serde[Keypair]{
    override def serializer(): Serializer[Keypair] = (_:String, data:Keypair)=>{
      data.keypair.mkString(",").getBytes
    }

    override def deserializer(): Deserializer[Keypair] = (_:String, data: Array[Byte])=>{
      val pattern = new String(data, StandardCharsets.UTF_8)
      val res = Option(pattern).filter(_.nonEmpty).map(_.split(",").map(_.toInt).toList).getOrElse(List.empty[Int])
      Keypair(keypair = res)
    }
  }
}
*/
object CountBeeLandings extends App {
  import Serdes._

  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "velocity-application")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(AUTO_OFFSET_RESET_CONFIG, "earliest")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
  props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
  /*
  val builder = new StreamsBuilder
  val stream = builder.stream[String, String]("events")

  stream.selectKey((k:String, v:String)=>s"${v.split(",")(0)}, ${v.split(",")(1)}").mapValues((k, v)=>
    s"${v.split(",")(0)},${v.split(",")(3)}").to("mid-events")

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }


  def makeTopology(T: Int, inputTopic: String, outputTopic: String) = {
    val windowSize = Duration.ofMillis(T)
    implicit val materializer: Materialized[String, Beeresult, ByteArrayWindowStore] =
      Materialized.as[String, Beeresult](Stores.inMemoryWindowStore("bee-store", Duration.ofHours(1), windowSize, true))

    val builder1 = new StreamsBuilder
    val stream1 = builder1.stream[String, String](inputTopic)

    stream1.groupByKey.windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize)).aggregate(Beeresult())(Function.untupled{
      case(_:String, data:String, Beeresult(beelist)) if !beelist.contains(data.split(",")(0)) => Beeresult(beelist.appended(data.split(",")(0)))
    })(materializer).mapValues({
      case(key:String, value: Beeresult)=>
        val output = s"key = ${key}, list = [${value.beelist.mkString(",")}]"
        println(output)
        output
    }: (String, Beeresult) => String).to(outputTopic)

    val streams1: KafkaStreams = new KafkaStreams(builder1.build(), props)
    streams1.start()

    sys.ShutdownHookThread {
      streams1.close(Duration.ofSeconds(10))
    }
  }
  */
  val windowSize = Duration.ofMillis(100)
  implicit val materializer: Materialized[String, Beeresult, ByteArrayWindowStore] =
    Materialized.as[String, Beeresult](Stores.inMemoryWindowStore("bee-store", Duration.ofHours(1), windowSize, true))

  val builder = new StreamsBuilder
  val stream = builder.stream[String, String]("mid-events")

  stream.groupByKey.windowedBy(TimeWindows.ofSizeWithNoGrace(windowSize)).aggregate(Beeresult())(Function.untupled {
    case (_: String, data: String, Beeresult(beelist)) if !beelist.contains(data.split(",")(0)) => Beeresult(beelist.appended(data.split(",")(0).toInt))
  })(materializer).toStream.foreach((key, Beeresult)=>{
    println(key)
    println(Beeresult)
  })

  val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }
}
