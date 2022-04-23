import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.{ByteArrayWindowStore, StreamsBuilder}
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Properties

case class Beeresult(beelist: List[String] = List.empty)

object Beeresult {
    implicit object BeeresultSerde extends Serde[Beeresult] {
        override def serializer(): Serializer[Beeresult] = (_: String, data: Beeresult) => {
            data.beelist.mkString(",").getBytes
        }

        override def deserializer(): Deserializer[Beeresult] = (_: String, data: Array[Byte]) => {
            val pattern = new String(data, StandardCharsets.UTF_8)
            val res = Option(pattern).filter(_.nonEmpty).map(_.split(",").map(_.toString).toList).getOrElse(List.empty[String])
            Beeresult(beelist = res)
        }
    }
}

object CountBeeLandings extends App {

    import Serdes._

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "velocity-application")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.stringSerde.getClass)
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)


    val streams: KafkaStreams = new KafkaStreams(makeCountBeeLandingsTopology(10, "events", "bee-counts"), props)

    def makeCountBeeLandingsTopology(T: Int, inputTopic: String, outputTopic: String) = {
        implicit val materializer: Materialized[String, Beeresult, ByteArrayWindowStore] =
            Materialized.as[String, Beeresult](Stores.inMemoryWindowStore("bee-store", Duration.ofHours(1), Duration.ofMillis(T), true))

        val builder = new StreamsBuilder
        val stream = builder.stream[String, String](inputTopic)

        stream.selectKey((k: String, v: String) => s"${v.split(",")(1)}, ${v.split(",")(2)}").mapValues((k, v) =>
            s"${v.split(",")(0)},${v.split(",")(3)}").groupByKey.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMillis(T))).aggregate(Beeresult())(Function.untupled {
            case (_: String, data: String, Beeresult(beelist)) if !beelist.contains(data.split(",")(0)) => Beeresult(beelist.appended(data.split(",")(0)))
        })(materializer).toStream.mapValues((window, Beeresult) => {
            val output = Beeresult.beelist.size.toString
            println(output)
            output
        }).to(outputTopic)
        builder.build()
    }

    streams.start()

    sys.ShutdownHookThread {
        streams.close(Duration.ofSeconds(10))
    }

}
