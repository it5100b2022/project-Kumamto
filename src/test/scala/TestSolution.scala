import org.apache.kafka.streams.scala.serialization.Serdes
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.apache.kafka.streams.{KeyValue, StreamsConfig, TopologyTestDriver}
import scala.jdk.CollectionConverters._

class TestSolution extends AnyFunSuite {
    import Serdes.stringSerde
    import CountBeeLandings.makeCountBeeLandingsTopology
    import LongDistanceFlyers.makeLongDistanceFlyersTopology

    test("The CountBeeLanding solution is correct") {
        val props = new java.util.Properties()
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

        val testDriver = new TopologyTestDriver(makeCountBeeLandingsTopology(10, "events", "bee-counts"), props)

        val testInputTopic = testDriver.createInputTopic("events", stringSerde.serializer(), stringSerde.serializer())
        val testOutputTopic = testDriver.createOutputTopic("bee-counts", stringSerde.deserializer(), stringSerde.deserializer())
        val events = Array(
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,6,8,1650598588",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a,1,6,1650598589",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,4,7,1650598589",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,0,6,1650598589",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,3,6,1650598589",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,8,9,1650598589",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,3,1,1650598589",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,3,7,1650598589",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,9,0,1650598590",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,6,6,1650598590",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,0,4,1650598590",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb,7,0,1650598590",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1,6,5,1650598590",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1,3,3,1650598590",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,6,6,1650598590",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,3,7,1650598590",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,8,7,1650598590",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,3,8,1650598591",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb,7,0,1650598591",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,1,9,1650598591",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,9,4,1650598591",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a,3,2,1650598591",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,9,3,1650598591",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,0,3,1650598591",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,6,4,1650598591",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb,6,8,1650598591",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,1,6,1650598592",
            "fdcd8f34-564e-4c06-9bd1-ffd7c57f5303,1,8,1650598592",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a,2,5,1650598592",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,2,8,1650598592",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,1,7,1650598592",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb,8,5,1650598592",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,0,3,1650598592",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,1,5,1650598592",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,5,3,1650598592",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb,4,5,1650598593",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a,4,8,1650598593",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,5,5,1650598593",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1,8,9,1650598593",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a,1,0,1650598593",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a,7,9,1650598593",
            "75373190-da69-411a-8a5d-07a428c7ee19,1,1,1650598593",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,4,5,1650598593",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,5,2,1650598593",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,1,5,1650598593",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1,9,5,1650598594",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,0,8,1650598594",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,4,4,1650598594",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a,5,1,1650598594",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,1,3,1650598594",
            "fdcd8f34-564e-4c06-9bd1-ffd7c57f5303,5,7,1650598594",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,3,0,1650598594",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1,6,5,1650598594",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1,9,8,1650598594",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,9,8,1650598595",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,6,7,1650598595",
            "75373190-da69-411a-8a5d-07a428c7ee19,6,9,1650598595",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,5,7,1650598595",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1,2,3,1650598595",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a,8,0,1650598595",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1,5,7,1650598595",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb,8,0,1650598595",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,5,9,1650598595",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,3,1,1650598596",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1,5,3,1650598596",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,2,7,1650598596",
            "75373190-da69-411a-8a5d-07a428c7ee19,3,3,1650598596",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,1,9,1650598596",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,1,2,1650598596",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a,9,7,1650598596",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,4,2,1650598596",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,6,7,1650598596",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb,5,1,1650598597",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,9,3,1650598597",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,1,9,1650598597",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,8,6,1650598597",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,8,3,1650598597",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,7,6,1650598597",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,5,2,1650598597",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,9,0,1650598597",
            "fdcd8f34-564e-4c06-9bd1-ffd7c57f5303,5,1,1650598597",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,5,4,1650598597",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,7,2,1650598598",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,1,0,1650598598",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,3,1,1650598598",
            "75373190-da69-411a-8a5d-07a428c7ee19,0,0,1650598598",
            "fdcd8f34-564e-4c06-9bd1-ffd7c57f5303,7,0,1650598598",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,2,4,1650598598",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,3,7,1650598598",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,9,3,1650598598",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,3,0,1650598598",
            "fdcd8f34-564e-4c06-9bd1-ffd7c57f5303,5,6,1650598599",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,1,7,1650598599",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,9,2,1650598599",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,8,4,1650598599",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,0,8,1650598599",
            "fdcd8f34-564e-4c06-9bd1-ffd7c57f5303,2,9,1650598599",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,3,2,1650598599",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb,1,5,1650598599",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb,7,8,1650598599"
        )
        Option(testDriver.getKeyValueStore[String, String]("bee-store")).foreach(store => events.foreach(store.delete))
        val inputVals = (0 to 99).map { i =>
            new KeyValue(events(i).split(",")(0), events(i))
        }.toList.asJava
        testInputTopic.pipeKeyValueList(inputVals)
        testOutputTopic.readValuesToList().asScala shouldBe List.fill(100)("1")
    }

    test("The LongDistanceFlyers solution is correct") {
        val props = new java.util.Properties()
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test")
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

        val testDriver = new TopologyTestDriver(makeLongDistanceFlyersTopology(10, "events", "long-distance-travellers"), props)

        val testInputTopic = testDriver.createInputTopic("events", stringSerde.serializer(), stringSerde.serializer())
        val testOutputTopic = testDriver.createOutputTopic("long-distance-travellers", stringSerde.deserializer(), stringSerde.deserializer())
        val events = Array(
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,6,8,1650598588",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a,1,6,1650598589",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,4,7,1650598589",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,0,6,1650598589",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,3,6,1650598589",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,8,9,1650598589",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,3,1,1650598589",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,3,7,1650598589",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,9,0,1650598590",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,6,6,1650598590",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,0,4,1650598590",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb,7,0,1650598590",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1,6,5,1650598590",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1,3,3,1650598590",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,6,6,1650598590",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,3,7,1650598590",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,8,7,1650598590",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,3,8,1650598591",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb,7,0,1650598591",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,1,9,1650598591",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,9,4,1650598591",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a,3,2,1650598591",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,9,3,1650598591",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,0,3,1650598591",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,6,4,1650598591",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb,6,8,1650598591",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,1,6,1650598592",
            "fdcd8f34-564e-4c06-9bd1-ffd7c57f5303,1,8,1650598592",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a,2,5,1650598592",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,2,8,1650598592",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,1,7,1650598592",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb,8,5,1650598592",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,0,3,1650598592",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,1,5,1650598592",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,5,3,1650598592",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb,4,5,1650598593",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a,4,8,1650598593",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,5,5,1650598593",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1,8,9,1650598593",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a,1,0,1650598593",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a,7,9,1650598593",
            "75373190-da69-411a-8a5d-07a428c7ee19,1,1,1650598593",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,4,5,1650598593",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,5,2,1650598593",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,1,5,1650598593",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1,9,5,1650598594",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,0,8,1650598594",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,4,4,1650598594",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a,5,1,1650598594",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,1,3,1650598594",
            "fdcd8f34-564e-4c06-9bd1-ffd7c57f5303,5,7,1650598594",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,3,0,1650598594",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1,6,5,1650598594",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1,9,8,1650598594",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,9,8,1650598595",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,6,7,1650598595",
            "75373190-da69-411a-8a5d-07a428c7ee19,6,9,1650598595",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,5,7,1650598595",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1,2,3,1650598595",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a,8,0,1650598595",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1,5,7,1650598595",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb,8,0,1650598595",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,5,9,1650598595",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,3,1,1650598596",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1,5,3,1650598596",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,2,7,1650598596",
            "75373190-da69-411a-8a5d-07a428c7ee19,3,3,1650598596",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,1,9,1650598596",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,1,2,1650598596",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a,9,7,1650598596",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,4,2,1650598596",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,6,7,1650598596",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb,5,1,1650598597",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,9,3,1650598597",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,1,9,1650598597",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,8,6,1650598597",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,8,3,1650598597",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,7,6,1650598597",
            "2e35c657-ccd9-4598-accc-b50ce778a57b,5,2,1650598597",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,9,0,1650598597",
            "fdcd8f34-564e-4c06-9bd1-ffd7c57f5303,5,1,1650598597",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,5,4,1650598597",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,7,2,1650598598",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,1,0,1650598598",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,3,1,1650598598",
            "75373190-da69-411a-8a5d-07a428c7ee19,0,0,1650598598",
            "fdcd8f34-564e-4c06-9bd1-ffd7c57f5303,7,0,1650598598",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,2,4,1650598598",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,3,7,1650598598",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,9,3,1650598598",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef,3,0,1650598598",
            "fdcd8f34-564e-4c06-9bd1-ffd7c57f5303,5,6,1650598599",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,1,7,1650598599",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,9,2,1650598599",
            "73e292ad-547a-42b2-b78c-7f304e40cb19,8,4,1650598599",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9,0,8,1650598599",
            "fdcd8f34-564e-4c06-9bd1-ffd7c57f5303,2,9,1650598599",
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7,3,2,1650598599",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb,1,5,1650598599",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb,7,8,1650598599"
        )
        Option(testDriver.getKeyValueStore[String, String]("long-store")).foreach(store => events.foreach(store.delete))
        val inputVals = (0 to 99).map { i =>
            new KeyValue(events(i).split(",")(0), events(i))
        }.toList.asJava
        testInputTopic.pipeKeyValueList(inputVals)
        testOutputTopic.readValuesToList().asScala shouldBe List(
            "02b24eba-44dd-43f4-b3f4-e04f13a4d3e7",
            "c6e400ee-9967-4a60-8130-e5b99bdc3ce9",
            "73e292ad-547a-42b2-b78c-7f304e40cb19",
            "bb9b4a6b-7c90-42a5-9ecd-d44c0afe84ef",
            "3e8be161-31cc-4b45-89ca-f40653be0fe1",
            "ec2c0d79-0cfd-4d3a-b41b-80001316908a",
            "ccd87bc1-addd-4988-a609-5ccbc30343cb",
            "2e35c657-ccd9-4598-accc-b50ce778a57b",
            "fdcd8f34-564e-4c06-9bd1-ffd7c57f5303",
            "75373190-da69-411a-8a5d-07a428c7ee19",
        )
    }
}
