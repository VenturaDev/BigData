package HDFSfIleProcessing

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.parsing.json.JSONObject

class KafkaMgrProducer(topic: String, brokers : String, clientId: String) extends Serializable {

  //kafka properties.
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("client.id", clientId)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  //Creates a new producer with the given properties.
  val producer = new KafkaProducer[String, String](props)

  def sendMessage(message: String): Unit =
  {
    val data = new ProducerRecord[String, String](topic, "", message)
    producer.send(data)
    producer.close()
  }

  def createExclMessage(data:String, idLoad : String, fileName: String, fechaProc: String) : JSONObject = {

    val json = JSONObject.apply(Map(
      "data" -> data,
      "filename" -> fileName,
      "fecha_proc" -> fechaProc,
      "idLoad" -> idLoad)
    )
    json
  }
}
