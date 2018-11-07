package HDFSfIleProcessing
import java.util.Properties
import org.apache.kafka.clients.producer._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class kafkaProducerExcl {

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val sc1 = new SparkContext()
  val sqlContext = new SQLContext(sc1)

  val sRDD = sqlContext.read.format("csv").option("header", "false").load("hdfs://master1.maticapartners.com:8020/user/albert/test_clean.csv").rdd
  // Array[org.apache.spark.sql.Row] = Array([7896545,0,1,0,1,0,1,1,0,2018-11-06], [0865456,1,0,1,1,0,1,0,1,2018-11-06])

  val TOPIC = "test"

  def send[T](message: T) = {

    for (row <- sRDD) {
      val record = new ProducerRecord(TOPIC, "key", s"$row")
      producer.send(record)
    }
    producer.close()

  }
}
