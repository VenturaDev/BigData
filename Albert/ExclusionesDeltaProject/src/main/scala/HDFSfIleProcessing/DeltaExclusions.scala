package HDFSfIleProcessing

import java.util.Calendar
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object DeltaExclusions {
  /**
    * val loadPath = "hdfs://master1.maticapartners.com:8020/user/albert/test.csv"
    * val savePath = "hdfs://master1.maticapartners.com:8020/user/albert/test_clean5.csv"
    */
  //def main(hdfsLoadPath: String, hdfsSavePath: String): Unit = {

  def main(args: Array[String]): Unit = {

    val hdfsLoadPath = args(0)
    val hdfsSavePath = args(1)

    //init spark and sql contexts.
    val sc = new SparkContext()
    val sqlc = new SQLContext(sc)

    //params: SparkContext, SQLContext, hdfs path where the file is located.
    val hdfsReaded = new HDFSReader(sc, sqlc, hdfsLoadPath)

    //params: SparkContext, SQLContext, Dataframe to process, hdfs path where to store the processed file.
    val hdfsProcessed = new HDFSProcessing(sc, sqlc, hdfsReaded.oldFileDataFrame, hdfsSavePath)

    //params: SparkContext, SQLContext, path where the processed file is stored(could be just a val stored in the hdfsprocessed class).
    //This class creates a RDD[Array[Strings]] from the given path.
    val hbaseExcluPutted = new hBaseExcluPutter(sc, sqlc, hdfsSavePath)

    //params: Column family, rdd with the data to insert/update.
    //Calling this function will send to the target table the data in the RDD[Array[Strings]]
    hbaseExcluPutted.insertLines("Albert_Exclusiones_Test", "master")

    //params: The target topic, target broker IP, target clientId
    val newKakfaProducer = new KafkaMgrProducer("test_topic", "62.210.245.152:9092", "Albert_App")

    //idLoad will be a string reporting the delivered kafka event timestamp.
    val now = Calendar.getInstance()
    val idLoad = now.toInstant.getEpochSecond.toString

    //This val is the JSON created by the createExclMessage function with the necessary information to the transmitted in kafka.
    val message = newKakfaProducer.createExclMessage("Load", idLoad, hdfsSavePath, hdfsProcessed.date)

    //params: Message to send (JSON to String in this case). This function send all the JSON data as a string.
    newKakfaProducer.sendMessage(message.toString())

  }
}
