package HDFSfIleProcessing

import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.lit


//Call the right parser depending of the file extension.
class HDFSProcessing(ext: String, path: String) {

  val sc1 = new SparkContext()
  val sqlContext = new SQLContext(sc1)

  val oldFileDataFrame = sqlContext.read.format("csv").option("header", "true").load("hdfs://master1.maticapartners.com:8020/user/albert/test.csv")
  val format = new SimpleDateFormat("YYYY-MM-dd")
  val date = format.format(new java.util.Date())

  val newFileDataFrame = oldFileDataFrame.withColumn("date", lit(date))

  newFileDataFrame.write.format("csv").save("hdfs://master1.maticapartners.com:8020/user/albert/test_clean.csv")

}
