package HDFSfIleProcessing


import java.text.SimpleDateFormat

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.lit

class HDFSProcessing(sc: SparkContext, sqlc: SQLContext, oldDF: DataFrame, savePath: String ) {

  //Create a date pattern and a new date for the processed file
  val format = new SimpleDateFormat("YYYY-MM-dd")
  val date = format.format(new java.util.Date())

  //We add the date column and the value of the actual day.
  private val newFileDataFrame = oldDF.withColumn("date", lit(date))

  //We write the file in hdfs in the desired save path.(this is optional, we could use a val to store the dataframe).
  // OJO: SIN CABECERA
  newFileDataFrame.write.format("com.databricks.spark.csv").option("header","true").save(savePath)
}
