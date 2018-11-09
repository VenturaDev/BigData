package HDFSfIleProcessing

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

class HDFSReader(sc: SparkContext, sqlc: SQLContext, path: String) {

  val index: Int = path.lastIndexOf(".")
  val ext: String = path.drop(index + 1)
  val pathFile: String = path
  val oldFileDataFrame: DataFrame = sqlc.read.format("com.databricks.spark.csv").option("header", "true").load(path)

}


