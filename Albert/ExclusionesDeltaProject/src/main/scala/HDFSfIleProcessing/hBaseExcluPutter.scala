package HDFSfIleProcessing
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import org.apache.spark.rdd.RDD // unused for the moment because the RDD is readed from hdfs,
//Will be necessary in case the RDD is provided as a parameter in the class constructor.

class hBaseExcluPutter(sc: SparkContext, sqlc: SQLContext, hdfsLoadPath: String) extends Serializable {

  val putRawRDD = sqlc.read.format("csv").option("header", "true").load(hdfsLoadPath).rdd
  // Array[org.apache.spark.sql.Row] = Array([7896545,0,1,0,1,0,1,1,0,2018-11-06], [0865456,1,0,1,1,0,1,0,1,2018-11-06])

  val putRDD = putRawRDD.map {
    _.toSeq.map {
      _.toString
    }.toArray
  } // Array[Array[String]] = Array(Array(7896545, 0, 1, 0, 1, 0, 1, 1, 0, 2018-11-06), Array(0865456, 1, 0, 1, 1, 0, 1, 0, 1, 2018-11-06))


  def insertLines(table_Name: String, cf: String): Unit = {

    //To maintain the process distributed, everything will be executed for each partition of the RDD.
    putRDD.foreachPartition(xPartition => {

      //We create a val with the table name provided as parameter and a connection.
      val tableName: TableName = TableName.valueOf(table_Name)
      val hTable =  hbaseManager.hConnection.getTable(tableName)
      xPartition.foreach(x => {
      //Knowing how our RDD is decomposed, we add to the given column the corresponded value. Also iterating the multiple rows(Arrays).
        val put = new Put(Bytes.toBytes(x(0)))
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("Numper"), Bytes.toBytes(x(0)))
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("excl1"), Bytes.toBytes(x(1)))
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("excl2"), Bytes.toBytes(x(2)))
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("excl3"), Bytes.toBytes(x(3)))
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("excl4"), Bytes.toBytes(x(4)))
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("excl5"), Bytes.toBytes(x(5)))
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("excl6"), Bytes.toBytes(x(6)))
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("excl7"), Bytes.toBytes(x(7)))
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("excl8"), Bytes.toBytes(x(8)))
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("date"), Bytes.toBytes(x(9)))
        hTable.put(put)
      })
    })
  }
}

