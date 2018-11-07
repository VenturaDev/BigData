package HDFSfIleProcessing

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class hBaseExcluPutter {
  val sc1 = new SparkContext()
  val sqlContext = new SQLContext(sc1)


  val hbaseContext = new HBaseContext(sc1, hbaseManager.confHbase)

  val multihbaseTab = TableName.valueOf("Albert_test")

  val putRawRDD = sqlContext.read.format("csv").option("header", "false").load("hdfs://master1.maticapartners.com:8020/user/albert/test_clean.csv").rdd
  // Array[org.apache.spark.sql.Row] = Array([7896545,0,1,0,1,0,1,1,0,2018-11-06], [0865456,1,0,1,1,0,1,0,1,2018-11-06])

  val putRDD = putRawRDD.map {
    _.toSeq.map {
      _.toString
    }.toArray
  } // Array[Array[String]] = Array(Array(7896545, 0, 1, 0, 1, 0, 1, 1, 0, 2018-11-06), Array(0865456, 1, 0, 1, 1, 0, 1, 0, 1, 2018-11-06))

  /**
    * Insert multiple lines into a row
    * table = target table
    * rk = lines row key
    * cf = column family's name
    * content = map of column family's qualifiers and corresponding values
    */
  def insertLines(table: Table, rk: String, cf: String, content: Array[Array[String]]): Unit = {
    val put = new Put(Bytes.toBytes(rk))
    content.foreach(x => {
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
    })
    table.put(put)

}
