package HDFSfIleProcessing

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory

object hbaseManager {

  val confHbase = {

    val conf = HBaseConfiguration.create()
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"))
    conf
  }
  val hConnection = ConnectionFactory.createConnection(confHbase)

}