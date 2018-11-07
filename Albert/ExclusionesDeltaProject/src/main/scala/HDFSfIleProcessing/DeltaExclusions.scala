package HDFSfIleProcessing

object DeltaExclusions {

  def main(path: String): Unit = {
    val hdfsReaded = new HDFSReader(path) // path = "hdfs://master1.maticapartners.com:8020/user/albert/test.csv"
    val hdfsProcessed = new HDFSProcessing(hdfsReaded.ext,hdfsReaded.pathFile)
    val hbaseputted = new hBaseExcluPutter()
    hbaseputted.insertLines(???)
    val kafkaproduced = new kafkaProducerExcl()
    kafkaproduced.send(kafkaproduced.sRDD)






  }


}
