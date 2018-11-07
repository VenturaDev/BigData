package HDFSfIleProcessing

object DeltaExclusions {

  def main(path: String): Unit = {
    val supportedExtentions = Seq("json, parquet, jdbc, orc, libsvm, csv, text")
    val hdfsReaded = new HDFSReader(path) // path = "hdfs://master1.maticapartners.com:8020/user/albert/test.csv"
    if(supportedExtentions.contains(hdfsReaded.ext)){
      val hdfsProcessed = new HDFSProcessing(hdfsReaded.ext,hdfsReaded.pathFile)
    }
    else print("Extension not supported")



  }


}
