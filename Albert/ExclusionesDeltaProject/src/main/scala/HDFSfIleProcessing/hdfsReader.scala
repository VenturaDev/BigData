package HDFSfIleProcessing

class HDFSReader(path: String) {
  private val index = path.lastIndexOf(".")
  val ext = path.drop(index + 1)
  val pathFile = path
}


