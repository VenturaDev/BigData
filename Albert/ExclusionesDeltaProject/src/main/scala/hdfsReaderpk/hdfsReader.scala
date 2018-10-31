import org.apache.spark.SparkContext

package hdfsReaderpk {

  class HDFSReader(path: String) {
    val sc1 = new SparkContext()
    val rdd1 = sc1.textFile(path)

    //Start parsing the file, we first get all the rows as an Array of Arrays.
    val rows = rdd1.map(line => line.split(",").map(_.trim)) //rows = Array[Array[String]]

    // Extracting the header
    val oldHeader = rows.first() //oldHeader = Array[String]

    // We add a Date column to the header format: Year/Month/Day
    val newHeader = oldHeader :+ "Date" // header = Array[String]

    //Extracting Data
    val oldData = rows.filter(_ (0) != oldHeader(0)) //oldData = Array[Array[String]]

    //For each Data row, we include a new element corresponding to Date column
    val newData = oldData.map(row => row :+ "-") //newData = Array[Array[String]]
  }
  class 
}