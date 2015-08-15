package sample

import org.apache.spark.{SparkContext, SparkConf}

object PairRDDSample {
  private val YEAR_BEGIN = 15
  private val YEAR_END = 19
  private val TEMP_BEGIN = 87
  private val TEMP_END = 92

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("spark-standalone-sample")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(0)).cache()
    val filteredLines = lines.filter { line => !line.substring(TEMP_BEGIN, TEMP_END).contains("+9999")}
    val pairs = filteredLines.map(line => (line.substring(YEAR_BEGIN, YEAR_END), line.substring(TEMP_BEGIN, TEMP_END)))
    val reducedPairs = pairs.reduceByKey((x, y) => if (x.toInt >= y.toInt) x else y)
    reducedPairs.saveAsTextFile(args(1))
  }
}
