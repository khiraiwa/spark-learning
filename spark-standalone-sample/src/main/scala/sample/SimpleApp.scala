package sample

import org.apache.spark.{SparkContext, SparkConf}

object SimpleApp {
  val file = "/usr/local/Cellar/apache-spark/1.4.0/README.md"

  def main(args: Array[String]) {

    // SparkContextの初期化
    // localを指定して、ローカルで1 theadで実行
    // setMasterは省略可
    val conf = new SparkConf().setMaster("local").setAppName("spark-standalone-sample")
    val sc = new SparkContext(conf)

    // ファイルを読み込みます
    // 第二引数は最小パーティション(入力データ分割)数とのこと.省略可
    // .cache()を呼ぶとクラスター間でin-memoryキャシュされます
    val fileData = sc.textFile(file, 2).cache()

    // カウントします
    val numAs = fileData.filter(line => line.contains("spark")).count()
    val numBs = fileData.filter(line => line.contains("scala")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}