package org.spnotes.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * Created by sunilpatil on 11/22/16.
  */
object HelloSparkElasticSearch {

  def main(argv: Array[String]): Unit = {
    if(argv.length != 4){
      println("Usage Pattern: HelloSparkElasticSearch <inputfilepath> <eshostname> <espost> <esresource>")
      return
    }
    val filePath = argv(0)
    val esHost = argv(1)
    val esPort = argv(2)
    val esResource = argv(3)


    val sparkConf = new SparkConf().setAppName("FileReader").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf);

    val lines = sparkContext.textFile(filePath)
    val wordCount = lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((x, y) => x + y)

    wordCount.foreach(println)
//    val esMap = Map("es.nodes" -> "localhost", "es.port" -> "9200", "es.resource" -> "wordcount/counts")
val esMap = Map("es.nodes" -> esHost, "es.port" -> esPort, "es.resource" -> esResource)
    wordCount.saveToEs(esMap)
  }
}
