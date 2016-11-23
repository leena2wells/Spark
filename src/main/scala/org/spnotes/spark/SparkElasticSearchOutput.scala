package org.spnotes.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

import scala.util.parsing.json.JSON

/**
  * Created by sunilpatil on 11/22/16.
  */
object SparkElasticSearchOutput {

  def main(argv: Array[String]): Unit = {
    if (argv.length != 4) {
      println("Usage Pattern: SparkElasticSearchOutput <inputfilepath> " +
        "<eshostname> <espost> <esresource>")
      return
    }
    val filePath = argv(0)
    val esHost = argv(1)
    val esPort = argv(2)
    val esResource = argv(3)


    val sparkConf = new SparkConf().setAppName("SparkElasticSearchOutput").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf);

    val lines = sparkContext.textFile(filePath)
    val wordCount = lines.flatMap(line => line.split(" ")).map(word => (word, 1)).
      reduceByKey((x, y) => x + y)

    //Take out empty words/words with length 0, take out words containing .
    val validWords = wordCount.filter(record => record._1.trim.length != 0).filter(record => !record._1.contains("."))
    val wordCountJson = validWords.map { record =>
      val jsonStr = "{\"" + record._1 + "\":\"" + record._2 + "\"}"
      JSON.parseFull(jsonStr)
    }.filter(record => record != None)

    wordCountJson.foreach(println)
    //    val esMap = Map("es.nodes" -> "localhost", "es.port" -> "9200", "es.resource" -> "wordcount/counts")
    //Since we already have JSON object save it as it is
    val esMap = Map("es.nodes" -> esHost, "es.port" -> esPort, "es.resource" -> esResource, "es.output.json" -> "true")
    wordCountJson.saveToEs(esMap)

  }
}
