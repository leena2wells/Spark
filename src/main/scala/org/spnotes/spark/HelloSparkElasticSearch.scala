package org.spnotes.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

/**
  * Created by sunilpatil on 11/22/16.
  */
object HelloSparkElasticSearch {

  def main(argv:Array[String]): Unit ={
    val sparkConf = new SparkConf().setAppName("FileReader").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf);

    val lines = sparkContext.textFile("/Users/sunilpatil/software/spark-1.6.2-bin-hadoop2.6/README.md")
    val wordCount = lines.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey((x,y)=> x+y)

    wordCount.foreach(println)
    val esMap = Map("es.nodes"->"localhost", "es.port"->"9200", "es.resource"->"wordcount/counts" )
    wordCount.saveToEs(esMap)
  }
}
