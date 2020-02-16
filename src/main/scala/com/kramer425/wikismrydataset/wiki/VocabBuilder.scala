package com.kramer425.wikismrydataset.wiki

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.explode

case class WordCount(word: String, count: Int)

class VocabBuilder(spark: SparkSession) extends Serializable {

  import spark.implicits._

  def vocabulary(lemmatizedWikiPages: Dataset[ProcessedWikiPage]): Dataset[WordCount] = {

    val summaries = lemmatizedWikiPages.select("summary").toDF("textSeq")
    val bodies = lemmatizedWikiPages.select("body").toDF("textSeq")

    val both = summaries.union(bodies)

    val text = both.select(explode($"textSeq")).as[String]
    val counts = text.map(word => (word, 0)).rdd.reduceByKey(_ + _).toDS()
    counts.map(t => WordCount(t._1, t._2))
  }
}

object VocabBuilder{
  def apply(spark: SparkSession) = new VocabBuilder(spark)
}