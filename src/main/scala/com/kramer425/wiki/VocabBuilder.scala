package com.kramer425.wiki

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.explode

class VocabBuilder(spark: SparkSession) extends Serializable {

  import spark.implicits._

  def vocabulary(lemmatizedWikiPages: Dataset[LemmatizedWikiPage]): Dataset[String] = {

    val summaries = lemmatizedWikiPages.select("summary").toDF("textSeq")
    val bodies = lemmatizedWikiPages.select("body").toDF("textSeq")

    val both = summaries.union(bodies)

    both.select(explode($"textSeq")).distinct.as[String]
  }

}

object VocabBuilder{
  def apply(spark: SparkSession) = new VocabBuilder(spark)
}