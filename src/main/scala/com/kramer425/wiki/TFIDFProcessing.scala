package com.kramer425.wiki

import org.apache.spark.ml.feature.{CountVectorizer, HashingTF, IDF, RegexTokenizer}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.linalg.Vector

case class TFIDF(docId: String, tfidf: Array[Double])

class TFIDFProcessing(private val spark: SparkSession) extends Serializable {
  import spark.implicits._

  /*
    regex for allowed tokens.
    one or more unicode characters optionally followed by apostrophe and one or more character or just apostrophe
    (?: ) symbolizes non-capture group
    ? means optional
   */
  private val tokenRegex = s"\\p{L}+(?:'\\p{L}+|')?"

  def tfidf(wikiPages: Dataset[WikiPage]): (Array[String], Dataset[TFIDF]) = {

    val documentDf = wikiPageToTokens(wikiPages)

    val cvModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("counts")
      .fit(documentDf)

    val countDf = cvModel.transform(documentDf)

    countDf.show(5)

    val idf = new IDF().setInputCol("counts").setOutputCol("tfidfVector")
    val idfModel = idf.fit(countDf)

    val tfidfDf = idfModel.transform(countDf)

    val tfidf = tfidfDf.withColumn("tfidf", vecToSeq($"tfidfVector")).select("docId", "tfidf").as[TFIDF]

    (cvModel.vocabulary, tfidf)
  }

  private val vecToSeq = udf((v: Vector) => v.toArray)

  //DataFrame: id, text, words
  def wikiPageToTokens(wikiPages: Dataset[WikiPage]): DataFrame = {
    val documentDf = wikiPages.map(wikiPage => (wikiPage.id, wikiPage.summary + " " + wikiPage.body))
                              .toDF("docId", "text")


    val tokenizer = new RegexTokenizer().setInputCol("text").setOutputCol("words").setPattern(tokenRegex).setGaps(false)

    tokenizer.transform(documentDf)
  }
}

object TFIDFProcessing {
  def apply(spark: SparkSession): TFIDFProcessing = {
    new TFIDFProcessing(spark)
  }
}
