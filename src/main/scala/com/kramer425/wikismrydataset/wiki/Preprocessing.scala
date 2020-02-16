package com.kramer425.wikismrydataset.wiki

import org.apache.spark.sql.{Dataset, SparkSession}
import java.text.Normalizer
import java.text.Normalizer.Form
import java.util.Properties

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class ProcessedWikiPage(id: Long, title: String, summary: Seq[String], body: Seq[String])

class Preprocessing(spark: SparkSession) extends Serializable {

  import spark.implicits._

  val NUMBER_TOKEN = "<NUMBER>"

  // Taken from: https://rosettacode.org/wiki/Determine_if_a_string_is_numeric#Scala
  val numberRegex =
    """[+-]?((\d+(e\d+)?[lL]?)|(((\d+(\.\d*)?)|(\.\d+))(e\d+)?[fF]?))""".r

  private val bStopWords: Broadcast[Set[String]] = loadStopWords()

  private def loadStopWords(): Broadcast[Set[String]] = {
    val is = getClass.getResourceAsStream("/org/apache/spark/ml/feature/stopwords/english.txt")
    val stopWords = scala.io.Source.fromInputStream(is)(scala.io.Codec.UTF8).getLines().toSet
    spark.sparkContext.broadcast(stopWords)
  }

  // Lowercases, converts unicode to ascii. Some characters like Mandarin characters are left in
  // Modified from: https://gist.github.com/agemooij/15a0eaebc2c1ddd5ddf4
  private def normalizeString(str: String): String = {
    val cleaned = str.trim.toLowerCase
    val normalized = Normalizer.normalize(cleaned, Form.NFD).replaceAll("[\\p{InCombiningDiacriticalMarks}\\p{IsM}\\p{IsLm}\\p{IsSk}]+", "")

    normalized.replaceAll("'s", "")
      .replaceAll("ß", "ss")
      .replaceAll("ø", "o")
      .replaceAll("₣", "f")
      .replaceAll("₳", "a")
      .replaceAll("㎝", "cm")
      .replaceAll("‒", "-")
  }

  def processWikiPages(wikiPages: Dataset[WikiPage]): Dataset[ProcessedWikiPage] = {
    wikiPages.mapPartitions( iter => {
      val stanfordNLP = nlpInstance()
      iter.map(wikiPage => processWikiPage(wikiPage, stanfordNLP, bStopWords.value))
    })
  }

  private def processWikiPage(wikiPage: WikiPage, stanfordNLP: StanfordCoreNLP, stopWords: Set[String]): ProcessedWikiPage = {
    val nSummary = normalizeString(wikiPage.summary)
    val nBody = normalizeString(wikiPage.body)

    val summaryLemmas: Seq[String] = plainTextToLemmas(nSummary, stanfordNLP, stopWords)
    val bodyLemmas: Seq[String] = plainTextToLemmas(nBody, stanfordNLP, stopWords)

    ProcessedWikiPage(wikiPage.id, wikiPage.title, summaryLemmas, bodyLemmas)
  }

  private def nlpInstance(): StanfordCoreNLP = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(props)
  }

  private def isOnlyLetters(str: String): Boolean = {
    str.forall(c => Character.isLetter(c))
  }

  private def isNumber(str: String): Boolean = {
    numberRegex.pattern.matcher(str).matches()
  }

  // Convert text to Seq of lemmas. Add sentence start tokens and sentence end tokens. Replace numbers with token
  private def plainTextToLemmas(text: String, stanfordNLP: StanfordCoreNLP, stopWords: Set[String]): Seq[String] = {
    val doc = new Annotation(text)
    stanfordNLP.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences.asScala; token <- sentence.get(classOf[TokensAnnotation]).asScala) {
      val lemma = token.get(classOf[LemmaAnnotation]).toLowerCase
      if (lemma.length > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
        lemmas += lemma
      } else if (isNumber(lemma)) {
        lemmas += NUMBER_TOKEN
      }
    }
    lemmas
  }
}

object Preprocessing {
  def apply(spark: SparkSession): Preprocessing = new Preprocessing(spark)
}