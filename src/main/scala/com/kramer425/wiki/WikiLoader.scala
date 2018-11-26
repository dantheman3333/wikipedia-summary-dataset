package com.kramer425.wiki

import io.mindfulmachines.input.XMLInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.wikiclean.WikiClean
import org.wikiclean.WikiClean.WikiLanguage


case class WikiPage(id: Long, title: String, summary: String, body: String)

class WikiLoader(spark: SparkSession, newlineReplacement: Option[String]) extends Serializable {
  import spark.implicits._

  private val headerString = "\n=="
  private val punctuationRegex = """[.|:|*]""".r
  private val letterRegex = """[a-zA-Z]+""".r

  def parseWikipediaDump(path: String): Dataset[WikiPage] = {
    val conf = new Configuration()
    conf.set(XMLInputFormat.START_TAG_KEY, "<page>")
    conf.set(XMLInputFormat.END_TAG_KEY, "</page>")

    val dump: RDD[(LongWritable, Text)] = spark.sparkContext.newAPIHadoopFile(path, classOf[XMLInputFormat],
      classOf[LongWritable], classOf[Text], conf)

    val rawXmls: Dataset[String] = dump.map(_._2.toString).toDS()

    rawXmls.filter($"value".isNotNull)
      .mapPartitions{ iter =>
        val wikiCleaner = wikiCleanerInstance()
        iter.flatMap( pageXml => xmlToWikiPage(wikiCleaner, pageXml))
      }
  }

  private def wikiCleanerInstance(): WikiClean = {
    new WikiClean.Builder()
      .withLanguage(WikiLanguage.EN)
      .withTitle(false)
      .withFooter(false).build()
  }


  private def xmlToWikiPage(wikiCleaner: WikiClean, pageXMl: String): Option[WikiPage] = {
    val firstHeadingIndex = pageXMl.indexOf(headerString)

    firstHeadingIndex match {
      case -1 => None
      case _ =>

        //split page into summary and body xml
        val summaryXml = pageXMl.substring(0, firstHeadingIndex) + "</text>"
        val bodyXml = "<text xml:space=\"preserve\"" + pageXMl.substring(firstHeadingIndex, pageXMl.length)

        val id = wikiCleaner.getId(summaryXml).toLong
        val title = wikiCleaner.getTitle(summaryXml)

        val (summary, body) = {
          try {
            val summaryText = wikiCleaner.clean(summaryXml)
            val rawBodyText = wikiCleaner.clean(bodyXml)
            //remove lines which are just section headers
            val cleanedBodyText = rawBodyText.split("\n").filterNot(isHeaderOrErrorLine).mkString("\n")

            //optionally replace newlines
            newlineReplacement match {
              case None => (summaryText, cleanedBodyText)
              case Some(rep) => (summaryText.replaceAll("\n", rep), cleanedBodyText.replaceAll("\n", rep))
            }
          }catch{
            //Sometimes WikiCleaner fails on certain characters
            case _: IllegalArgumentException => ("", "")
          }
        }

        //filter body lengths that are greater than 1 std dev above mean (3476) std dev (6516)
        if(title.isEmpty || isMetaArticle(title.toLowerCase) || summary.isEmpty || body.isEmpty || body.length > 10000){
          None
        }else{
          Some(WikiPage(id, title, summary, body))
        }
    }
  }

  private def isMetaArticle(lowerTitle: String): Boolean = {
    lowerTitle.contains("disambiguation") || lowerTitle.startsWith("wikipedia:") || lowerTitle.startsWith("template:") ||
      lowerTitle.startsWith("file:") || lowerTitle.startsWith("draft:") || lowerTitle.startsWith("help:")
  }

  private def isHeaderOrErrorLine(line: String): Boolean = {
    //header lines don't have any punctuation, also remove lines without letters
    val puncMatches = punctuationRegex.findAllIn(line)
    val letterMatches = letterRegex.findAllIn(line)
    line.isEmpty || line.startsWith("File:") || puncMatches.isEmpty || letterMatches.isEmpty ||
      (puncMatches.nonEmpty && letterMatches.isEmpty)
  }

}

object WikiLoader{
  def apply(spark: SparkSession, newlineReplacement: Option[String] = None): WikiLoader = {
    new WikiLoader(spark, newlineReplacement)
  }
}
