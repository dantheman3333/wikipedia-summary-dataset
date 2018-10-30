package com.kramer425.wiki

import edu.umd.cloud9.collection.XMLInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.wikiclean.WikiClean
import org.wikiclean.WikiClean.WikiLanguage


case class WikiPage(id: String, title: String, summary: String, body: String)

class WikiLoader(private val spark: SparkSession) extends Serializable {
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

  /*
  private def xmlToWikiPage(wikiCleaner: WikiClean, pageXMl: String): Option[WikiPage] = {
      val headingMatches = headerRegex.findAllIn(pageXMl)

      headingMatches.hasNext match {
        case false => None
        case true => {
          val firstHeadingIndex = headingMatches.start

          //split page into summary and body xml
          val summaryXml = pageXMl.substring(0, firstHeadingIndex) + "</text>"
          val bodyXml = "<text xml:space=\"preserve\"" + pageXMl.substring(firstHeadingIndex, pageXMl.length)

          val id = wikiCleaner.getId(pageXMl);
          val title = wikiCleaner.getTitle(pageXMl);
          val summary = wikiCleaner.clean(summaryXml);

          val rawBody = wikiCleaner.clean(bodyXml);
          //remove lines which are just section headers
          val body = rawBody.split("\n").filterNot(isHeaderOrErrorLine).mkString("\n")

          if(title.isEmpty || title.toLowerCase.contains("disambiguation") || summary.isEmpty || body.isEmpty){
            None
          }else{
            Some(WikiPage(id, title, summary, body))
          }
        }
      }
  }*/
  private def xmlToWikiPage(wikiCleaner: WikiClean, pageXMl: String): Option[WikiPage] = {
    val firstHeadingIndex = pageXMl.indexOf(headerString)

    firstHeadingIndex match {
      case -1 => None
      case _ => {

        //split page into summary and body xml
        val summaryXml = pageXMl.substring(0, firstHeadingIndex) + "</text>"
        val bodyXml = "<text xml:space=\"preserve\"" + pageXMl.substring(firstHeadingIndex, pageXMl.length)

        val id = wikiCleaner.getId(pageXMl);
        val title = wikiCleaner.getTitle(pageXMl);
        val summary = wikiCleaner.clean(summaryXml);

        val rawBody = wikiCleaner.clean(bodyXml);
        //remove lines which are just section headers
        val body = rawBody.split("\n").filterNot(isHeaderOrErrorLine).mkString("\n")

        if(title.isEmpty || title.toLowerCase.contains("disambiguation") || summary.isEmpty || body.isEmpty){
          None
        }else{
          Some(WikiPage(id, title, summary, body))
        }
      }
    }
  }

  private def isHeaderOrErrorLine(line: String): Boolean = {
    //header lines don't have any punctuation, also remove lines without letters
    val puncMatches = punctuationRegex.findAllIn(line)
    val letterMatches = letterRegex.findAllIn(line)
    line.isEmpty || line.startsWith("File:") || puncMatches.isEmpty || letterMatches.isEmpty ||
      (!puncMatches.isEmpty && letterMatches.isEmpty)
  }

}

object WikiLoader{
  def apply(spark: SparkSession): WikiLoader = {
    new WikiLoader(spark)
  }
}
