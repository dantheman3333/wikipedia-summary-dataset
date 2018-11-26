package com.kramer425.wiki

import com.kramer425.wiki.config.WikiConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws}

object Main {

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("Please supply config file path in hdfs as an argument.")
      System.exit(1)
    }

    val wikiConfig = WikiConfig(args(0))
    println(wikiConfig)

    val spark = SparkSession.builder().appName("wiki-smry-dataset").getOrCreate()
    import spark.implicits._

    val startTime = System.nanoTime()

    val wikiLoader = WikiLoader(spark, wikiConfig.newlineReplacement)

    val pagesDs = wikiLoader.parseWikipediaDump(wikiConfig.inputDir)

    pagesDs.cache()

    if(wikiConfig.outputRegularPath.isDefined){
      println(s"Saving regular to ${wikiConfig.outputRegularPath.get}")
      pagesDs.write
        .format(wikiConfig.outputFormat)
        .options(wikiConfig.sparkWriteOptions)
        .save(wikiConfig.outputRegularPath.get)
    }

    pagesDs.show(10, false)

    println(s"Articles processed: ${pagesDs.count()}")

    pagesDs.map(wikiPage => Array(wikiPage.summary.length, wikiPage.summary.split("\\.").length,
      wikiPage.body.length, wikiPage.body.split("\\.").length))
      .select(
        col("value").getItem(0).as("summaryLength"),
        col("value").getItem(1).as("summarySentenceCount"),
        col("value").getItem(2).as("bodyLength"),
        col("value").getItem(3).as("bodySentenceCount")
      )
      .describe()
      .show()

    val preprocesser = Preprocessing(spark)
    val processedPagesDs = preprocesser.processWikiPages(pagesDs)

    processedPagesDs.cache()


    if(wikiConfig.outputLemmatizedPath.isDefined){
      println(s"Saving lemmatized to ${wikiConfig.outputLemmatizedPath.get}")
      processedPagesDs.select(
        col("id"),
        col("title"),
        concat_ws(" ", col("summary")).as("summary"),
        concat_ws(" ", col("body")).as("body"))
        .write
        .format(wikiConfig.outputFormat)
        .options(wikiConfig.sparkWriteOptions)
        .save(wikiConfig.outputLemmatizedPath.get)
    }

    processedPagesDs.show(10,false)

    processedPagesDs.map(processedWikiPage => Array(processedWikiPage.summary.length, processedWikiPage.body.length))
      .select(
        col("value").getItem(0).as("processedSummaryLength"),
        col("value").getItem(1).as("processedBodyLength")
      )
      .describe()
      .show()

    if(wikiConfig.outputVocabularyPath.isDefined){
      println(s"Saving vocab to ${wikiConfig.outputVocabularyPath.get}")
      val vocabBuilder = VocabBuilder(spark)
      val vocabDs = vocabBuilder.vocabulary(processedPagesDs)
      vocabDs.repartition(1).write.text(wikiConfig.outputVocabularyPath.get)
    }

    val endTime = System.nanoTime()

    println(s"Elapsed time: ${(endTime - startTime) / 1e9d}")
  }


}


