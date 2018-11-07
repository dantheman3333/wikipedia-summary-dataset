package com.kramer425.wiki

import com.kramer425.wiki.config.WikiConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Main {

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("Please supply config file path in hdfs as an argument.")
      System.exit(1)
    }

    val wikiConfig = WikiConfig(args(0))

    val spark = SparkSession.builder().appName("wiki-smry-dataset").getOrCreate()
    import spark.implicits._

    val startTime = System.nanoTime()

    val wikiLoader = WikiLoader(spark, wikiConfig.newlineReplacement)

    val pagesDs = wikiLoader.parseWikipediaDump(wikiConfig.inputDir)

    pagesDs.cache()

    //    val tfidfProcessor = TFIDFProcessing(spark)
    //
    //    val (columns, tfidf) = tfidfProcessor.tfidf(pagesDs)
    //
    //    tfidf.show(5)
    //    tfidf.printSchema()

    pagesDs.write
      .format(wikiConfig.outputFormat)
      .option("delimiter", wikiConfig.outputDelimiter)
      .save(wikiConfig.outputDir)

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

    pagesDs.unpersist()

    val endTime = System.nanoTime()

    println(s"Elapsed time: ${(endTime - startTime) / 1e9d}")
  }


}


