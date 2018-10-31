package com.kramer425

import com.kramer425.config.WikiConfig
import com.kramer425.wiki.{WikiLoader, WikiPage}
import org.apache.spark.sql.{Dataset, SparkSession}

object Main {

  def main(args: Array[String]): Unit = {

    if(args.length == 0){
      println("Please supply config file as an argument.")
      System.exit(1)
    }

    val wikiConfig = WikiConfig(args(0))

    val spark = SparkSession.builder().appName("wikiprocessing").getOrCreate()

    val startTime = System.nanoTime()

    val wikiLoader = WikiLoader(spark, wikiConfig.newlineReplacement)

    val pagesDs: Dataset[WikiPage] = {
      val ds = wikiLoader.parseWikipediaDump(wikiConfig.inputDir)

      if (wikiConfig.outputSingleFile) {
        ds.repartition(1)
      } else {
        ds
      }
    }

    pagesDs.cache()

    pagesDs.write
      .format(wikiConfig.outputFormat)
      .option("delimiter", wikiConfig.outputDelimiter)
      .save(wikiConfig.outputDir)

    printf(s"Articles processed: ${pagesDs.count()}")

    pagesDs.unpersist()

    val endTime = System.nanoTime()

    printf(s"Elapsed time: ${(endTime - startTime)/1e9d}")
  }


}


