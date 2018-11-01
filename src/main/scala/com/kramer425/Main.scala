package com.kramer425

import com.kramer425.config.WikiConfig
import com.kramer425.wiki.WikiLoader
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    if(args.length == 0){
      println("Please supply config file path in hdfs as an argument.")
      System.exit(1)
    }

    val wikiConfig = WikiConfig(args(0))

    val spark = SparkSession.builder().appName("wiki-smry-dataset").getOrCreate()

    val startTime = System.nanoTime()

    val wikiLoader = WikiLoader(spark, wikiConfig.newlineReplacement)

    val pagesDs = wikiLoader.parseWikipediaDump(wikiConfig.inputDir)

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


