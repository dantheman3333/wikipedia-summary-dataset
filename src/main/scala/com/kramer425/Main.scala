package com.kramer425

import com.kramer425.wiki.{WikiLoader, WikiPage}
import org.apache.spark.sql.{Dataset, SparkSession}

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    val wikiLoader = WikiLoader(spark)

    val pagesDf: Dataset[WikiPage] = wikiLoader.parseWikipediaDump("file:/Users/dkramer/dev/wikipedia/enwiki-20180701-pages-articles1.xml-p10p30302")

    pagesDf.limit(10).collect().foreach({ case page =>
      println("id------------:")
      println(page.id)
      println("title------------:")
      println(page.title)
      println("summary------------:")
      println(page.summary)
      println("body------------:")
      println(page.body)
      println("-------------------------------------")
    })

  }


}


