package com.kramer425.wikismrydataset.wiki.config

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigSyntax}
import com.kramer425.wikismrydataset.files.FileReader

case class WikiConfig(configPath: String,
                      inputDir: String,
                      outputRegularPath: Option[String],
                      outputLemmatizedPath: Option[String],
                      outputVocabularyPath: Option[String],
                      sparkMaster: String,
                      keepBody: Option[Boolean],
                      newlineReplacement: Option[String]) {

  override def toString: String = {
    s"""configPath: $configPath
       |inputDir: $inputDir
       |sparkMaster: $sparkMaster
       |newlineReplacement: $newlineReplacement
       |keepBody: $keepBody
       |outputRegularPath: $outputRegularPath
       |outputLemmatizedPath: $outputLemmatizedPath
       |outputVocabularyPath: $outputVocabularyPath
     """.stripMargin
  }
}


object WikiConfig {

  def apply(path: String): WikiConfig = {
    parseConfig(path)
  }

  private def parseConfig(filePath: String): WikiConfig = {
    val config = readFile(filePath)

    val inputDir = config.getString("input.dir")

    val outputBaseDir = config.getString("output.base.path")

    val sparkMaster = config.getString("spark.master")

    val keepBody = config.getOptionalBool("keepBody")

    val outputRegularPath = config.getOptionalString("output.regularFileName").map(join(outputBaseDir, _))
    val outputLemmatizedPath = config.getOptionalString("output.lemmatizedFileName").map(join(outputBaseDir, _))
    val outputVocabularyPath = config.getOptionalString("output.vocabularyFileName").map(join(outputBaseDir, _))

    val newLineReplacement = config.getOptionalString("output.newLineReplacement")

    WikiConfig(filePath, inputDir, outputRegularPath, outputLemmatizedPath, outputVocabularyPath, sparkMaster, keepBody, newLineReplacement)
  }

  private def join(base: String, post: String): String = {
    val sb = new StringBuilder(base)
    if (!base.endsWith("/")) {
      sb.append("/")
    }
    sb.append(post)
    sb.toString()
  }

  private def readFile(filePath: String): Config = {
    val str = FileReader.readFile(filePath)
    ConfigFactory.parseString(str, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.PROPERTIES))
  }

  implicit class ConfigOpts(config: Config) {
    def getOptionalString(path: String): Option[String] = {
      if (config.hasPath(path)) {
        Some(config.getString(path))
      } else {
        None
      }
    }
    def getOptionalBool(path: String): Option[Boolean] = {
      if (config.hasPath(path)) {
        Some(config.getBoolean(path))
      } else {
        None
      }
    }
  }


}
