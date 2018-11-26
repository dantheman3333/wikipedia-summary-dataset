package com.kramer425.wiki.config

import com.typesafe.config.ConfigException.BadValue
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigSyntax}
import com.kramer425.files.FileReader

case class WikiConfig(configPath: String,
                      inputDir: String,
                      outputRegularPath: Option[String],
                      outputLemmatizedPath: Option[String],
                      outputVocabularyPath: Option[String],
                      outputFormat: String,
                      outputDelimiter: Option[String],
                      newlineReplacement: Option[String]){
  val sparkWriteOptions = {
    var map = scala.collection.mutable.Map[String, String]()

    if(outputFormat == "parquet"){
      map += "compression" -> "none"
    }else{
      map += "delimiter" -> outputDelimiter.get
    }
    map
  }

  override def toString: String = {
    s"""configPath: $configPath
       |inputDir: $inputDir
       |outputFormat: $outputFormat
       |outputDelimiter: $outputDelimiter
       |newlineReplacement: $newlineReplacement
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

  val allowedOutputFormats = Set("csv", "txt", "parquet")

  private def parseConfig(filePath: String): WikiConfig = {
    val config = readFile(filePath)

    val inputDir= config.getString("input.dir").toLowerCase

    val outputBaseDir = config.getString("output.base.path").toLowerCase

    val outputRegularPath = config.getOptional[String]("output.regularFileName").map(join(outputBaseDir, _))
    val outputLemmatizedPath = config.getOptional[String]("output.lemmatizedFileName").map(join(outputBaseDir, _))
    val outputVocabularyPath = config.getOptional[String]("output.vocabularyFileName").map(join(outputBaseDir, _))

    val outputFormat = config.getString("output.format").toLowerCase

    if(!allowedOutputFormats.contains(outputFormat)){
      throw new BadValue("output.format", "Allowed output formats: " + allowedOutputFormats.mkString(", "))
    }

    val outputDelimiter = config.getOptional[String]("output.delimiter")

    val newLineReplacement = config.getOptional[String]("output.newLineReplacement")

    WikiConfig(filePath, inputDir, outputRegularPath, outputLemmatizedPath, outputVocabularyPath, outputFormat, outputDelimiter, newLineReplacement)
  }

  private def join(base: String, post: String): String = {
    val sb = new StringBuilder(base)
    if(!base.endsWith("/")){
      sb.append("/")
    }
    sb.append(post)
    sb.toString()
  }

  private def readFile(filePath: String): Config = {
    val str = FileReader.readFile(filePath)
    ConfigFactory.parseString(str, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.PROPERTIES))
  }

  implicit class ConfigOpts(config: Config){
    def getOptional[T](path: String): Option[T] = {
      if(config.hasPath(path)){
        Some(config.getAnyRef(path).asInstanceOf[T])
      }else{
        None
      }
    }
  }


}
