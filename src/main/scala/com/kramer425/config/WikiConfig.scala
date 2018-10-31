package com.kramer425.config

import java.io.File

import com.typesafe.config.ConfigException.BadValue
import com.typesafe.config.{Config, ConfigFactory}

case class WikiConfig(inputDir: String,
                      outputDir: String,
                      outputSingleFile: Boolean,
                      outputFormat: String,
                      outputDelimiter: String,
                      newlineReplacement: Option[String])


object WikiConfig {

  def apply(path: String): WikiConfig = {
    parseConfig(path)
  }

  val allowedOutputFormats = Set("csv", "txt")

  private def parseConfig(path: String): WikiConfig = {
    val configFile = new File(path)
    val config = ConfigFactory.parseFile(configFile)

    val inputDir= config.getString("input.dir").toLowerCase

    val outputDir = config.getString("output.dir").toLowerCase

    val outputSingleFile = config.getBoolean("output.singlefile")

    val outputFormat = config.getString("output.format").toLowerCase

    if(!allowedOutputFormats.contains(outputFormat)){
      throw new BadValue("output.format", "Allowed output formats: " + allowedOutputFormats.mkString(", "))
    }

    val outputDelimiter = config.getString("output.delimiter").toLowerCase

    val newLineReplacement = config.getOptional[String]("newLineReplacement")

    WikiConfig(inputDir, outputDir, outputSingleFile, outputFormat, outputDelimiter, newLineReplacement)
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
