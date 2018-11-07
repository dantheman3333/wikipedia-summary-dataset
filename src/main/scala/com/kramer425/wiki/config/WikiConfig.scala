package com.kramer425.wiki.config

import com.typesafe.config.ConfigException.BadValue
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions, ConfigSyntax}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

case class WikiConfig(inputDir: String,
                      outputDir: String,
                      outputFormat: String,
                      outputDelimiter: String,
                      newlineReplacement: Option[String])


object WikiConfig {

  def apply(path: String): WikiConfig = {
    parseConfig(path)
  }

  val allowedOutputFormats = Set("csv", "txt")

  private def parseConfig(filePath: String): WikiConfig = {
    val config = readFile(filePath)

    val inputDir= config.getString("input.dir").toLowerCase

    val outputDir = config.getString("output.filePath").toLowerCase

    val outputFormat = config.getString("output.format").toLowerCase

    if(!allowedOutputFormats.contains(outputFormat)){
      throw new BadValue("output.format", "Allowed output formats: " + allowedOutputFormats.mkString(", "))
    }

    val outputDelimiter = config.getString("output.delimiter").toLowerCase

    val newLineReplacement = config.getOptional[String]("output.newLineReplacement")

    WikiConfig(inputDir, outputDir, outputFormat, outputDelimiter, newLineReplacement)
  }

  private def readFile(filePath: String): Config = {
    val conf = new Configuration()

    val path = new Path(filePath)
    val fs = path.getFileSystem(conf)
    val dataInputStream = fs.open(path)

    val config = try {
      val str = IOUtils.toString(dataInputStream, "UTF-8")
      ConfigFactory.parseString(str, ConfigParseOptions.defaults().setSyntax(ConfigSyntax.PROPERTIES))
    } finally {
      dataInputStream.close()
    }

    config
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
