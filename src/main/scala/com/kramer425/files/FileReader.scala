package com.kramer425.files

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object FileReader {

  def readFile(filePath: String): String = {
    val conf = new Configuration()

    val path = new Path(filePath)
    val fs = path.getFileSystem(conf)
    val dataInputStream = fs.open(path)

    val str = IOUtils.toString(dataInputStream, "UTF-8")

    dataInputStream.close()
    str
  }

  def readLines(filePath: String): Seq[String] = {
    readFile(filePath).split("\n")
  }
}
