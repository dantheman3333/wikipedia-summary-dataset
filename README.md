# wikipedia-summary-dataset

#### A library for parsing Wikipedia dumps into summary and body representations for use in Text Summarization projects.  


A summary is defined as the [leading section](https://en.wikipedia.org/wiki/Wikipedia:Manual_of_Style/Lead_section), and the body is all of the text in each section, concatenated. All references and links are removed, but minimal processing is done. Section headers are removed.

Spark is used to process the dumps distributively into:
```scala
Dataset[WikiPage]
```
where WikiPage is defined as:
```scala
case class WikiPage(id: Long, title: String, summary: String, body: String)
```
