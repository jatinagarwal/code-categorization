/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Map


object LsaApp {
  def main(args: Array[String]) {
  	val keywords = List("abstract", "continue", "for", "new", "switch", "assert", "default", "goto", "package", "synchronized",
  		"boolean", "do", "if", "private", "this", "break", "double", "implements", "protected", "throw", "byte", "else",
  		"import", "public", "throws", "case", "enum", "instanceof", "return", "transient", "catch", "extends", "int",
  		"short", "try", "char", "final", "interface", "static", "void", "class", "finally", "long", "strictfp", "volatile",
  		"const", "float", "native", "super", "while", "true", "false", "null", "String", "java", "util", "ArrayList", "println",
      "Arrays", "System", "File", "main")

    val numTerms = 20
    val dataFiles = "smallSampleJava" // Should be some file on your system

    val conf = new SparkConf().setAppName("Lsa Application")
    val sc = new SparkContext(conf)

    val codeData = sc.wholeTextFiles(dataFiles).cache()

    codeData.persist()

    val codeDataWithOutComments = codeData.mapValues{inp => 
    	val regex = """(/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/)|(//.*)""".r
    	val specialChar = """[^a-zA-Z\n\s\#]""".r
    	val commentsRemoved = regex.replaceAllIn(inp,"")
    	specialChar.replaceAllIn(commentsRemoved, " ")
    }	

    val reserveWords = sc.broadcast(keywords)

    val words = codeDataWithOutComments.mapValues{inp =>
      val listOfWords = inp.split("\\s+").toList
      val allReserveWords = reserveWords.value
      val removeReserveWords =  new ListBuffer[String]()
      for(word <- listOfWords if !allReserveWords.contains(word)) removeReserveWords+=word
      val bagOfWords = removeReserveWords.toList.sorted
      bagOfWords
	  }

    words.persist()

    val documentFrequencies = words.flatMapValues{inp =>
      inp.groupBy(x => x).mapValues(_.size)  
    }.values.mapValues{ inp => inp/inp}.reduceByKey((x,y) => x+y).sortByKey()

    val docFreqs = documentFrequencies.collect().sortBy(- _._2).take(numTerms)
    val numDocs = words.count()

    val idfs = inverseDocumentFrequencies(docFreqs, numDocs)

    val vocabulary = documentFrequencies.keys

    val termDocumentFrequencies = words.mapValues{inp =>
      inp.groupBy(x => x).mapValues(_.size)
    } 

    // val count : Int = codeData.collect().count()
    // val numAs = logData.filter(line => line.contains("a")).count()
    // val numBs = logData.filter(line => line.contains("b")).count()
    // println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    // println("count of RDD: "+count)
    // codeData.take(2).foreach(println)
    // codeDataWithOutComments.take(2).foreach(println)
    println("****************************************Number of documents *********************"+numDocs)
    words.take(2).foreach(println)

    println("**************************************************Number of documents*********************"+termDocumentFrequencies.count())
    termDocumentFrequencies.values.foreach(println)

    vocabulary.foreach(println)    

    println("**************************************************Total terms *********************"+ documentFrequencies.count())
    documentFrequencies.foreach(println)

    docFreqs.foreach(println)
    idfs.foreach(println) 
  }

  def inverseDocumentFrequencies(docFreqs: Array[(String, Int)], numDocs: Long)
    : Map[String, Double] = {
    docFreqs.map{ case (term, count) => (term, math.log(numDocs.toDouble / count))}.toMap
  }

  // def removeComments(inp: String): 
}