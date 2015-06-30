/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Map
import scala.collection.mutable.ArrayBuffer



object LsaApp {
  def main(args: Array[String]) {

    /* 'keywords' is list of all reserve words in java */
  	val keywords = List("abstract", "continue", "for", "new", "switch", "assert", "default", "goto", "package", "synchronized",
  		"boolean", "do", "if", "private", "this", "break", "double", "implements", "protected", "throw", "byte", "else",
  		"import", "public", "throws", "case", "enum", "instanceof", "return", "transient", "catch", "extends", "int",
  		"short", "try", "char", "final", "interface", "static", "void", "class", "finally", "long", "strictfp", "volatile",
  		"const", "float", "native", "super", "while", "true", "false", "null", "String", "java", "util", "ArrayList", "println",
      "Arrays", "System", "File", "main")

    /* Number terms that need to be used to construct document vector*/
    val numTerms = 20

    /* Location of the directory where all data files resides */
    val dataFiles = "smallSampleJava" // Path to directory on the system

    /* Spark Coonfiguration object in order to perform configuration settings. 'Lsa Application' is name of application*/
    val conf = new SparkConf().setAppName("Lsa Application")

    /* 'sc' is spark context object to perform all spark related operations with configuration setting from 'conf' */
    val sc = new SparkContext(conf)

    /* 'codeData' is a RDD representing tuples of form (fileName, fileContent) where 'fileContent' is content in a file named
     'fileName'. Here both 'fileName' and 'fileContent are strings. 'codeData' is cached to avoid memoery overhead */
    val codeData = sc.wholeTextFiles(dataFiles).cache()

    /* codeData is persisted for inorder to perform operation */
    codeData.persist()




    /* In this step, data is cleaned by removing all comments and special characters from each '.java' file*/
    val codeDataWithOutComments = codeData.mapValues{inp => 
      /* 'regexForComments' represents a pattern for all types of comments in java program  */
    	val regexForComments = """(/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/)|(//.*)""".r

      /* 'regexSpecialchars' represents a pattern for special characters*/
    	val regexSpecialChars = """[^a-zA-Z\n\s\#]""".r

      /* 'inp' represents content of a'.java' file and all occurences of 'regexForComments' is replaced by "", using 'replaceAllIn' method of scala's regex 
      utility and stor the result in variable named 'commentsRemoved'*/
    	val commentsRemoved = regexForComments.replaceAllIn(inp,"")

      /* 'commentsRemoved' represents content of a'.java' file without any comments and all occurences of 'regexSpecialChars' are 
      replaced by " ", using 'replaceAllIn' method of scala's regex utility */
    	regexSpecialChars.replaceAllIn(commentsRemoved, " ")
    }	



    /* Here 'codeData' RDD is unpersited because it is not needed*/  
    codeData.unpersist()

    codeDataWithOutComments.persist()


    /* Java reserve words are broadcasted to all nodes of a cluster */
    val reserveWords = sc.broadcast(keywords)



    /* In step this cleaned text is splitted into list of words. And reserved words are removed from the list of words*/
    val identifiersForEachDocument = codeDataWithOutComments.mapValues{inp =>
      /* Text 'inp' is splitted into list of words and stored in listOfWords'*/
      val listOfWords = inp.split("\\s+").toList

      /* Broadcasted listed of reserve words are accessed and stored in 'allReserveWords' */
      val allReserveWords = reserveWords.value

      /* 'removeReserveWords' is list buffer to strings */
      val removeReserveWords =  new ListBuffer[String]()

      /* words other than reserve words are appended to 'removeReserveWords'*/
      for(word <- listOfWords if !allReserveWords.contains(word)) removeReserveWords+=word

      /* 'bagOfWords' contains sorted list of identifiers from a '.java'*/
      val bagOfWords = removeReserveWords.toList.sorted
      bagOfWords
	  }  

    /* Unpersisting 'codeDataWithOutComments and persisting 'words'  */
    codeDataWithOutComments.unpersist()
    identifiersForEachDocument.persist()

    /* In this step, document frequiencies for all the identifiers are calculated using flattening all identifier list across all
     documents. Firstly, sorted list of identifiers is grouped based on uniqueness inorder to calculate (identifier, count) pairs. 
     Secondly, each (identifier, count) pair is mapped to  (identifier, 1). Now each (identifier, 1) pair is reduced based on key to
     compute (identifier, df) where 'df' is the document frequency of identifier across all the documents*/
    val documentFrequencies = identifiersForEachDocument.flatMapValues{inp =>
      /* Grouping based on uniqueness and computing size of each group to obtain (identifier,count) pairs */
      inp.groupBy(x => x).mapValues(_.size)  
    }.values.mapValues{ inp => 1}.reduceByKey((x,y) => x+y).sortByKey() /* Calculating document frequencies for all the terms across
    all the documents*/

    // val docFreqs = documentFrequencies.collect().sortBy(- _._2)
    /* Collecting (identifier, df) pairs from a  'documentFrequencies'*/
    val docFreqs = documentFrequencies.collect()

    /*Computing number of documents*/
    val numDocs = identifiersForEachDocument.count()

    /* Computing inverse document frequencies 'idfs' from document frequencies */
    val idfs = inverseDocumentFrequencies(docFreqs, numDocs)

    /* Broadcasting 'idfs' across nodes of cluster*/
    val bidfs = sc.broadcast(idfs.toMap)

    /* Collecting all the identifiers from (identifier, df) pairs*/
    val vocabulary = documentFrequencies.keys.collect()
    /* Broadcasting vocabulary across all nodes of clusters*/
    val termList = sc.broadcast(vocabulary)

    /* Computing term-document frequencies for each document*/
    val termDocumentFrequencies = identifiersForEachDocument.mapValues{inp =>
      /* Grouping based on uniqueness and computing size of each group to obtain (identifier,count) pairs */
      inp.groupBy(x => x).mapValues(_.size)
    } 

    identifiersForEachDocument.unpersist()
    termDocumentFrequencies.persist()

    /* Computing tfidf from term frequencies and Inverse document frequencies */
    val tfidf = termDocumentFrequencies.mapValues{inp =>
      /* Locally obtaining broadcasted bidfs values */
      val idf = bidfs.value

       /* Locally obtaining broadcasted  values */
      val identifiersAcrossAllDocuments = termList.value

      /* Obtaining all terms from this document*/
      val termInThisDocument = inp.keySet.toList

      /* Computing number of terms(identifiers) across all the documents*/
      val sizeOfVector = identifiersAcrossAllDocuments.size

      /* Computing a map of (identifier, tfidf) pairs from term-document (identifier, count) pairs and document-frequency 
      (identifier, idfs) pair */
      var tfidfMap:Map[Int,Double] = Map()
      for(term <- termInThisDocument) {
        tfidfMap += (identifiersAcrossAllDocuments.indexOf(term) -> inp(term)*idf(term))
      }

      /* Converting 'tfidfMap' to a sequence inorder to pass it to a document */
      val tfidfSeq = tfidfMap.toSeq
      /*Obtaining sparse vector from 'tfidfSeq' and size of vector*/
      Vectors.sparse(sizeOfVector, tfidfSeq)
    }

    termDocumentFrequencies.unpersist()
    tfidf.cache()
    tfidf.persist()

    /* Constructing sparse matrix from tfidf sparse vectors obtained in the previous step*/
    val mat = new RowMatrix(tfidf.values)

    tfidf.unpersist()

    /* Computing svd from the 'mat' to obtain matrices*/
    val svd = mat.computeSVD(10, computeU=true)

    println("****************************************Number of documents *********************"+numDocs)
    identifiersForEachDocument.take(2).foreach(println)

    println("****************************************Number of documents*********************"+termDocumentFrequencies.count())
    termDocumentFrequencies.values.foreach(println)

    vocabulary.foreach(println)    

    println("**************************************************Total terms *********************"+ documentFrequencies.count())
    documentFrequencies.foreach(println)

    docFreqs.foreach(println)
    tfidf.values.foreach(println)

    println("**********************************************************SVD computed*********************************************")
    println("Singular values: " + svd.s)

  }

  def inverseDocumentFrequencies(docFreqs: Array[(String, Int)], numDocs: Long)
    : Map[String, Double] = {
    docFreqs.map{ case (term, count) => (term, math.log(numDocs.toDouble / count))}.toMap
  }

  // def removeComments(inp: String): 
}