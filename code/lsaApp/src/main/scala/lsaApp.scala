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

    
    val numTerms = 20/* Number terms that need to be used to construct document vector*/    
    val dataFiles = "sampleJava2" /* Location of the directory where all data files resides */
    val conf = new SparkConf().setAppName("Lsa Application")/* Spark Coonfiguration object in order 
    to perform configuration settings. 'Lsa Application' is name of application*/
    val sc = new SparkContext(conf)/* 'sc' is spark context object to perform all spark related 
    operations with configuration setting from 'conf' */
    val codeData = sc.wholeTextFiles(dataFiles).cache()/* 'codeData' is a RDD representing tuples of form 
    (fileName, fileContent) where 'fileContent' is content in a file named 'fileName'. Here both 'fileName' 
     and 'fileContent are strings. 'codeData' is cached to avoid memoery overhead */
    codeData.persist()/* codeData is per sisted for inorder to perform operation */
    codeData.count()

    /* In this step, data is cleaned by removing all comments and special characters from each '.java' file*/
    val codeDataWithOutComments = codeData.mapValues{inp =>       
    	val regexForComments = """(/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/)|(//.*)""".r/* 'regexForComments' represents a pattern for all types of comments in java program  */
      val regexSpecialChars = """[^a-zA-Z\n\s]""".r/* 'regexSpecialchars' represents a pattern for special characters*/
    	val commentsRemoved = regexForComments.replaceAllIn(inp,"")/* 'inp' represents content of a'.java' file and all occurences
      of 'regexForComments' is replaced by "", using 'replaceAllIn' method of scala's regex utility and stor the result in variable named 'commentsRemoved'*/
    	regexSpecialChars.replaceAllIn(commentsRemoved, " ")/* 'commentsRemoved' represents content of a'.java' file without any 
      comments and all occurences of 'regexSpecialChars' are replaced by " ", using 'replaceAllIn' method of scala's regex utility */
    }	

    /* Here 'codeData' RDD is unpersited because it is not needed*/  
    // codeData.unpersist()
    codeDataWithOutComments.persist()
    codeDataWithOutComments.count()
    /* Java reserve words are broadcasted to all nodes of a cluster */
    val reserveWords = sc.broadcast(keywords)



    /* In step this cleaned text is splitted into list of words. And reserved words are removed from the list of words*/
    val identifiersForEachDocument = codeDataWithOutComments.mapValues{inp =>      
      val listOfWords = inp.split("\\s+").toList/* Text 'inp' is splitted into list of words and stored in listOfWords'*/      
      val allReserveWords = reserveWords.value/* Broadcasted listed of reserve words are accessed and stored in 'allReserveWords' */      
      val removeReserveWords =  new ListBuffer[String]()/* 'removeReserveWords' is list buffer to strings */      
      for(word <- listOfWords if !allReserveWords.contains(word)) removeReserveWords+=word/* words other than reserve words
       are appended to 'removeReserveWords'*/      
      val bagOfWords = removeReserveWords.toList.sorted/* 'bagOfWords' contains sorted list of identifiers from a '.java'*/
      bagOfWords
    }


    /* Unpersisting 'codeDataWithOutComments and persisting 'words'  */
    // codeDataWithOutComments.unpersist()
    identifiersForEachDocument.persist()
    identifiersForEachDocument.count()

     /* Computing term-document frequencies for each document*/
    val termDocumentFrequencies = identifiersForEachDocument.mapValues{inp =>
      inp.groupBy(x => x).mapValues(_.size)/* Grouping based on uniqueness and computing size of each group
      to obtain (identifier,count) pairs */
    } 
     // identifiersForEachDocument.unpersist()
    termDocumentFrequencies.persist()
    termDocumentFrequencies.count()

    val docIds = termDocumentFrequencies.map(_._1).zipWithUniqueId().map(_.swap).collectAsMap().toMap

    /* In this step, document frequiencies for all the identifiers are calculated using flattening all identifier list across all
     documents. Firstly, sorted list of identifiers is grouped based on uniqueness inorder to calculate (identifier, count) pairs. 
     Secondly, each (identifier, count) pair is mapped to  (identifier, 1). Now each (identifier, 1) pair is reduced based on key to
     compute (identifier, df) where 'df' is the document frequency of identifier across all the documents*/
    val documentFrequencies = termDocumentFrequencies.flatMapValues(inp => inp).values.mapValues{inp => 1}.reduceByKey((x,y) => x+y).sortByKey() 
    /* Calculating document frequencies for all the terms across
    all the documents*/
    /* Grouping based on uniqueness and computing size of each group to obtain (identifier,count) pairs */
      // inp.groupBy(x => x).mapValues(_.size)  



    // val docFreqs = documentFrequencies.collect().sortBy(- _._2)
    
    val docFreqs = documentFrequencies.collect()/* Collecting (identifier, df) pairs from a  'documentFrequencies'*/   
    val numDocs = identifiersForEachDocument.count() /*Computing number of documents*/    
    val idfs = inverseDocumentFrequencies(docFreqs, numDocs)/* Computing inverse document frequencies 'idfs' from document frequencies */    
    val bidfs = sc.broadcast(idfs.toMap)/* Broadcasting 'idfs' across nodes of cluster*/   
    val vocabulary = documentFrequencies.keys.collect()/* Collecting all the identifiers from (identifier, df) pairs*/
    val termList = sc.broadcast(vocabulary)/* Broadcasting vocabulary across all nodes of clusters*/
    val termIds = vocabulary.zipWithIndex.map(_.swap).toMap 

   

    /* Computing tfidf from term frequencies and Inverse document frequencies */
    val tfidf = termDocumentFrequencies.mapValues{inp =>      
      val idf = bidfs.value/* Locally obtaining broadcasted bidfs values */
      val identifiersAcrossAllDocuments = termList.value/* Locally obtaining broadcasted  values */
      val termInThisDocument = inp.keySet.toList/* Obtaining all terms from this document*/
      val sizeOfVector = identifiersAcrossAllDocuments.size/* Computing number of terms(identifiers) across all the documents*/
      var tfidfMap:Map[Int,Double] = Map()/* Computing a map of (identifier, tfidf) pairs from term-document (identifier, count)
      pairs and document-frequency (identifier, idfs) pair */
      for(term <- termInThisDocument) {
        tfidfMap += (identifiersAcrossAllDocuments.indexOf(term) -> inp(term)*idf(term))
      }      
      val tfidfSeq = tfidfMap.toSeq/* Converting 'tfidfMap' to a sequence inorder to pass it to a document */
      Vectors.sparse(sizeOfVector, tfidfSeq)/*Obtaining sparse vector from 'tfidfSeq' and size of vector*/
    }

    // termDocumentFrequencies.unpersist()
    tfidf.cache()
    tfidf.persist()
    tfidf.count()
    
    val mat = new RowMatrix(tfidf.values)/* Constructing sparse matrix from tfidf sparse vectors obtained in the previous step*/

    val m = mat.numRows
    val n = mat.numCols

    // tfidf.unpersist()

    /* Computing svd from the 'mat' to obtain matrices*/
    val svd = mat.computeSVD(15, computeU=true)

    val topConceptTerms = topTermsInTopConcepts(svd, 10, 10, termIds)
    /* Extracts top ten terms from top most concepts */

    val topConceptDocs = topDocsInTopConcepts(svd, 10, 10, docIds)
    /* Extracts top ten documents from top most concepts */

    /* Printing top terms from top concepts and corresponding concepts */

    println("****************************************Number of Documents: " +numDocs +" **************************************")
    println("****************************************Number of Terms: " +vocabulary.size+" **************************************")
    // println("*********************************************** LIST OF WORDS ***************************************************")
    // identifiersForEachDocument.take(2).foreach(println)

    // println("****************************************Number of documents*********************"+termDocumentFrequencies.count())
    // println("************************************TERM DOCUMENT FREQUENCIES ***************************************************")    
    // termDocumentFrequencies.values.foreach(println)

    // println("*********************************************** VOCABULARY ***************************************************")
    // vocabulary.take(10).foreach(println)    

    // println("**************************************************Total terms *********************"+ documentFrequencies.count())
    // println("*********************************************** DOCUMET FREQUENCIES ***************************************************")
    // // documentFrequencies.foreach(println)

    // docFreqs.take(10).foreach(println)

    // println("*********************************************** TFIDF VECTORS ***************************************************")
    // tfidf.values.take(10).foreach(println)

    println("**********************************************************SVD computed*********************************************")
    println("Singular values: " + svd.s)

    println("Number of rows: "+m+ " " + "Number of Columns: "+n)
    println("**************************************************Doc Ids******************************************************")
    docIds.take(10).foreach(println)

    for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
      println("Concept terms: " + terms.map(_._1).mkString(", "))
      println("Concept docs: " + docs.map(_._1).mkString(", "))
      println()
    }

  }

  def inverseDocumentFrequencies(docFreqs: Array[(String, Int)], numDocs: Long)
    : Map[String, Double] = {
    docFreqs.map{ case (term, count) => (term, math.log(numDocs.toDouble / count))}.toMap
  }

  def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
      numTerms: Int, termIds: Map[Int, String]): Seq[Seq[(String, Double)]] = {
    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
    val arr = v.toArray
    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termWeights.sortBy(-_._1)
      topTerms += sorted.take(numTerms).map{case (score, id) => (termIds(id), score)}
    }
    topTerms
  }

  def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
      numDocs: Int, docIds: Map[Long, String]): Seq[Seq[(String, Double)]] = {
    val u  = svd.U
    val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
    for (i <- 0 until numConcepts) {
      val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId
      topDocs += docWeights.top(numDocs).map{case (score, id) => (docIds(id), score)}
    }
    topDocs
  }

  // def removeComments(inp: String): 
}