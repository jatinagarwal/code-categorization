/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

import org.apache.spark._
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Map
import scala.collection.mutable.ArrayBuffer



object LsaApp {
  def main(args: Array[String]) {
    val k = if (args.length > 0) args(0).toInt else 100
    /* 'k' is dimensionanlity of the term-documentt matrix which should be
    passed at command line if not passed then by default it is set to 100 */

    val numTopConcepts = if (args.length > 1) args(1).toInt else 10
    /* 'numTopConcepts' is the number of top concepts. If not passed then by default it is set to 10 */

    val numTopTerms = if (args.length > 2) args(2).toInt else 10
    /* 'numTopTerms' is the number of top terms in a concept. If not passed then by default it is set to 10 */

    val numTopDocs = if (args.length > 3) args(3).toInt else 10
    /* 'numTopDocs' is the number of top docs in a concept. If not passed then by default it is set to 10 */

    val dataFiles = if (args.length > 4) args(4).toString else "Data/sampleJava"
    /* 'dataFiles' implies path of the directory where data resides */
  /******************************************** INITIALIZATION STARTS HERE********************************************/
    /* 'keywords' is list of all reserve words in java programming language*/
  	val keywords = List("abstract", "continue", "for", "new", "switch", "assert", "default", "goto", "package",
    "synchronized", "boolean", "do", "if", "private", "this", "break", "double", "implements", "protected", "throw",
    "byte", "else", "import", "public", "throws", "case", "enum", "instanceof", "return", "transient", "catch",
    "extends", "int",	"short", "try", "char", "final", "interface", "static", "void", "class", "finally", "long",
    "strictfp", "volatile",	"const", "float", "native", "super", "while", "true", "false", "null", "String", "java",
    "util", "ArrayList", "println", "Arrays", "System", "File", "main")

    val conf = new SparkConf().setAppName("Lsa Application").set("spark.executor.memory", "10g")/* Spark Coonfiguration
     object in order to perform configuration settings. 'Lsa Application' is name of application*/
    val sc = new SparkContext(conf)/* 'sc' is spark context object to perform all spark related 
    operations with configuration setting from 'conf' */
    val codeData = sc.wholeTextFiles(dataFiles,10)/* 'codeData' is a RDD representing tuples of form 
    (fileName, fileContent) where 'fileContent' is content in a file named 'fileName'. Here both 'fileName' 
     and 'fileContent are strings. 'codeData' is cached to avoid memoery overhead */
    val reserveWords = sc.broadcast(keywords) /* Broadcasting reserve words to be used during data parsing*/
  /********************************************** INITIALIZATION ENDS HERE*********************************************/




  /**********************************************DATA PARSING STARTS HERE**********************************************/
    /* In this step, data is cleaned by removing all comments and special characters from each '.java' file*/
    val codeDataWithOutComments = codeData.mapValues{fileContent =>
    	// val regexForComments = """(/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/)|(//.*)""".r 
     //  /* 'regexForComments' represent	a pattern for all types of comments in java program  */
      val regexSpecialChars = """[^a-zA-Z\n\s]""".r/* 'regexSpecialchars' represents a pattern for special characters*/
    	// val commentsRemoved = regexForComments.replaceAllIn(fileContent,"")
      /* 'inp' represents content of a'.java' file and all occurences of 'regexForComments' is replaced by "", using 
      'replaceAllIn' method of scala's regex utility and store the result in variable named 'commentsRemoved'*/
    	regexSpecialChars.replaceAllIn(fileContent, " ")/* 'commentsRemoved' represents content of a'.java' file
    	without any comments and all occurences of 'regexSpecialChars' are replaced by " ", using 'replaceAllIn' method
    	of scala's regex utility */
    }	


    /* In step this cleaned text is splitted into list of words. And reserved words are removed from the list of words*/
    val identifiersForEachDocument = codeDataWithOutComments.mapValues{inp =>      
      val listOfWords = inp.split("\\s+").toList.filter(x => x != "") /* Text 'inp' is splitted into list of words and
      stored in listOfWords'*/
      val allReserveWords = reserveWords.value /* Broadcasted listed of reserve words are accessed and stored in
      'allReserveWords' */
      val removeReserveWords =  new ListBuffer[String]()/* 'removeReserveWords' is list buffer to strings */      
      for(word <- listOfWords if !allReserveWords.contains(word)) removeReserveWords+=word/* words other than reserve
       words are appended to 'removeReserveWords'*/
      val bagOfWords = removeReserveWords.toList.sorted/* 'bagOfWords' contains sorted list of identifiers from a
      '.java'*/
      bagOfWords
    }
  /*************************************************DATA PARSING ENDS HERE*********************************************/



/************************************************TERM FREQUENCIES STARTS HERE******************************************/
     /* Computing term-document frequencies for each document*/
    val termDocumentFrequencies = identifiersForEachDocument.mapValues{inp =>
      inp.groupBy(x => x).mapValues(_.size)/* Grouping based on uniqueness and computing size of each group
      to obtain (identifier,count) pairs */
    } 
    termDocumentFrequencies.persist(StorageLevel.MEMORY_AND_DISK) /* RDD 'termDocumentFrequencies' is persisted in the
    memory as two or more transformation are performed on it*/

    val docIds = termDocumentFrequencies.map(_._1).zipWithUniqueId().map(_.swap).collectAsMap().toMap
    /* Documents names are associated to unique ids */
/***********************************************TERM FREQUENCIES ENDS HERE*********************************************/



  /*******************************************DOCUMENT FREQUENCIES STARTS HERE*****************************************/
    /* In this step, document frequiencies for all the identifiers are calculated using flattening all identifier list
    across all documents. Firstly, sorted list of identifiers is grouped based on uniqueness inorder to calculate
    (identifier, count) pairs. Secondly, each (identifier, count) pair is mapped to  (identifier, 1). Now each
    (identifier, 1) pair is reduced based on key to compute (identifier, df) where 'df' is the document frequency of
     identifier across all the documents*/
    val documentFrequencies = termDocumentFrequencies.flatMapValues(inp => 
      inp).values.mapValues{inp => 1}.reduceByKey((x,y) => x+y).sortByKey() 
    /* In above step document frequencies are calculated for the terms across all the documents*/
  
    // val docFreqs = documentFrequencies.collect().sortBy(- _._2)
    val numDocs = identifiersForEachDocument.count() /*Computing number of documents*/
    val docFreqs = documentFrequencies.filter{ case(identifier,count) => count >1 && count <= numDocs/2}/* Filtering
    (identifier, df) pairs from 'documentFrequencies' based on optimization specified in paper*/
    docFreqs.persist(StorageLevel.MEMORY_AND_DISK)/* RDD 'docFreqs' is persisted in the memory as two or more 
    transformation are performed on it*/
    val idfs = inverseDocumentFrequencies(docFreqs.collect(), numDocs)/* Computing inverse document frequencies 'idfs'
    from document frequencies */
    val bidfs = sc.broadcast(idfs.toMap)/* Broadcasting 'idfs' across nodes of cluster*/   
    val vocabulary = docFreqs.keys.collect()/* Collecting all the identifiers from filtering (identifier, df) pairs*/
    val termList = sc.broadcast(vocabulary)/* Broadcasting vocabulary across all nodes of clusters*/
    val termIds = vocabulary.zipWithIndex.map(_.swap).toMap /* Terms are associated with the ids as shown (id, term) */
/*******************************************DOCUMENT FREQUENCIES ENDS HERE*********************************************/



   
/**********************************************TFIDF COMPUTATION STARTS HERE*******************************************/
    /* Computing tfidf from term frequencies and Inverse document frequencies */
    val tfidf = termDocumentFrequencies.mapValues{termFreqPair =>
      val idf = bidfs.value/* Locally obtaining broadcasted bidfs values */
      val allIdentifiers = termList.value/* Locally obtaining broadcasted  values */
      val termInThisDocument = termFreqPair.keySet.toList/* Obtaining all terms from this document*/
      val filteredTerms =  new ListBuffer[String]()
      for(term <- termInThisDocument if allIdentifiers.contains(term)) filteredTerms+=term /* All relevant documents
       are filtered  and stored in a 'filteredTerms' */
      val filteredTermsOfThisDocument = filteredTerms.toList
      val sizeOfVector = allIdentifiers.size/* Computing number of terms(identifiers) across all the documents*/
      var tfidfMap:Map[Int,Double] = Map()/* Computing a map of (identifier, tfidf) pairs from term-document
       (identifier, count) pairs and document-frequency (identifier, idfs) pair */
      for(term <- filteredTermsOfThisDocument) {
        tfidfMap += (allIdentifiers.indexOf(term) -> termFreqPair(term)*idf(term)) /* TFIDF computation */
      }      
      val tfidfSeq = tfidfMap.toSeq/* Converting 'tfidfMap' map to a sequence */
      Vectors.sparse(sizeOfVector, tfidfSeq) /*Obtaining sparse vector from 'tfidfSeq' sequence and 'sizeOfVector'*/
    }
    tfidf.persist(StorageLevel.MEMORY_AND_DISK)/* RDD 'tfidf' is persisted in the memory as two or more 
    operations are performed while computing row matrix*/
    tfidf.count() /* Action is performed on tfidf vector in order to evaluate tfidf as it is needed in next step */
   
/**********************************************TFIDF COMPUTATION ENDS HERE*********************************************/
  


/**********************************************SVD COMPUTATION STARTS HERE*********************************************/
    /* Constructing sparse matrix from tfidf sparse vectors obtained in the previous step*/
    val mat = new RowMatrix(tfidf.values)

    val m = mat.numRows /* Number of rows in a matrix */
    val n = mat.numCols /* Number of columns in a matrix */
    termDocumentFrequencies.unpersist()
    docFreqs.unpersist()

    /* Computing svd from the 'mat' to obtain matrices*/
    val svd = mat.computeSVD(k, computeU=true)
/**********************************************SVD COMPUTATION ENDS HERE***********************************************/
    /* Extracts top terms from top most concepts */
    val topConceptTerms = topTermsInTopConcepts(svd, numTopConcepts, numTopTerms, termIds)
    /* Extracts top documents from top most concepts */
    val topConceptDocs = topDocsInTopConcepts(svd, numTopConcepts, numTopDocs, docIds)
    
/*******************************************CONSOLE PRINTING STARTS HERE***********************************************/
    println("********************************Number of Documents: " +numDocs +" **************************************")
    // println("***********************************Total terms ***************************: "+ documentFrequencies.count())
    println("****************************Number of Terms after filtering: " +vocabulary.size+" ***********************")
    println("*************************************** LIST OF WORDS ***************************************************")
//    identifiersForEachDocument.take(2).foreach(println)
//
//    println("*******************************Number of documents*********************"+termDocumentFrequencies.count())
//    println("************************************TERM DOCUMENT FREQUENCIES *****************************************")
//    termDocumentFrequencies.values.foreach(println)
//
//    println("*********************************************** VOCABULARY ********************************************")
//    vocabulary.foreach(println)
//
//    println("*********************************************** TERM IDS **********************************************")
//    termIds.foreach(println)
//
//    println("***************************************** DOCUMET FREQUENCIES *****************************************")
//    documentFrequencies.foreach(println)
//    println("**************************************FILTERED DOCUMET FREQUENCIES ************************************")
//    docFreqs.foreach(println)
//
//    println("***************************************** TFIDF VECTORS ***********************************************")
//    tfidf.values.take(10).foreach(println)

    println("************************************************SVD computed*********************************************")
    println("Singular values: " + svd.s)

    println("Number of rows: "+m+ " " + "Number of Columns: "+n)
    println("**********************************************Doc Ids****************************************************")
    docIds.take(10).foreach(println)
    val regexToRemovePath = """.*\/""".r
    for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
      println("Concept terms: " + terms.map(_._1).mkString(", ")  )
      // println("Concept docs: " + docs.map(_._1).mkString(", "))
      println("Concept docs: ")
      docs.map(_._1).foreach{x => println(regexToRemovePath.replaceAllIn(x,""))}
      println()
    }
/*********************************************CONSOLE PRINTING ENDS HERE***********************************************/

  }
  /* FUNCTION TO COMPUTE INVERSE DCOUMENT FREQUENCIES*/
  def inverseDocumentFrequencies(docFreqs: Array[(String, Int)], numDocs: Long)
    : Map[String, Double] = {
    docFreqs.map{ case (term, count) => (term, math.log(numDocs.toDouble / count))}.toMap
  }

  /* FUNCTION TO COMPUTE TOP TERMS IN TOP CONCEPTS*/
  def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
      numTerms: Int, termIds: Map[Int, String]): Seq[Seq[(String, Double)]] = {
    val v = svd.V /* Matrix representing term space*/
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
    val arr = v.toArray
    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex /* Picking each column of the matrix 'v'*/
      val sorted = termWeights.sortBy(-_._1)
      topTerms += sorted.take(numTerms).map{case (score, id) => (termIds(id), score)} /* Associating scores with
      corresponding terms using termIds*/
    }
    topTerms
  }

  /* FUNCTION TO COMPUTE TOP DOCUMENTS IN TOP CONCEPTS*/
  def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
      numDocs: Int, docIds: Map[Long, String]): Seq[Seq[(String, Double)]] = {
    val u  = svd.U /* Matrix representing document space*/
    val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
    for (i <- 0 until numConcepts) {
      val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId  /* Picking each row of the row matrix 'u'*/
      topDocs += docWeights.top(numDocs).map{case (score, id) => (docIds(id), score)} /* Associating scores with
      corresponding documents using docIds */
    }
    topDocs
  }
}