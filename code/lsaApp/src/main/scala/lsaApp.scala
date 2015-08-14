/* SimpleApp.scala */
package com.lsa.app
import com.lsa.app.ZipBasicParser
import com.lsa.app.JavaCommentsRemover
import com.lsa.app.CamelCase

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.input.PortableDataStream
import org.apache.spark.storage.StorageLevel

import breeze.linalg.{DenseMatrix => BDenseMatrix, DenseVector => BDenseVector, SparseVector => BSparseVector}
import org.apache.spark._
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition, Vectors, Matrices, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import java.util.zip.ZipInputStream
import java.io._

import scala.Predef
import scala.collection.immutable
import scala.util.matching.Regex
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Map
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Try
import scala.util.control.Breaks._

object LsaApp extends Logger {
  def main(args: Array[String]) {
    val k = if (args.length > 0) args(0).toInt else 100
    /* 'k' is dimensionanlity of the term-documentt matrix which should be
    passed at command line if not passed then by default it is set to 100 */

    val numTopConcepts: Int = if (args.length > 1) args(1).toInt else 10
    /* 'numTopConcepts' is the number of top concepts. If not passed then by default it is set to 10 */

    val numTopTerms: Int = if (args.length > 2) args(2).toInt else 10
    /* 'numTopTerms' is the number of top terms in a concept. If not passed then by default it is set to 10 */

    val numTopDocs: Int = if (args.length > 3) args(3).toInt else 10
    /* 'numTopDocs' is the number of top docs in a concept. If not passed then by default it is set to 10 */

     val featureVectorPercent = if (args.length > 4) args(4).toDouble else 1.0
    /* 'featureVectorPercent' is the percentage of terms with top document frequencies feature vector*/

    val dataFiles: String = if (args.length > 5) args(5).toString else "Data/sampleJava"
    /* 'dataFiles' implies path of the directory where data resides */
  /******************************************** INITIALIZATION STARTS HERE********************************************/
    /* 'keywords' is list of all reserve words in java programming language*/
    var a: Char = readChar()
    val keywords: List[String] = List("abstract", "continue", "for", "new", "switch", "assert", "default", "goto", "package",
    "synchronized", "boolean", "do", "if", "private", "this", "break", "double", "implements", "protected", "throw",
    "byte", "else", "import", "public", "throws", "case", "enum", "instanceof", "return", "transient", "catch",
    "extends", "int",	"short", "try", "char", "final", "interface", "static", "void", "class", "finally", "long",
    "strictfp", "volatile",	"const", "float", "native", "super", "while", "true", "false", "null", "String", "java",
    "util", "ArrayList", "println", "Arrays", "System", "File", "main")

    val conf: SparkConf = new SparkConf().setAppName("Lsa Application").set("spark.executor.memory", "14g")/* Spark Coonfiguration
     object in order to perform configuration settings. 'Lsa Application' is name of application*/
    val sc: SparkContext = new SparkContext(conf)/* 'sc' is spark context object to perform all spark related
    operations with configuration setting from 'conf' */
    val codeData: RDD[(String, PortableDataStream)] = sc.binaryFiles(dataFiles)/* 'codeData' is a RDD representing tuples of form
    (fileName, fileContent) where 'fileContent' is content in a file named 'fileName'. Here both 'fileName' 
     and 'fileContent are strings. 'codeData' is cached to avoid memoery overhead */
    //codeData.persist(StorageLevel.MEMORY_AND_DISK)
    val reserveWords: Broadcast[List[String]] = sc.broadcast(keywords) /* Broadcasting reserve words to be used during data parsing*/
  /********************************************** INITIALIZATION ENDS HERE*********************************************/


  /*******************************************************************************************************************/
  val regexToRemovePath: Regex = """.*\/""".r
  val zipStreamToText : RDD[(String, String)] = codeData.map{
    case(fileName, zipStream) =>
      val zipInputStream: DataInputStream = zipStream.open()
      val (fileContent,count) = ZipBasicParser.readFilesAndPackages(new ZipInputStream(zipInputStream))
      println(fileName +":"+count)
      zipInputStream.close()
      val repoName: String = regexToRemovePath.replaceAllIn(fileName,"")
      (repoName,fileContent)
  }


  val commentsRemoved: RDD[(String, String)] = zipStreamToText.mapValues{fileContent =>
    val reader:Reader = new StringReader(fileContent)
    val writer: StringWriter = new StringWriter()
    val jcr: JavaCommentsRemover = new JavaCommentsRemover(reader,writer)
    val codeWithOutComments: String = jcr.process()
    codeWithOutComments   
  }
  /*******************************************************************************************************************/




  /**********************************************DATA PARSING STARTS HERE**********************************************/
    /* In this step, data is cleaned by removing all comments and special characters from each '.java' file*/
    val codeDataWithOutComments: RDD[(String, String)] = commentsRemoved.mapValues{fileContent =>
    	// val regexForComments = """(/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/)|(//.*)""".r 
     //  /* 'regexForComments' represent	a pattern for all types of comments in java program  */
      val regexSpecialChars: Regex = """[^a-zA-Z\s]""".r/* 'regexSpecialchars' represents a pattern for special characters*/
    	// val commentsRemoved = regexForComments.replaceAllIn(fileContent,"")
      /* 'inp' represents content of a'.java' file and all occurences of 'regexForComments' is replaced by "", using 
      'replaceAllIn' method of scala's regex utility and store the result in variable named 'commentsRemoved'*/
    	regexSpecialChars.replaceAllIn(fileContent, " ")/* 'commentsRemoved' represents content of a'.java' file
    	without any comments and all occurences of 'regexSpecialChars' are replaced by " ", using 'replaceAllIn' method
    	of scala's regex utility */
    }	


    /* In step this cleaned text is splitted into list of words. And reserved words are removed from the list of words*/
    val identifiersForEachDocument: RDD[(String, List[String])] = codeDataWithOutComments.mapValues{inp =>
      val listOfWords: List[String] = inp.split("\\s+").toList.filter(x => x != "") /* Text 'inp' is splitted into list of words and
      stored in listOfWords'*/
      val allReserveWords: List[String] = reserveWords.value /* Broadcasted listed of reserve words are accessed and stored in
      'allReserveWords' */
      val removeReserveWords: ListBuffer[String] =  new ListBuffer[String]()/* 'removeReserveWords' is list buffer to strings */
      for(word <- listOfWords if !allReserveWords.contains(word)) removeReserveWords+=word
      /* words other than reserve words are appended to 'removeReserveWords'*/
//       for(word <- listOfWords if !allReserveWords.contains(word)) {
//         val cc:CamelCase = new CamelCase()
//         val words: Array[String] = cc.splitCamelCase(word).split(" ")
//         for(w <- words) {
//           removeReserveWords+=w.toLowerCase
//           /* words other than reserve words are appended to 'removeReserveWords'*/
//         }
//       }
      val bagOfWords:List[String] = removeReserveWords.toList.sorted/* 'bagOfWords' contains sorted list of identifiers from a
      '.java'*/
      bagOfWords
    }
  /*************************************************DATA PARSING ENDS HERE*********************************************/



/************************************************TERM FREQUENCIES STARTS HERE******************************************/
     /* Computing term-document frequencies for each document*/
    val termDocumentFrequencies: RDD[(String, Predef.Map[String, Int])] = identifiersForEachDocument.mapValues{inp =>
      inp.groupBy(x => x).mapValues(_.size)/* Grouping based on uniqueness and computing size of each group
      to obtain (identifier,count) pairs */
    } 
    termDocumentFrequencies.persist(StorageLevel.MEMORY_AND_DISK)
   /* RDD 'termDocumentFrequencies' is persisted in the memory as two or more transformation are performed on it*/
    val numDocs: Long = termDocumentFrequencies.count() /*Computing number of documents*/

    val idDocs: Predef.Map[String, Long] = termDocumentFrequencies.map(_._1).zipWithUniqueId().collectAsMap().toMap
    val docIds: Predef.Map[Long, String] = idDocs.map(_.swap)
    /* Documents names are associated to unique ids */
/***********************************************TERM FREQUENCIES ENDS HERE*********************************************/



  /*******************************************DOCUMENT FREQUENCIES STARTS HERE*****************************************/
    /* In this step, document frequiencies for all the identifiers are calculated using flattening all identifier list
    across all documents. Firstly, sorted list of identifiers is grouped based on uniqueness inorder to calculate
    (identifier, count) pairs. Secondly, each (identifier, count) pair is mapped to  (identifier, 1). Now each
    (identifier, 1) pair is reduced based on key to compute (identifier, df) where 'df' is the document frequency of
     identifier across all the documents*/
    val documentFrequencies: RDD[(String, Int)] = termDocumentFrequencies.flatMapValues(inp =>
      inp).values.mapValues{inp => 1}.reduceByKey((x,y) => x+y)
    /* In above step document frequencies are calculated for the terms across all the documents*/
  
    // val docFreqs = documentFrequencies.collect().sortBy(- _._2)    

    
    val docFreqs: RDD[(String, Int)] = documentFrequencies.filter{ case(identifier,count) => count >1 && count <= numDocs/5}.map{case(id,c) =>
      (-c,id)}.sortByKey().map{case(c,id) => (id,-c)}
    /* Filtering (identifier, df) pairs from 'documentFrequencies' based on optimization specified in paper*/
    docFreqs.persist(StorageLevel.MEMORY_AND_DISK)
    val numTerms: Long = docFreqs.count()
    /* RDD 'docFreqs' is persisted in the memory as two or more transformation are performed on it*/
    var featureVectorSize:Int = (numTerms*featureVectorPercent).toInt;
    val featureVector: Array[(String, Int)] = docFreqs.take(featureVectorSize)
    val idfs: Array[(String, Double)] = inverseDocumentFrequencies(featureVector, numDocs)
    /* Computing inverse document frequencies 'idfs' from document frequencies */
    val idfsMap: Predef.Map[String, Double] = idfs.toMap
    val bidfs: Broadcast[Predef.Map[String, Double]] = sc.broadcast(idfsMap)/* Broadcasting 'idfs' across nodes of cluster*/
    val vocabulary: Predef.Map[String, Int] = idfsMap.keys.zipWithIndex.toMap/* Collecting all the identifiers after filtering (identifier, df) pairs*/
    val termList: Broadcast[Predef.Map[String, Int]] = sc.broadcast(vocabulary)/* Broadcasting vocabulary across all nodes of clusters*/
    val termIds: Predef.Map[Int, String] = vocabulary.map(_.swap).toMap /* Terms are associated with the ids as shown (id, term) */
    docFreqs.unpersist()
/*******************************************DOCUMENT FREQUENCIES ENDS HERE*********************************************/



   
/**********************************************TFIDF COMPUTATION STARTS HERE*******************************************/
    /* Computing tfidf from term frequencies and Inverse document frequencies */
    val tfidf: RDD[(String, Vector)] = termDocumentFrequencies.mapValues{termFreqPair =>
      val idf: Predef.Map[String, Double] = bidfs.value/* Locally obtaining broadcasted bidfs values */
      val allIdentifiers: Predef.Map[String, Int] = termList.value/* Locally obtaining broadcasted  values */
      val docTotalTerms: Int = termFreqPair.values.sum
      val termInThisDocument: List[String] = termFreqPair.keySet.toList/* Obtaining all terms from this document*/
      val sizeOfVector: Int = allIdentifiers.size/* Computing number of terms(identifiers) across all the documents*/
      var tfidfMap:Map[Int,Double] = Map()/* Computing a map of (identifier, tfidf) pairs from term-document
       (identifier, count) pairs and document-frequency (identifier, idfs) pair */
      for(term <- termInThisDocument if allIdentifiers.contains(term)) {
        tfidfMap += (allIdentifiers(term) -> termFreqPair(term)*idf(term)/docTotalTerms) /* TFIDF computation */
      }      
      val tfidfSeq: Seq[(Int, Double)] = tfidfMap.toSeq/* Converting 'tfidfMap' map to a sequence */
      Vectors.sparse(sizeOfVector, tfidfSeq) /*Obtaining sparse vector from 'tfidfSeq' sequence and 'sizeOfVector'*/
    }
    tfidf.persist(StorageLevel.MEMORY_AND_DISK)/* RDD 'tfidf' is persisted in the memory as two or more 
    operations are performed while computing row matrix*/
    tfidf.count() /* Action is performed on tfidf vector in order to evaluate tfidf as it is needed in next step */
   
/**********************************************TFIDF COMPUTATION ENDS HERE*********************************************/
  


/**********************************************SVD COMPUTATION STARTS HERE*********************************************/
    /* Constructing sparse matrix from tfidf sparse vectors obtained in the previous step*/
    termDocumentFrequencies.unpersist()
    val mat: RowMatrix = new RowMatrix(tfidf.values)

    val m: Long = mat.numRows /* Number of rows in a matrix */
    val n: Long = mat.numCols /* Number of columns in a matrix */
    

    /* Computing svd from the 'mat' to obtain matrices*/
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(k, computeU=true)
/**********************************************SVD COMPUTATION ENDS HERE***********************************************/
    /* Extracts top terms from top most concepts */
    val topConceptTerms: Seq[Seq[(String, Double)]] = topTermsInTopConcepts(svd, numTopConcepts, numTopTerms, termIds)
    /* Extracts top documents from top most concepts */
    val topConceptDocs: Seq[Seq[(String, Double)]] = topDocsInTopConcepts(svd, numTopConcepts, numTopDocs, docIds)

    val US: RowMatrix = multiplyByDiagonalMatrix(svd.U, svd.s)
    val normalizedUS: RowMatrix = rowsNormalized(US)

    val VS: BDenseMatrix[Double] = multiplyByDiagonalMatrix(svd.V, svd.s)
    val normalizedVS: BDenseMatrix[Double] = rowsNormalized(VS)
/*******************************************CONSOLE PRINTING STARTS HERE***********************************************/
    println("********************************Number of Documents: " +numDocs +" **************************************")
    println("**************************Size of Feature Vector: " +featureVectorSize +" *******************************")
    // println("***********************************Total terms ***************************: "+ documentFrequencies.count())
    println("****************************Number of Terms after filtering: " +numTerms+" ***********************")
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
   println("**************************************FILTERED DOCUMET FREQUENCIES ************************************")
   featureVector.foreach(println)
//
//    println("***************************************** TFIDF VECTORS ***********************************************")
//    tfidf.values.take(10).foreach(println)

    println("************************************************SVD computed*********************************************")
    println("Singular values: " + svd.s)

    println("Number of rows: "+m+ " " + "Number of Columns: "+n)
    println("**********************************************Doc Ids****************************************************")
      docIds.take(10).foreach(println)
        
    for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
      println("Concept terms: " + terms.map(_._1).mkString(", ")  )
      // println("Concept docs: " + docs.map(_._1).mkString(", "))
      println("Concept docs: ")
      docs.map(_._1).foreach(println)
      println()
    }

    println("Enter a path to a repo to find similar repos:")
    breakable {
      for (doc <- Source.stdin.getLines) {
        if(doc.toString == "break")
          break
        println("Top documents for "+doc+" are:")
        topDocsForNewDoc(normalizedUS, svd.V, vocabulary, idfsMap, docIds, doc.toString, numTopDocs, bidfs, termList, reserveWords)
        println("Enter a path to a repo to find similar repos:")
      }
    }

    println("Enter a repo to find similar repos:")
    breakable {
      for (doc <- Source.stdin.getLines) {
        if(doc.toString == "break")
          break
        println("Top documents for "+doc+" are:")
        printTopDocsForDoc(normalizedUS, doc.toString, idDocs, docIds, numTopDocs)
        println("Enter a repo to find similar repos:")
      }
    }

    println("Enter a term to find similar terms:")
    breakable {
      for (term <- Source.stdin.getLines) {
        if(term.toString == "break")
          break
        println("Top terms for "+term+" are:")
        printRelevantTerms(term.toString, normalizedVS, vocabulary, termIds)
        println("Enter a term to find similar terms:")
      }
    }

    println("Enter a term to find repos containing it:")
    breakable {
      for (term <- Source.stdin.getLines) {
        if(term.toString == "break")
          break
        println("Top documents contianing "+term+":")
        printTopDocsForTerm(normalizedUS, svd.V, term.toString, vocabulary, docIds, numTopDocs)
        println("Enter a terms to find repos containing it:")
      }
    }

    println("Enter set of term to find repos containing it:")
    breakable {
      for (term <- Source.stdin.getLines) {
        if(term.toString == "break")
          break
        println("Top documents contianing "+term+":")
        val termSeq = term.toString.split(",").toSeq
        printRelevantDocs(normalizedUS, svd.V, termSeq, vocabulary, idfsMap, docIds, numTopDocs)
        println("Enter set of terms to find repos containing it:")
      }
    }
    
/*********************************************CONSOLE PRINTING ENDS HERE***********************************************/

  }
  /* FUNCTION TO COMPUTE INVERSE DCOUMENT FREQUENCIES*/
  def inverseDocumentFrequencies(docFreqs: Array[(String, Int)], numDocs: Long)
    : Array[(String, Double)] = {
    docFreqs.map{ case (term, count) => (term, math.log(numDocs.toDouble / count))}
  }

  /* FUNCTION TO COMPUTE TOP TERMS IN TOP CONCEPTS*/
  def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
      numTerms: Int, termIds: Map[Int, String]): Seq[Seq[(String, Double)]] = {
    val v: Matrix = svd.V /* Matrix representing term space*/
    val topTerms: ArrayBuffer[Seq[(String, Double)]] = new ArrayBuffer[Seq[(String, Double)]]()
    val arr: Array[Double] = v.toArray
    for (i <- 0 until numConcepts) {
      val offs: Int = i * v.numRows
      val termWeights: Array[(Double, Int)] = arr.slice(offs, offs + v.numRows).zipWithIndex /* Picking each column of the matrix 'v'*/
      val sorted: Array[(Double, Int)] = termWeights.sortBy(-_._1)
      topTerms += sorted.take(numTerms).map{case (score, id) => (termIds(id), score)} /* Associating scores with
      corresponding terms using termIds*/
    }
    topTerms
  }

  /* FUNCTION TO COMPUTE TOP DOCUMENTS IN TOP CONCEPTS*/
  def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
      numDocs: Int, docIds: Map[Long, String]): Seq[Seq[(String, Double)]] = {
    val u: RowMatrix = svd.U /* Matrix representing document space*/
    val topDocs: ArrayBuffer[Seq[(String, Double)]] = new ArrayBuffer[Seq[(String, Double)]]()
    for (i <- 0 until numConcepts) {
      val docWeights: RDD[(Double, Long)] = u.rows.map(_.toArray(i)).zipWithUniqueId  /* Picking each row of the row matrix 'u'*/
      topDocs += docWeights.top(numDocs).map{case (score, id) => (docIds(id), score)} /* Associating scores with
      corresponding documents using docIds */
    }
    topDocs
  }

  /**
   * Selects a row from a matrix.
   */
  def row(mat: BDenseMatrix[Double], index: Int): Seq[Double] = {
    (0 until mat.cols).map(c => mat(index, c))
  }


   /**
   * Selects a row from a matrix.
   */
  def row(mat: Matrix, index: Int): Seq[Double] = {
    val arr: Array[Double] = mat.toArray
    (0 until mat.numCols).map(i => arr(index + i * mat.numRows))
  }

   /**
   * Selects a row from a distributed matrix.
   */
  def row(mat: RowMatrix, id: Long): Array[Double] = {
    mat.rows.zipWithUniqueId.map(_.swap).lookup(id).head.toArray
  }

  /**
   * Returns a matrix where each row is divided by its length.
   */
  def rowsNormalized(mat: BDenseMatrix[Double]): BDenseMatrix[Double] = {
    val newMat: BDenseMatrix[Double] = new BDenseMatrix[Double](mat.rows, mat.cols)
    for (r <- 0 until mat.rows) {
      val length: Double = math.sqrt((0 until mat.cols).map(c => mat(r, c) * mat(r, c)).sum)
      (0 until mat.cols).map(c => newMat.update(r, c, mat(r, c) / length))
    }
    newMat
  }


  /**
   * Returns a distributed matrix where each row is divided by its length.
   */
  def rowsNormalized(mat: RowMatrix): RowMatrix = {
    new RowMatrix(mat.rows.map(vec => {
      val length: Double = math.sqrt(vec.toArray.map(x => x * x).sum)
      Vectors.dense(vec.toArray.map(_ / length))
    }))
  }

   /**
   * Finds the product of a dense matrix and a diagonal matrix represented by a vector.
   * Breeze doesn't support efficient diagonal representations, so multiply manually.
   */
  def multiplyByDiagonalMatrix(mat: Matrix, diag: Vector): BDenseMatrix[Double] = {
    val sArr: Array[Double] = diag.toArray
    new BDenseMatrix[Double](mat.numRows, mat.numCols, mat.toArray)
      .mapPairs{case ((r, c), v) => v * sArr(c)}
  }

  def multiplyByDiagonalMatrix(mat: RowMatrix, diag: Vector): RowMatrix = {
    val sArr: Array[Double] = diag.toArray
    new RowMatrix(mat.rows.map(vec => {
      val vecArr: Array[Double] = vec.toArray
      val newArr: Array[Double] = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
      Vectors.dense(newArr)
    }))
  }

  /**
   * Finds docs relevant to a doc. Returns the doc IDs and scores for the docs with the highest
   * relevance scores to the given doc.
   */
  def topDocsForDoc(normalizedUS: RowMatrix, docId: Long, numTopDocs:Int): Seq[(Double, Long)] = {
    // Look up the row in US corresponding to the given doc ID.
    val docRowArr: Array[Double] = row(normalizedUS, docId)
    val docRowVec: Matrix = Matrices.dense(docRowArr.length, 1, docRowArr)


    // Compute scores against every doc
    val docScores: RowMatrix = normalizedUS.multiply(docRowVec)

    // Find the docs with the highest scores
    val allDocWeights: RDD[(Double, Long)] = docScores.rows.map(_.toArray(0)).zipWithUniqueId

    // Docs can end up with NaN score if their row in U is all zeros.  Filter these out.
    allDocWeights.filter(!_._1.isNaN).top(numTopDocs)
  }

  def topDocsForNewDoc(normalizedUS: RowMatrix, V: Matrix, idTerms: Map[String, Int],
   idfs: Map[String, Double], docIds: Map[Long, String], newDocPath: String, numTopDocs:Int,
    bidfs:Broadcast[Map[String,Double]], termList:Broadcast[Map[String,Int]],
    reserveWords:Broadcast[List[String]]) = {
    val zipFile:FileInputStream = new FileInputStream(newDocPath)
    val zipIn:ZipInputStream = new ZipInputStream(zipFile)
    val (fileContent,count) = ZipBasicParser.readFilesAndPackages(zipIn)

    val reader:Reader = new StringReader(fileContent)
    val writer: StringWriter = new StringWriter()
    val jcr: JavaCommentsRemover = new JavaCommentsRemover(reader,writer)
    val codeWithOutComments = jcr.process()

    val regexSpecialChars: Regex = """[^a-zA-Z\s]""".r
    val lines = regexSpecialChars.replaceAllIn(fileContent, " ")

    val listOfWords: List[String] = lines.split("\\s+").toList.filter(x => x != "") /* Text 'inp' is splitted into list of words and
      stored in listOfWords'*/
    val allReserveWords: List[String] = reserveWords.value /* Broadcasted listed of reserve words are accessed and stored in
      'allReserveWords' */
    val removeReserveWords: ListBuffer[String] =  new ListBuffer[String]()  /* 'removeReserveWords' is list buffer to strings */
    for(word <- listOfWords if !allReserveWords.contains(word)) removeReserveWords+=word
    /* words other than reserve words are appended to 'removeReserveWords'*/
    // for(word <- listOfWords if !allReserveWords.contains(word)) {
    //   val cc:CamelCase = new CamelCase()
    //   val words = cc.splitCamelCase(word).split(" ")
    //   for(w <- words) {
    //     removeReserveWords+= w.toLowerCase
    //     /* words other than reserve words are appended to 'removeReserveWords'*/
    //   }
    // }
    val bagOfWords:List[String] = removeReserveWords.toList.sorted/* 'bagOfWords' contains sorted list of identifiers from a
      '.java'*/
    val pairs: Predef.Map[String, Int] = bagOfWords.groupBy(x => x).mapValues(_.size)

    val idf: Predef.Map[String, Double] = bidfs.value/* Locally obtaining broadcasted bidfs values */
    val allIdentifiers: Predef.Map[String, Int] = termList.value/* Locally obtaining broadcasted  values */
    val docTotalTerms: Int = pairs.values.sum
    val termInThisDocument: List[String] = pairs.keySet.toList/* Obtaining all terms from this document*/
    val sizeOfVector: Int = allIdentifiers.size/* Computing number of terms(identifiers) across all the documents*/
    var tfidfMap:Map[String,Double] = Map()/* Computing a map of (identifier, tfidf) pairs from term-document
       (identifier, count) pairs and document-frequency (identifier, idfs) pair */
    var termSequence: ListBuffer[String] = new ListBuffer[String]()
    for(term <- termInThisDocument if allIdentifiers.contains(term)) {
      tfidfMap += (term -> pairs(term)*idf(term)/docTotalTerms) /* TFIDF computation */
      termSequence += term
    }
    val termSeq: immutable.Seq[String] = termSequence.toList.toSeq
    printRelevantDocs(normalizedUS, V, termSeq, idTerms, tfidfMap, docIds, numTopDocs)
    // val tfidfValues = tfidfMap.values.toArray
  }

  def printIdWeights[T](idWeights: Seq[(Double, T)], entityIds: Map[T, String]) {
    idWeights.map{case (score, id) => (entityIds(id), score)}.foreach(println)
  }

  def printTopDocsForDoc(normalizedUS: RowMatrix, doc: String, idDocs: Map[String, Long],
      docIds: Map[Long, String], numTopDocs:Int) {
    try {
      val docID:Long = idDocs(doc)
      printIdWeights[Long](topDocsForDoc(normalizedUS, docID, numTopDocs), docIds)
    } catch {
      case ex: Exception => log.error("Exception doc not found {}", ex)
      println("Term not found. Enter another term")
    }
  }

  def topDocsForTerm(US: RowMatrix, V: Matrix, termId: Int, numTopDocs:Int): Seq[(Double, Long)] = {
    val termRowArr: Array[Double] = row(V, termId).toArray
    val termRowVec: Matrix = Matrices.dense(termRowArr.length, 1, termRowArr)

    // Compute scores against every doc
    val docScores: RowMatrix = US.multiply(termRowVec)

    // Find the docs with the highest scores
    val allDocWeights: RDD[(Double, Long)] = docScores.rows.map(_.toArray(0)).zipWithUniqueId
    allDocWeights.filter(!_._1.isNaN).top(numTopDocs)
  }

  def printTopDocsForTerm(US: RowMatrix, V: Matrix, term: String, idTerms: Map[String, Int],
      docIds: Map[Long, String], numTopDocs:Int) {
    try {
      val termID:Int = idTerms(term)
      printIdWeights[Long](topDocsForTerm(US, V, termID, numTopDocs), docIds)
    } catch {
      case ex: Exception => log.error("Exception term not found {}", ex)
      println("Term not found. Enter another term")
    }
  }

  def termsToQueryVector(terms: Seq[String], idTerms: Map[String, Int], idfs: Map[String, Double])
    : BSparseVector[Double] = {
    val indices: Array[Int] = terms.map(idTerms(_)).toArray
    val values: Array[Double] = terms.map(idfs(_)).toArray
    new BSparseVector[Double](indices, values, idTerms.size)
  }

  def topDocsForTermQuery(normalizedUS: RowMatrix, V: Matrix, query: BSparseVector[Double], numTopDocs:Int)
    : Seq[(Double, Long)] = {
    val breezeV: BDenseMatrix[Double] = new BDenseMatrix[Double](V.numRows, V.numCols, V.toArray)
    val termRowArr: Array[Double] = (breezeV.t * query).toArray
    //val termRowArr = (normalizedVS.t * query).toArray


    val termRowVec: Matrix = Matrices.dense(termRowArr.length, 1, termRowArr)

    // Compute scores against every doc
    val docScores: RowMatrix = normalizedUS.multiply(termRowVec)

    // Find the docs with the highest scores
    val allDocWeights: RDD[(Double, Long)] = docScores.rows.map(_.toArray(0)).zipWithUniqueId
    allDocWeights.filter(!_._1.isNaN).top(numTopDocs)
  }

  def printRelevantDocs(normalizedUS: RowMatrix, V: Matrix, terms: Seq[String], idTerms: Map[String, Int],
   idfs: Map[String, Double], docIds: Map[Long, String], numTopDocs:Int) {
    val queryVec: BSparseVector[Double] = termsToQueryVector(terms, idTerms, idfs)
    printIdWeights(topDocsForTermQuery(normalizedUS, V, queryVec, numTopDocs), docIds)
  }

  /**
   * Finds terms relevant to a term. Returns the term IDs and scores for the terms with the highest
   * relevance scores to the given term.
   */
  def topTermsForTerm(normalizedVS: BDenseMatrix[Double], termId: Int): Seq[(Double, Int)] = {
    // Look up the row in VS corresponding to the given term ID.
    val termRowVec: BDenseVector[Double] = new BDenseVector[Double](row(normalizedVS, termId).toArray)

    // Compute scores against every term
    val termScores: Array[(Double, Int)] = (normalizedVS * termRowVec).toArray.zipWithIndex

    // Find the terms with the highest scores
    termScores.sortBy(-_._1).take(20)
  }

  def printRelevantTerms(term: String, normalizedVS: BDenseMatrix[Double], idTerms: Map[String, Int], termIds: Map[Int, String]) {
    try {
      val id: Int = idTerms(term)
      printIdWeights[Int](topTermsForTerm(normalizedVS, id), termIds)
    } catch {
      case ex: Exception => log.error("Exception term not found {}", ex)
      println("Term not found. Enter another term")
    }
  }
}