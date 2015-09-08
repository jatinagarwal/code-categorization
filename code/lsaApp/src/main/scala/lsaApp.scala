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
import scala.annotation.switch
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

    val documentFrequencyMinSize = if (args.length > 4) args(4).toInt else 10
    /* 'featureVectorPercent' is the percentage of terms with top document frequencies feature vector*/

    val documentFrequencyMaxSize = if (args.length > 5) args(5).toInt else 50
    /* 'featureVectorPercent' is the percentage of terms with top document frequencies feature vector*/

    val dataFiles: String = if (args.length > 6) args(6).toString else "Data/sampleJava"
    /* 'dataFiles' implies path of the directory where data resides */
  /******************************************** INITIALIZATION STARTS HERE******************************************/
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
  /********************************************** INITIALIZATION ENDS HERE*******************************************/

/**********************************************DATA PARSING STARTS HERE**********************************************/
    val zipStreamToText : RDD[(String, String)] = zipToText(codeData)
    val codeDataWithOutComments: RDD[(String, String)] = removeJavaComments(zipStreamToText)
    val identifiersForEachDocument: RDD[(String, List[String])] = extractIdentifierCountPairs(codeDataWithOutComments,reserveWords)
/*************************************************DATA PARSING ENDS HERE*********************************************/

/************************************************TERM FREQUENCIES STARTS HERE****************************************/
     /* Computing term-document frequencies for each document*/
    val termDocumentFrequencies: RDD[(String, Predef.Map[String, Int])] = identifiersForEachDocument.mapValues{inp =>
        val identifierCountPairs: Predef.Map[String, Int] = inp.groupBy(x => x).mapValues(_.size)
//        val docTotal = identifierCountPairs.values.sum + 0.0
//        (docTotal,identifierCountPairs)
        /* Grouping based on uniqueness and computing size of each group to obtain (identifier,count) pairs */
        identifierCountPairs
    }
    termDocumentFrequencies.persist(StorageLevel.MEMORY_AND_DISK)
    val numDocs: Long = termDocumentFrequencies.count() /*Computing number of documents*/
    /* RDD 'termDocumentFrequencies' is persisted in the memory as two or more transformation are performed on it*/

/***********************************************TERM FREQUENCIES ENDS HERE*******************************************/



  /*******************************************DOCUMENT FREQUENCIES STARTS HERE***************************************/
    /* In this step, document frequiencies for all the identifiers are calculated using flattening all identifier list
    across all documents. Firstly, sorted list of identifiers is grouped based on uniqueness inorder to calculate
    (identifier, count) pairs. Secondly, each (identifier, count) pair is mapped to  (identifier, 1). Now each
    (identifier, 1) pair is reduced based on key to compute (identifier, df) where 'df' is the document frequency of
     identifier across all the documents*/
    val documentFrequencies: RDD[(String, Int)] = termDocumentFrequencies.flatMapValues(inp =>
      inp).values.mapValues{inp => 1}.reduceByKey((x,y) => x+y)
    /* In above step document frequencies are calculated for the terms across all the documents*/
//     val docFreqs = documentFrequencies.collect().sortBy(- _._2)
    val docFreqs: RDD[(String, Int)] = documentFrequencies.filter{ case(identifier,count) =>
      count >= documentFrequencyMinSize && count <= documentFrequencyMaxSize
    }.sortBy(_._2)
//      map{case(id,c) =>
//      (-c,id)}.sortByKey().map{case(c,id) => (id,-c)}
    /* Filtering (identifier, df) pairs from 'documentFrequencies' based on optimization specified in paper*/
    //docFreqs.persist(StorageLevel.MEMORY_AND_DISK)
    //val numTerms: Long = docFreqs.count()
    /* RDD 'docFreqs' is persisted in the memory as two or more transformation are performed on it*/
    //var featureVectorSize:Int = (numTerms*featureVectorPercent).toInt
    //val featureVector: Array[(String, Int)] = docFreqs.take(featureVectorSize)
    //val idfs: RDD[(String, Double)] = inverseDocumentFrequencies(docFreqs, numDocs)
    /* Computing inverse document frequencies 'idfs' from document frequencies */
    //val idfsMap: Predef.Map[String, Double] = idfs.toMap
    //val bidfs: Broadcast[Predef.Map[String, Double]] = sc.broadcast(idfsMap)/* Broadcasting 'idfs' across nodes of cluster*/
    //val vocabulary: RDD[(String, Long)] = idfs.keys.zipWithIndex/* Collecting all the identifiers after filtering (identifier, df) pairs*/
    //val termList: Broadcast[Predef.Map[String, Int]] = sc.broadcast(vocabulary.collect())/* Broadcasting vocabulary across all nodes of clusters*/
    //val termIds: RDD[(Long, String)] = vocabulary.map(_.swap)/* Terms are associated with the ids as shown (id, term) */
    //docFreqs.unpersist()
/*******************************************DOCUMENT FREQUENCIES ENDS HERE*********************************************/


/**********************************************TFIDF COMPUTATION STARTS HERE*******************************************/
    /* Computing tfidf from term frequencies and Inverse document frequencies */
//    val tfidf: RDD[(String, Vector)] = termDocumentFrequencies.mapValues{termFreqPair =>
//      val idf: Predef.Map[String, Double] = bidfs.value/* Locally obtaining broadcasted bidfs values */
//      val allIdentifiers: Predef.Map[String, Int] = termList.value/* Locally obtaining broadcasted  values */
//      val docTotalTerms: Int = termFreqPair.values.sum
//      val termInThisDocument: List[String] = termFreqPair.keySet.toList/* Obtaining all terms from this document*/
//      val sizeOfVector: Int = allIdentifiers.size/* Computing number of terms(identifiers) across all the documents*/
//      var tfidfMap:Map[Int,Double] = Map()/* Computing a map of (identifier, tfidf) pairs from term-document
//       (identifier, count) pairs and document-frequency (identifier, idfs) pair */
//      for(term <- termInThisDocument if allIdentifiers.contains(term)) {
//        tfidfMap += (allIdentifiers(term) -> termFreqPair(term)*idf(term)/docTotalTerms) /* TFIDF computation */
//      }
//      val tfidfSeq: Seq[(Int, Double)] = tfidfMap.toSeq/* Converting 'tfidfMap' map to a sequence */
//      Vectors.sparse(sizeOfVector, tfidfSeq) /*Obtaining sparse vector from 'tfidfSeq' sequence and 'sizeOfVector'*/
//    }

    val tfidf: RDD[((String, Long), Iterable[(String, Long, Double)])] = termDocumentFrequencies.mapValues{inp =>
      val docTotal = inp.values.sum + 0.0
      (docTotal,inp)
      /* Grouping based on uniqueness and computing size of each group to obtain (identifier,count) pairs */
    }.flatMap{
      case(k,v) =>
        v._2.map(a => (k,a._1,a._2,v._1))
    }.groupBy(_._2).flatMap(a => a._2.map(b => (b._1, b._2,b._3,b._4,a._2.size))).filter{
      case(a,b,c,d,e) => e >= documentFrequencyMinSize && e <= documentFrequencyMaxSize
    }.map(b => (b._1,b._2,b._3*math.log(numDocs.toDouble/b._5)/b._4)).groupBy(_._2).zipWithIndex.map{
      case(k,v) =>
        ((k._1,v),k._2.map(c => (c._1,v,c._3)))
    }

    tfidf.persist(StorageLevel.MEMORY_AND_DISK)
    tfidf.count()
    termDocumentFrequencies.unpersist()

    val vocabulary: RDD[(String, Long)] = tfidf.keys
    val termIds: RDD[(Long, String)] = vocabulary.map(_.swap)
    termIds.persist(StorageLevel.MEMORY_AND_DISK)
//    val tfi: RDD[(String, Long, Double)] = idf.flatMap{
//      case(k,v) =>
//        v
//    }
    val sizeOfVector: Long = vocabulary.count()
    val tfidfVector: RDD[(String, Vector)] = tfidf.flatMap{
      case(k,v) =>
        v
    }.groupBy(_._1).map{
      case(k,v) =>
        //val allIdentifiers: Predef.Map[String, Int] = termList.value
        //val sizeOfVector = allIdentifiers.size
        val values: Seq[(Int, Double)] = v.map(a => (a._2.toInt,a._3)).toSeq
        (k,Vectors.sparse(sizeOfVector.toInt, values))
    }
    tfidfVector.persist(StorageLevel.MEMORY_AND_DISK)
    /* RDD 'tfidfVector' is persisted in the memory as two or more operations are performed while computing row matrix*/
    val numDocss: Long = tfidfVector.count()
    /* Action is performed on tfidf vector in order to evaluate tfidf as it is needed in next step */
    val idDocs: RDD[(String, Long)] = tfidfVector.map(_._1).zipWithUniqueId()
    val docIds: RDD[(Long, String)] = idDocs.map(_.swap)
    docIds.persist(StorageLevel.MEMORY_AND_DISK)
    val terms: Long = termIds.count()
    val docs: Long = docIds.count()
    tfidf.unpersist()
    /* Documents names are associated to unique ids */
/**********************************************TFIDF COMPUTATION ENDS HERE*********************************************/


/**********************************************MATRIX COMPUTATION STARTS HERE******************************************/
    /* Constructing sparse matrix from tfidf sparse vectors obtained in the previous step*/
    val mat: RowMatrix = new RowMatrix(tfidfVector.values)
    val m: Long = mat.numRows /* Number of rows in a matrix */
    val n: Long = mat.numCols /* Number of columns in a matrix */
    /* Computing svd from the 'mat' to obtain matrices*/
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(k, computeU=true)

    /* Extracts top terms from top most concepts */
    val topConceptTerms: Seq[Seq[(String, Double)]] = topTermsInTopConcepts(svd, numTopConcepts, numTopTerms, termIds)
    /* Extracts top documents from top most concepts */
    val topConceptDocs: Seq[Seq[(String, Double)]] = topDocsInTopConcepts(svd, numTopConcepts, numTopDocs, docIds)

    val US: RowMatrix = multiplyByDiagonalMatrix(svd.U, svd.s)
    val normalizedUS: RowMatrix = rowsNormalized(US)

    val VS: BDenseMatrix[Double] = multiplyByDiagonalMatrix(svd.V, svd.s)
    val normalizedVS: BDenseMatrix[Double] = rowsNormalized(VS)
/**********************************************MATRIX COMPUTATION ENDS HERE********************************************/


/*******************************************CONSOLE PRINTING STARTS HERE***********************************************/
    println("Terms: "+terms+" Docs: "+docs)
    println("********************************Number of Documents: " +numDocs +" **************************************")
    println("********************************Number of Documentss: " +numDocss +" **************************************")
    println("**************************Size of Feature Vector: " + sizeOfVector +" *******************************")
    // println("***********************************Total terms ***************************: "+ documentFrequencies.count())
//    println("****************************Number of Terms after filtering: " +numTerms+" ***********************")
    println("*************************************** LIST OF WORDS ***************************************************")
    println("**************************************FILTERED DOCUMENT FREQUENCIES ************************************")
    docFreqs.collectAsMap().foreach(println)
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

//    breakable {
//      while(true) {
//        println("Enter one of the following options:")
//        println("new-doc : To find similar repos to a new repo")
//        println("doc-doc : To find most similar repos to a repo in trained dataset")
//        println("term-term : To find most similar terms to a term")
//        println("term-doc: To find most similar repos to a term")
//        println("terms-doc: To find most similar repos to a set of terms")
//        println("exit : To exit the application")
//        var i = readLine()
//        try {
//          val x = (i: @switch) match {
//              case "new-doc"  => {
//                println("Enter a path to a repo to find similar repos:")
//                val input = readLine()
//                println("Top documents for "+input+" are:")
//                topDocsForNewDoc(normalizedUS, svd.V, vocabulary, idfsMap, docIds, input.toString, numTopDocs, bidfs, termList, reserveWords)
//              }
//              case "doc-doc" => {
//                println("Enter a repo to find similar repos:")
//                val input = readLine()
//                println("Top documents for "+input+" are:")
//                printTopDocsForDoc(normalizedUS, input.toString, idDocs, docIds, numTopDocs)
//              }
//              case "term-term" => {
//                println("Enter a term to find similar terms:")
//                val input = readLine()
//                println("Top terms for "+input+" are:")
//                printRelevantTerms(input.toString, normalizedVS, vocabulary, termIds)
//              }
//              case "term-doc"  => {
//                println("Enter a terms to find repos containing it:")
//                val input = readLine()
//                println("Top documents contianing "+input+":")
//                printTopDocsForTerm(normalizedUS, svd.V, input.toString, vocabulary, docIds, numTopDocs)
//              }
//              case "terms-doc" => {
//                println("Enter set of terms to find repos containing it:")
//                val input = readLine()
//                println("Top documents contianing "+input+":")
//                val termSeq = input.toString.split(",").toSeq
//                printRelevantDocs(normalizedUS, svd.V, termSeq, vocabulary, idfsMap, docIds, numTopDocs)
//              }
//              case "exit" => break
//            }
//          }
//          catch {
//            case ex: Exception => log.error("Exception match not found {}", ex)
//            println("Match not found. Enter another option")
//          }
//        }
//      }
/*********************************************CONSOLE PRINTING ENDS HERE***********************************************/
  }

  def zipToText(codeData:RDD[(String, PortableDataStream)]): RDD[(String, String)] = {
    val zipStreamToText : RDD[(String, String)] = codeData.map{
      case(fileName, zipStream) =>
        val regexToRemovePath: Regex = """.*\/""".r
        val zipInputStream: DataInputStream = zipStream.open()
        val (fileContent,count) = ZipBasicParser.readFilesAndPackages(new ZipInputStream(zipInputStream))
        println(fileName +":"+count)
        zipInputStream.close()
        val repoName: String = regexToRemovePath.replaceAllIn(fileName,"")
        (repoName,fileContent)
    }
    zipStreamToText 
  }

  /* Method 'removeJavaComments' removes all comments and special characters from each '.java' file*/
  def removeJavaComments(zipStreamToText : RDD[(String, String)]) : RDD[(String, String)] = {
    val commentsRemoved: RDD[(String, String)] = zipStreamToText.mapValues{fileContent =>
      removeCommentsAndSpecilChars(fileContent)
    }
    commentsRemoved   
  }

  def removeCommentsAndSpecilChars(fileContent:String) : String = {
    val regexSpecialChars: Regex = """[^a-zA-Z\s]""".r
    /* 'regexSpecialchars' represents a pattern for special characters*/
    val reader:Reader = new StringReader(fileContent)
    val writer: StringWriter = new StringWriter()
    val jcr: JavaCommentsRemover = new JavaCommentsRemover(reader,writer)
    val codeWithOutComments: String = jcr.process()
    regexSpecialChars.replaceAllIn(codeWithOutComments, " ")/* 'commentsRemoved' represents content of a'.java' file
      without any comments and all occurences of 'regexSpecialChars' are replaced by " ", using 'replaceAllIn' method
      of scala's regex utility */
  }

  /* In step this cleaned text is splitted into list of words. And reserved words are removed from the list of words*/
  def extractIdentifierCountPairs(codeDataWithOutComments: RDD[(String, String)],reserveWords: Broadcast[List[String]]) 
  : RDD[(String, List[String])] = {
    val identifiersForEachDocument: RDD[(String, List[String])] = codeDataWithOutComments.mapValues{codeWithOutComments =>
      extractBagOfWords(codeWithOutComments,reserveWords)
    }
    identifiersForEachDocument
  }

  def extractBagOfWords(codeWithOutComments: String,reserveWords: Broadcast[List[String]]) : List[String] = {
    val listOfWords: List[String] = codeWithOutComments.split("\\s+").toList.filter(x => x != "") 
    /* Text 'inp' is splitted into list of words and stored in listOfWords'*/
    val allReserveWords: List[String] = reserveWords.value 
    /* Broadcasted listed of reserve words are accessed and stored in 'allReserveWords' */
    val removeReserveWords: ListBuffer[String] =  new ListBuffer[String]()/* 'removeReserveWords' is list buffer to strings */
    for(word <- listOfWords if !allReserveWords.contains(word)) convertToCamelCase(word).foreach(removeReserveWords+=_)
    /* words other than reserve words are appended to 'removeReserveWords'*/
    val bagOfWords:List[String] = removeReserveWords.toList.sorted
    /* 'bagOfWords' contains sorted list of identifiers from a '.java'*/
    bagOfWords
  }

  def convertToCamelCase(word:String) : List[String] = {
    val cc:CamelCase = new CamelCase()
    val words: List[String] = cc.splitCamelCase(word).split(" ").toList.map(_.toLowerCase)
    words
  }

  /* FUNCTION TO COMPUTE INVERSE DCOUMENT FREQUENCIES*/
  def inverseDocumentFrequencies(docFreqs: RDD[(String, Int)], numDocs: Long): RDD[(String, Double)]
  = {
    docFreqs.map{ case (term, count) => (term, math.log(numDocs.toDouble / count))}
  }

  /* FUNCTION TO COMPUTE TOP TERMS IN TOP CONCEPTS*/
  def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
      numTerms: Int, termIds: RDD[(Long, String)]): Seq[Seq[(String, Double)]] = {
    val v: Matrix = svd.V /* Matrix representing term space*/
    val topTerms: ArrayBuffer[Seq[(String, Double)]] = new ArrayBuffer[Seq[(String, Double)]]()
    val arr: Array[Double] = v.toArray
    for (i <- 0 until numConcepts) {
      val offs: Int = i * v.numRows
      val termWeights: Array[(Double, Int)] = arr.slice(offs, offs + v.numRows).zipWithIndex /* Picking each column of the matrix 'v'*/
      val sorted: Array[(Double, Int)] = termWeights.sortBy(-_._1)
      topTerms += sorted.take(numTerms).map{case (score, id) => (termIds.lookup(id.toLong)(0), score)} /* Associating scores with
      corresponding terms using termIds*/
    }
    topTerms
  }

  /* FUNCTION TO COMPUTE TOP DOCUMENTS IN TOP CONCEPTS*/
  def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
      numDocs: Int, docIds: RDD[(Long, String)]): Seq[Seq[(String, Double)]] = {
    val u: RowMatrix = svd.U /* Matrix representing document space*/
    val topDocs: ArrayBuffer[Seq[(String, Double)]] = new ArrayBuffer[Seq[(String, Double)]]()
    for (i <- 0 until numConcepts) {
      val docWeights: RDD[(Double, Long)] = u.rows.map(_.toArray(i)).zipWithUniqueId  /* Picking each row of the row matrix 'u'*/
      topDocs += docWeights.top(numDocs).map{case (score, id) => (docIds.lookup(id.toLong)(0), score)} /* Associating scores with
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

    val lines = removeCommentsAndSpecilChars(fileContent)

    val bagOfWords = extractBagOfWords(lines,reserveWords)

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