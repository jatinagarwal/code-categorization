"""lsiApp.py"""
import keyword, re, math
from pyspark import SparkContext
from stemming.porter2 import stem
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from itertools import groupby

keywords = ['abstract', 'continue', 'for', 'new', 'switch', 'assert', 'default', 'goto', 'package', 'synchronized',
 'boolean', 'do', 'if', 'private', 'this', 'break', 'double', 'implements', 'protected', 'throw', 'byte', 'else',
  'import', 'public', 'throws', 'case', 'enum', 'instanceof', 'return', 'transient', 'catch', 'extends', 'int',
   'short', 'try', 'char', 'final', 'interface', 'static', 'void', 'class', 'finally', 'long', 'strictfp', 'volatile',
    'const', 'float', 'native', 'super', 'while', 'true', 'false', 'null']

vocab = []    

#To Count number of wordss in document
def wordCount(s):
	words = s.split(" ")

	return len(words)

# Method to remove special characters from a given text
def removeSpecialChar(inp):
	return re.sub('[^a-zA-Z\n\#]', ' ',inp)

# Method to remove all comments from any java program
def removeComments(inp):
	tmp = re.sub('(/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/)|(//.*)', ' ',inp)
	out1 = removeSpecialChar(tmp)
	out2 = removeReserveWords(out1, keywords)
	out2.sort()
	# out3 = [(key,len(list(group))) for key, group in groupby(out2)]
	return out2

# Method to calculate term frequency in a document using hashing
def wordHashCount(inp):
	hashingTF = HashingTF(V)
	tf = hashingTF.transform(inp)
	return tf

# Method to calculate (word, count) pair from a text
def computeWordCountPairs(inp):
	out3 = [(key,len(list(group))) for key, group in groupby(inp)]
	return out3	

# Method to calculate tfidf from term frequencies
def tfidfComputation(inp):
	idf = IDF().fit(inp)
	tfidf = idf.transform(inp)
	return tfidf

# Method to remove java reserve words from a java code
def removeReserveWords(inp, keyword):
	output = []
	words = inp.split()
	for word in words:
		if word not in keyword:
			output.append(word)
	return output
		
# Method to apply stemming on each word
def stemming(inp):
	output = []
	for word in inp:
		stemmedWord = stem(word)
		output.append(stemmedWord)
	return output		

# Method to calculate vocabulary(unique list of words across all documents of corpus) from (word,count) pairs
def vocab(inp):
	return [item for sublist in inp for item in sublist]
	
# Method to calculate TFIDF using IDF and returning vector representing a document
def calculateTFIDF(inp):
	out = [0] * len(vocab)
	IDF = idf.value
	for value in inp:
		if value[0] in vocab :
			out[vocab.index(value[0])] = value[1]*IDF[value[0]]
	return out

# Method to compute inverse document frequencies using document frequencies
def inverseDocumentFreq(documentFreq):
	temp = (D+1.0)/(documentFreq+1.0)
	return 	math.log(temp)


# Constructing spark context object for creating RDD, Broadcast variables and accumulators for application named 'LSI App'
sc = SparkContext("local", "LSI App")

# Creating RDD named 'distributedFiles' by reading java programs from a directory named 'sampleJava'. This RDD stores data 
# in (key, value) format where 'key' indicatemes 'documentName' and 'value' indicates 'documentContent'.
distributedFiles = sc.wholeTextFiles("sampleJava").cache()

# Persisting 'distributedFiles'
distributedFiles.persist()

# Removing comments from by applying 'removeComments' method on 'fileContent' of each file and returns a transformed RDD 
# 'codeWithoutComments' with following format ('documentName', 'documentContentWithoutComments')
codeWithoutComments = distributedFiles.mapValues(removeComments)

# Step to compute all (word, count) pairs across all the documents
wordCountTuplesOnCorpus = codeWithoutComments.flatMapValues(computeWordCountPairs)

# Step to extract (word,count) pairs from all the documents into 'wordsCountAcrossCorpus'
wordsCountAcrossCorpus = wordCountTuplesOnCorpus.values()

# Documents frequencies for each word are computed by aggregating same words across all the documents and sotred in 'docFreq'
docFreq = wordsCountAcrossCorpus.mapValues(lambda x: x/x).reduceByKey(lambda x,y: x+y)

# Step to compute term frequencies for each document and storing it in 'tf' in following format
# (documentName, list of (word, count) pairs)
tf = codeWithoutComments.mapValues(computeWordCountPairs)

# 'D' is total number of documents
D = tf.count()

# A map 'word:inverse_document_frequency' is computed using document frequencies 'docFreq' and 'D'
inverseDocFreq = docFreq.mapValues(inverseDocumentFreq).sortByKey().collectAsMap()

# 'inverseDocFreq' map is broadcasted across all workers 
idf = sc.broadcast(inverseDocFreq)

# TFIDF vector is computed for each docuement using 'tf' and 'idf'. And stored in 'tfidfVectors'
tfidfVectors = tf.mapValues(calculateTFIDF)


# Collecting and printing results at different stages
print wordsCountAcrossCorpus.sortByKey().collect()
print docFreq.sortByKey().collect()
print inverseDocFreq
vocab =wordsCountAcrossCorpus.groupByKey().sortByKey().keys().collect()
V = len(vocab)
print V
print vocab
print tf.collect()
print D
print tfidfVectors.collectAsMap()

# distFile.unpersist()
# wordHashCounts = withoutComments.mapValues(wordHashCount)
# wordHashCounts.persist()
# sparseVectors = wordHashCounts.values()
# sparseVectors.cache()
# sparseVectorsKeys = wordHashCounts.keys()
# tfidf = sparseVectors.mapValues(tfidfComputation)
# idf = IDF().fit(sparseVectors)
# tfidf = idf.transform(sparseVectors)

#withoutSpecialChar = withoutComments.mapValues(removeSpecialChar)


# fileCount = distFile.count()
# firstTuple =  distFile.first()
# fileNames = distFile.keys().collect()
# fileContent = distFile.values()
# fileLength = distFile.mapValues(lambda s: len(s))
# countWords = distFile.mapValues(wordCount)
# countLines = distFile.mapValues(removeComments)
# lp = countWords.lookup('/home/jatina/Downloads/spark-1.3.1/samplePy/parquet_inputformat.py')
# distFileMap = withoutComments.collectAsMap()
# wordHashCountPairs = wordHashCounts.collectAsMap()

# idfCollect = idf.collect()

# tfidfCollection = tfidf.collect()
# D = len(tfidfCollection)
# print tfidfCollection
# print D
# for element in tfidfCollection:
# 	print element

# print wordHashCountPairs
# wordHashCounts.unpersist()

# print distFileMap
# print len(tfidfCollection)

# print vocab
# print V
# vocabulary = vocab(wordCountTuplesOnCorpus)
# vocabularyRDD = sc.parallelize(vocabulary)
# wordList = vocabularyRDD.keys().collect()
# wordList.sort()
# wordSet = list(set(wordList))
# wordSet.sort()



# print fileCount
# print firstTuple
# #print fileNames.first()
# print fileContent.first()
# print fileLength.first()
# print countWords.first()
# print lp
# print distFileMap
# print "Vocabulary"
# print vocabulary
# print type(vocabulary)
# print "word list"
# print wordList
# print reserveWords
# print fileNames


# print "Only word count pairs"
# print wordCountPairs[u'/home/jatina/Downloads/spark-1.3.1/sampleJava/HelloWorld.java']
# print "wordset"
# print wordSet
# print "vectors"
# tmp = vectors[u'/home/jatina/Downloads/spark-1.3.1/sampleJava/HelloWorld.java']
# print tmp
# vectorLen = len(wordSet)
# for x in range(0, vectorLen):
# 	if tmp[x]:
# 		print wordSet[x]
# # print vectors	
# matrix = []
# D = len(vectors)
# print D
# for key in vectors:
# 	matrix.append(vectors[key])

# print matrix	
# transposeMatrix = [list(x) for x in zip(*matrix)]
# print transposeMatrix
# documentFrequenies = []
# for row in transposeMatrix:
# 	count = 0
# 	for ele in row:
# 		if ele:
# 			count = count + 1
# 	documentFrequenies.append(inverseDocumentFreq(count,D))

# print documentFrequenies

# tfidfMatrix = []

# for row in matrix:
# 	newRow = []
# 	i=0
# 	for ele in row:
# 		newRow.append(ele*documentFrequenies[i])
# 		i = i+1
# 	tfidfMatrix.append(newRow)
# print tfidfMatrix
# print len(tfidfMatrix)		
