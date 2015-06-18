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

def wordCount(s):
	words = s.split(" ")
	return len(words)

def removeComments(s): # for removing comments
	lines = s.splitlines()
	#for line in lines:
	return len(lines)
	
# def getKeywords():
# 	return keyword.kwlist

def removeSpecialChar(inp):
	return re.sub('[^a-zA-Z\n\#]', ' ',inp)

def removeComment(inp):
	tmp = re.sub('(/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/)|(//.*)', ' ',inp)
	out1 = removeSpecialChar(tmp)
	out2 = removeReserveWords(out1, keywords)
	out2.sort()
	# out3 = [(key,len(list(group))) for key, group in groupby(out2)]
	return out2

def wordHashCount(inp):
	hashingTF = HashingTF(V)
	tf = hashingTF.transform(inp)
	return tf

def wordHashPair(inp):
	out3 = [(key,len(list(group))) for key, group in groupby(inp)]
	return out3	

def tfidfComputation(inp):
	idf = IDF().fit(inp)
	tfidf = idf.transform(inp)
	return tfidf


def removeReserveWords(inp, keyword):
	output = []
	words = inp.split()
	for word in words:
		if word not in keyword:
			output.append(word)
	return output
		
def stemming(inp):
	output = []
	for word in inp:
		stemmedWord = stem(word)
		output.append(stemmedWord)
	return output		

def vocab(inp):
	return [item for sublist in inp for item in sublist]
	
def calculateTFIDF(inp):
	out = [0] * len(vocab)
	IDF = idf.value
	for value in inp:
		if value[0] in vocab :
			out[vocab.index(value[0])] = value[1]*IDF[value[0]]
	return out

def inverseDocumentFreq(documentFreq):
	temp = (D+1.0)/(documentFreq+1.0)
	return 	math.log(temp)

# reserveWords = getKeywords(); 	

sc = SparkContext("local", "LSI App")
distFile = sc.wholeTextFiles("sampleJava").cache()
distFile.persist()
withoutComments = distFile.mapValues(removeComment)
wordHashPairs = withoutComments.flatMapValues(wordHashPair)
tf = withoutComments.mapValues(wordHashPair)
# print wordHashPairs.collect()
wordCountTuplesOnCorpus = wordHashPairs.values()
docFreq = wordCountTuplesOnCorpus.mapValues(lambda x: x/x).reduceByKey(lambda x,y: x+y)
D = tf.count()
inverseDocFreq = docFreq.mapValues(inverseDocumentFreq).sortByKey().collectAsMap()
idf = sc.broadcast(inverseDocFreq)
print wordCountTuplesOnCorpus.sortByKey().collect()
print docFreq.sortByKey().collect()
print inverseDocFreq
vocab = wordCountTuplesOnCorpus.groupByKey().sortByKey().keys().collect()
V = len(vocab)
print V
print vocab
print tf.collect()
print D
tfidfVectors = tf.mapValues(calculateTFIDF)
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
