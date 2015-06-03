"""lsiApp.py"""
import keyword, re, math
from pyspark import SparkContext
from stemming.porter2 import stem
from itertools import groupby

keywords = ['abstract', 'continue', 'for', 'new', 'switch', 'assert', 'default', 'goto', 'package', 'synchronized',
 'boolean', 'do', 'if', 'private', 'this', 'break', 'double', 'implements', 'protected', 'throw', 'byte', 'else',
  'import', 'public', 'throws', 'case', 'enum', 'instanceof', 'return', 'transient', 'catch', 'extends', 'int',
   'short', 'try', 'char', 'final', 'interface', 'static', 'void', 'class', 'finally', 'long', 'strictfp', 'volatile',
    'const', 'float', 'native', 'super', 'while', 'true', 'false', 'null']

wordSet = []    

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

def wordCount(inp):
	out3 = [(key,len(list(group))) for key, group in groupby(inp)]
	return out3



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
	
def wordCountVector(inp):
	out = [0] * len(wordSet)
	for value in inp:
		if value[0] in wordSet :
			out[wordSet.index(value[0])] = value[1]
	return out

def inverseDocumentFreq(documentFreq,D):
	temp = (D+1.0)/(documentFreq+1.0)
	return 	math.log(temp)

# reserveWords = getKeywords(); 	

sc = SparkContext("local", "LSI App")
distFile = sc.wholeTextFiles("sampleJava").cache()
distFile.persist()
withoutComments = distFile.mapValues(removeComment)
# distFile.unpersist()
wordCounts = withoutComments.mapValues(wordCount)
wordCounts.persist()

#withoutSpecialChar = withoutComments.mapValues(removeSpecialChar)


# fileCount = distFile.count()
# firstTuple =  distFile.first()
# fileNames = distFile.keys().collect()
# fileContent = distFile.values()
# fileLength = distFile.mapValues(lambda s: len(s))
# countWords = distFile.mapValues(wordCount)
# countLines = distFile.mapValues(removeComments)
# lp = countWords.lookup('/home/jatina/Downloads/spark-1.3.1/samplePy/parquet_inputformat.py')
distFileMap = withoutComments.collectAsMap()
wordCountPairs = wordCounts.collectAsMap()
onlyValues = wordCounts.values().collect()
vocabulary = vocab(onlyValues)
vocabularyRDD = sc.parallelize(vocabulary)
wordList = vocabularyRDD.keys().collect()
wordList.sort()
wordSet = list(set(wordList))
wordSet.sort()
wordCountVectors = wordCounts.mapValues(wordCountVector)
vectors = wordCountVectors.collectAsMap()


# print fileCount
# print firstTuple
# #print fileNames.first()
# print fileContent.first()
# print fileLength.first()
# print countWords.first()
# print lp
# print distFileMap
print "Only word count pairs"
print wordCountPairs[u'/home/jatina/Downloads/spark-1.3.1/sampleJava/HelloWorld.java']
# print "Vocabulary"
# print vocabulary
# print type(vocabulary)
# print "word list"
# print wordList
print "wordset"
print wordSet
print "vectors"
tmp = vectors[u'/home/jatina/Downloads/spark-1.3.1/sampleJava/HelloWorld.java']
print tmp
vectorLen = len(wordSet)
for x in range(0, vectorLen):
	if tmp[x]:
		print wordSet[x]
# print vectors	
matrix = []
D = len(vectors)
print D
for key in vectors:
	matrix.append(vectors[key])

print matrix	
transposeMatrix = [list(x) for x in zip(*matrix)]
print transposeMatrix
documentFrequenies = []
for row in transposeMatrix:
	count = 0
	for ele in row:
		if ele:
			count = count + 1
	documentFrequenies.append(inverseDocumentFreq(count,D))

print documentFrequenies

tfidfMatrix = []

for row in matrix:
	newRow = []
	i=0
	for ele in row:
		newRow.append(ele*documentFrequenies[i])
		i = i+1
	tfidfMatrix.append(newRow)
print tfidfMatrix
print len(tfidfMatrix)		
# print reserveWords
# print fileNames