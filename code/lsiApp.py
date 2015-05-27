"""lsiApp.py"""
from pyspark import SparkContext

def wordCount(s):
	words = s.split(" ")
	return len(words)

def removeComments(s):
	lines = s.splitlines()
	print lines
	return len(lines)
	

sc = SparkContext("local", "LSI App")
distFile = sc.wholeTextFiles("samplePy").cache()
distFile.persist()
fileCount = distFile.count()
firstTuple =  distFile.first()
fileNames = distFile.keys()
fileContent = distFile.values()
fileLength = distFile.mapValues(lambda s: len(s))
countWords = distFile.mapValues(wordCount)
countLines = distFile.mapValues(removeComments)
lp = countWords.lookup('/home/jatina/Downloads/spark-1.3.1/samplePy/parquet_inputformat.py')
distFileMap = countWords.collectAsMap()
print fileCount
print firstTuple
print fileNames.first()
print fileContent.first()
print fileLength.first()
print countWords.first()
print lp
print distFileMap
