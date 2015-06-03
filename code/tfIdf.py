from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF

sc = SparkContext()

# Load documents (one per line).
documents = "abc bcd cde abc cde def cde".split(" ")
doc = documents
# print doc
#print documents

hashingTF = HashingTF()
tf = hashingTF.transform(documents)
tF = tf
# tf.cache()
# idf = IDF().fit(tf)
# tfidf = idf.transform(tf).collect()
print tF
print len(tF)
print doc
print hashingTF.indexOf('bcd')
