from pyspark import SparkContext
from pyspark.mllib.feature import Word2Vec

sc = SparkContext(appName='Word2Vec')
inp = sc.textFile("NOTICE").map(lambda row: row.split(" "))

word2vec = Word2Vec()
model = word2vec.fit(inp)
# mod = model.collectAsMap()
print type(model)

synonyms = model.findSynonyms('Apache', 5)

for word, cosine_distance in synonyms:
    print "{}: {}".format(word, cosine_distance)