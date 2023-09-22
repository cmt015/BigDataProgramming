import pyspark
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import sys

#Get number of clusters
k = sys.argv[-1]
#If number of clusters is not listed, default is 2.
if not isinstance(k, int):
    k = 2

conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession \
    .builder \
    .appName("Tweets Count by State") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#Load Dataset
dataset = spark.read.format("libsvm").load("/user/hadoop/data/kmeans_input.txt")

dataset.show()

#Create kmeans model
kmeans = KMeans().setK(k).setSeed(1)
model = kmeans.fit(dataset)

# Make predictions
predictions = model.transform(dataset)

predictions.show()

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()

silhouette = evaluator.evaluate(predictions)
print("Silhouette with squared euclidean distance = " + str(silhouette))

# Shows the result.
centers = model.clusterCenters()
print("Cluster Centers: ")
for center in centers:
    print(center)

