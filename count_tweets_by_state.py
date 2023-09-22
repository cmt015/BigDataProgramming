import pyspark
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import *
import json
import os

conf = SparkConf()
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

spark = SparkSession \
    .builder \
    .appName("Tweets Count by State") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

DF1 = spark.read.json("/user/hadoop/data/cityStateMap.json")

DF2 = spark.read.json("/user/hadoop/data/tweets.json")

DF3 = DF2.join(DF1, DF1.city == DF2.geo).drop(DF1.city)

DF4 = DF3.groupBy('state').count()


DF2.show()
DF1.show()
DF3.show()
DF4.show()
