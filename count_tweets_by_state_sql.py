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
DF1.createOrReplaceTempView("CITYSTATE")

DF2 = spark.read.json("/user/hadoop/data/tweets.json")
DF2.createOrReplaceTempView("TWEETS")

DF3 = spark.sql(""" SELECT C.state, count(*) as count \ 
    FROM CITYSTATE C, TWEETS T \
    WHERE T.geo = C.city GROUP BY C.state""")

DF2.show()
DF1.show()
DF3.show()
