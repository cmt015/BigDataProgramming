import pyspark
from pyspark.context import SparkContext
from pyspark import SparkConf

conf = SparkConf()
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")


# Load the adjacency list file
AdjList1 = sc.textFile("/user/hadoop/data/02AdjacencyList.txt")
print(AdjList1.collect())

# 1. Split each line into a token
AdjList2 = AdjList1.map(lambda line : line.split("\n"))  
# 2. Key = node, value = list of out-links
AdjList3 = AdjList2.map(lambda x : (x[0].split(" ")[0], x[0].split(" ")[1:]))  
AdjList3.persist()
print(AdjList3.collect())

nNumOfNodes = AdjList3.count()
print("Total Number of nodes: " + str(nNumOfNodes))

# Initialize each page's rank; since we use mapValues, the resulting RDD will have the same partitioner as links
print("Initialization")
# 3. Initalize value of each node as being (1/n)
PageRankValues = AdjList3.mapValues(lambda v : 1/nNumOfNodes)  

# Run 30 iterations
print("Run 30 Iterations")
for i in range(1, 31):
    print("PageRankValues:")
    print(PageRankValues.collect())
    print("\nNumber of Iterations: "+ str(i))
    JoinRDD = AdjList3.join(PageRankValues) # (node, [ [j1, j2, ...], pagerank_of_node])
    print("Join Results:")
    print(JoinRDD.collect())
    # 4. x[0] is i, x[1][0] is the list of outlinks of i, and x[1][1] is the pagerank of i
    # So, we need to look at the list of outlinks of i, and for each outlink we need to get 
    # the product of c * page_rank_of_i * 1/number_outlinks_of_i.
    # Since we are iterating through the outlinks of i, this requires a nested lambda function.
    # The new key will be j, and the value will be c * 1/O(i) * prev_r(i)
    contributions = JoinRDD.flatMap(lambda x : map(lambda j: (j, x[1][1] * 0.85 * 1/len(x[1][0])), x[1][0]))
    print("Contributions:")
    print(contributions.collect())
    # 5. Here we are only summing the various values or c * 1/O(i) * prev_r(i)
    accumulations = contributions.reduceByKey(lambda x, y : x + y)  
    print("Accumulations:")
    print(accumulations.collect())
    # 6. Finally, we add (1-c)/n to each sum and sort the updated pagerank values
    PageRankValues = accumulations.mapValues(lambda v : v + (1-0.85)/nNumOfNodes).sortByKey()

print("=== Final PageRankValues ===")
print(PageRankValues.collect())

# Write out the final ranks
PageRankValues.coalesce(1).saveAsTextFile("/user/hadoop/PageRankValues_Final")
