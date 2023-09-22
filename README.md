# BigDataProgramming
Contains code written for assignments for Big Data Programming, designed to be implemented for hadoop and spark clusters. 

Spark Branch:
* PageRank_Spark - Compute the PageRank values using the Power Iteration method using lambda functions, assumes 30 iterations  
    Input: file containing adjacency list (see 02AdjacencyList.txt for example of valid input file.)  
    Output: PageRankValues and txt file containing PageRank values  
* count_tweets_by_state - Given two json files (1) contains cities and corresponding states and (2) contains tweets and corresponding cities. Using Spark dataframes, find the number of tweets from each state.  
    Input: None. Code assumes location of filepaths. Example files are tweets.json and cityStateMap.json  
    Output: shows updated dataframe of number of tweets from each state  
* count_tweets_by_state_sql - Same as count_tweets_by_state but uses sql statements instead of dataframes.
* KMeans_Spark.py - Implement the kMeans algorithm in Spark  
    Input: number of clusters (optional, default is 2). Code assumes location of filepath of kmeans input file in libsvm format. Example is kmeans_input.txt  
    Output: prints the squared euclidean distance and the cluster centers.   
