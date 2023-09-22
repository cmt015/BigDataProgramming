# BigDataProgramming
Contains code written for assignments for Big Data Programming, designed to be implemented for hadoop and spark clusters with 1 master and 2 slaves. 

* ComputePi - Compute the value of Pi using the Monte Carlo method (I.e., populate a square quadrant with random points. Determine if the points are within a circle centered around the origin with the radius as the length of the quadrant. The ratio of the points within the circle over the total points will be equal to pi/4.)  
    Input: the number of random points to be generated and the output directory path.  
    Ouput: the calculated value of pi, found in the given output directory.  
* PageIterative - Compute PageRank values of a graph using Power Iteration  
    Input: file containing initial PageRank values separated by newline, file containing adjacency list separated by newline, the output directory path, and the number of iterations (see 01InitialPRValues.txt and 02AdjacencyList.txt for examples of valid input files.)  
    Output: the updated PageRank value of each iteration  
* PageRankMC - Compute Page Rank values of a graph using the Monte Carlo method  
    Input: file containing adjacency list separated by newline, the output directory path, the number of simulations, and the number of nodes (see 02AdjacencyList.txt for example of valid input file.)  
    Output: the final updated PageRank value  


