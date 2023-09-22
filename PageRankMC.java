//package Assignments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.lang.Math;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankMC {
	public static class PageRankMCMapper extends Mapper<Text, Text, IntWritable, IntWritable>{
		private Integer targetNode = 0;
        private Integer finalNode = 0;
    	private IntWritable nFinalNode = new IntWritable();
    	private IntWritable one = new IntWritable(1);
		
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			// You need to complete this function.
			StringTokenizer line = new StringTokenizer(key.toString(), "\n"); //Break values into tokens by line
        	while(line.hasMoreTokens()){ //for each line
                StringTokenizer itr = new StringTokenizer(line.nextToken());
                targetNode = Integer.parseInt(itr.nextToken());
                finalNode = Integer.parseInt(itr.nextToken());              
                if(finalNode == targetNode){
                    nFinalNode.set(targetNode);
                    context.write(nFinalNode, one);
                }
			}
		}
	}
	public static class PageRankMCCombiner extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
		private IntWritable subsetSum = new IntWritable();
				
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
			for(IntWritable val : values){
        		sum += val.get();
      		}
      		subsetSum.set(sum);
      		context.write(key, subsetSum); 
		}
	}
	public static class PageRankMCReducer extends Reducer<IntWritable,IntWritable,IntWritable,DoubleWritable> {
		private DoubleWritable PRValue = new DoubleWritable();
        private int numOfSimulations = 0;
		// The PageRank Values of all the nodes; the PageRank vector
		
		@Override
		protected void setup(Reducer<IntWritable,IntWritable,IntWritable,DoubleWritable>.Context context) throws IOException, InterruptedException {
			this.numOfSimulations = context.getConfiguration().getInt("pr.num.simulations", -1);
			super.setup(context);
		}
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			// You need to complete this function.
      		int sum = 0;
      		for(IntWritable val : values){
        		sum += val.get();
      		}
      		PRValue.set(((double)(sum)) / ((double)(numOfSimulations)));
      		context.write(key, PRValue);//<j, updated pagerank of j>
		}
	}
    
    public static class FakeInputSplit extends InputSplit implements Writable {
        public void readFields(DataInput arg0) throws IOException {

        }
        public void write(DataOutput arg0) throws IOException {

        }
        public long getLength() throws IOException, InterruptedException {
            return 0;
        }
        public String[] getLocations() throws IOException, InterruptedException {
            return new String[0];
        }
    }

    public static class PRInputFormat extends InputFormat<Text, Text> {
        public static final String NUM_SIMULATIONS = "pr.num.simulations";
        public static final String NUM_MAP_TASKS = "pr.map.tasks";
        public static final String NUM_NODES = "pr.num.nodes";
        public static final String NUM_RECORDS_PER_NODE = "pr.num.rec.per.node";
        private int nSimulations = 0;
        
        public List<InputSplit> getSplits(JobContext job) throws IOException {
            // Get the number of map tasks configured for
            int numSplits = job.getConfiguration().getInt(NUM_MAP_TASKS, -1);
            // Create a number of input splits equivalent to the number of tasks
            ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
            for (int i = 0; i < numSplits; ++i) {
                splits.add(new FakeInputSplit());
            }
            return splits;
        }

        public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
            PRRecordReader rr = new PRRecordReader();
            rr.initialize(split, context);
            return rr;
        }

        public static void setNumSimulations(Job job, int i) {
            job.getConfiguration().setInt(NUM_SIMULATIONS, i);
        }

        public static void setNumMapTasks(Job job, int i) {
            job.getConfiguration().setInt(NUM_MAP_TASKS, i);

        }
        public static void setNumNodes(Job job, int i) {
            job.getConfiguration().setInt(NUM_NODES, i);
        }

    }

    public static class PRRecordReader extends RecordReader<Text, Text> {
        private int numRecordsToCreate = 0;
        private int numOfSimulations = 0;
        private int numOfNodes = 0;
        private int createdRecords = 1;
        private Text key = new Text();
        private Text value = new Text();
        // The PageRank Values of all the nodes; the PageRank vector
		private Map<Integer, List<Integer>> adjacencyMap = new HashMap<Integer, List<Integer>>();

        private List<Double> probabilityA = new LinkedList<Double>();
		// The variables for this node and its out-neighbor nodes
        private Integer targetNode = 0;
        private Integer currentNode = 0;
        private Integer pathLength = 0;
        private Double totalProbability = 0.0;

        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            // Get the number of records to create from the configuration
            if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
				URI[] cacheFiles = context.getCacheFiles();
				String sCacheFileName = cacheFiles[0].toString();//Adjacency List
        		FileSystem aFileSystem = FileSystem.get(context.getConfiguration());
        		Path aPath = new Path(sCacheFileName);
            	BufferedReader br = new BufferedReader(new InputStreamReader(aFileSystem.open(aPath)));
        		String line;
        		while ((line = br.readLine()) != null) {
					// process the line.
                    Integer thisNode = 0;
        			String[] tokens = line.split(" ");
                    thisNode = Integer.parseInt(tokens[0]);
                    int numOfOutlinks = tokens.length;
                    List<Integer> outlinks = new LinkedList<Integer>();
                    adjacencyMap.put(thisNode, outlinks);
                    for (int i = 1; i < tokens.length; i++){
                        outlinks.add(Integer.parseInt(tokens[i]));
                    }
				}
			}
            //Set probability distribution of lengths of paths using equation t = (1-c)c^t
            int a = 0;
            double prob = (1-0.85);
            probabilityA.add(a, prob);
            a = 1;
            double newProb = prob;
            while(newProb > 0.01){
                newProb = (1-0.85)*Math.pow(0.85, a);
                prob += newProb;
                probabilityA.add(a, prob);
                a++;
            }
            this.totalProbability = prob;
            this.numOfSimulations = context.getConfiguration().getInt("pr.num.simulations", -1);
            this.numOfNodes = context.getConfiguration().getInt("pr.num.nodes", -1);
            this.numRecordsToCreate = this.numOfSimulations * this.numOfNodes;
        }

        public boolean nextKeyValue() throws IOException,InterruptedException {
            // If we still have records to create
	    Random random = new Random();
            if (createdRecords <= numRecordsToCreate) {
                Integer targetNode = (createdRecords / numOfSimulations) + 1;
		        //assign a random starting node
                currentNode = random.nextInt(adjacencyMap.size()) + 1;
                //Generate length of path with given distribution.
                double randProb = Math.random()*totalProbability; 
                int i = 0;
                while(i < probabilityA.size()){
                    if(randProb < probabilityA.get(i)){
                        pathLength = i;
                        i = probabilityA.size();
                    } else {
                        i++;
                    }
                }
                //Iterate through path
                int t = 0;
                while(t < pathLength){
                    //Generate index of random outlink of the current node
                    int randOutlink = random.nextInt(adjacencyMap.get(currentNode).size());
                    Integer tmpNode = currentNode;
                    //Set the current node to the random outlink
                    currentNode = adjacencyMap.get(tmpNode).get(randOutlink);
                    t++;
                }
                String k = targetNode.toString() + " " + currentNode.toString();
                key.set(k);
                ++createdRecords;
                return true;
            } else {
                return false;
            }
        }
	public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }
        public float getProgress() throws IOException, InterruptedException {
            return (float) createdRecords / (float) numRecordsToCreate;
        }
        public void close() throws IOException {
            // nothing to do here...
        }
}
	public static void main(String[] args) throws Exception {
		System.out.println("Main start");
		// args[0] the input file containing the adjacency list of the graph
	  	String sInputAdjacencyList = args[0];
	  	// args[1] Output path
	  	String outputDir = args[1];
	  	// args[2] number of simulations
	  	Integer nNumSimulations = Integer.parseInt(args[2]);
        // args[3] number of nodes
        Integer nNumOfNodes = Integer.parseInt(args[3]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PageRankMC");
        job.setNumReduceTasks(1);
        job.setJarByClass(PageRankMC.class);
        job.setMapperClass(PageRankMCMapper.class);
        job.setCombinerClass(PageRankMCCombiner.class);
        job.setReducerClass(PageRankMCReducer.class);
        job.setInputFormatClass(PRInputFormat.class);
        PRInputFormat.setNumSimulations(job, nNumSimulations);
	PRInputFormat.setNumMapTasks(job, 1);
        PRInputFormat.setNumNodes(job, nNumOfNodes);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.addCacheFile(new Path(sInputAdjacencyList).toUri());
        FileOutputFormat.setOutputPath(job, new Path(outputDir));
        System.exit(job.waitForCompletion(true) ? 0 : 2);
		}
	}

