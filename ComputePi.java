//package Examples;

/*Arguments: {input file, output path, number of simulations}
* Sources:
* https://beginnersbook.com/2014/01/how-to-write-to-file-in-java-using-bufferedwriter/
* https://data-flair.training/blogs/how-hadoop-mapreduce-works/
* https://www.geeksforgeeks.org/estimating-value-pi-using-monte-carlo/
* https://javadeveloperzone.com/hadoop/java-read-write-files-hdfs-example/
* pages 184-188 of Mapreduce Design Patterns by Donald Miner & Adam Shook
*/


//import java.io.BufferedWriter;
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileWriter;
//import java.io.OutputStreamWriter;
//import java.io.FileReader;
//import java.io.InputStreamReader;
import java.io.IOException;
import java.util.StringTokenizer;
//import java.net.URI;
import java.lang.Math;
import java.util.ArrayList;
import java.util.List;
import java.io.DataInput;
import java.io.DataOutput;


import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ComputePi {
    public static class RadiusMapper extends Mapper<Text, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer line = new StringTokenizer(key.toString(), "\n"); //Break values into tokens by line
            		while(line.hasMoreTokens()){
                		StringTokenizer coord = new StringTokenizer(line.nextToken());//break each line into tokens
            			//StringTokenizer coord = new StringTokenizer(key.toString(), "\n"); //Break values into tokens by line
            			Double x_Double, y_Double, centerDistance = null;
            			x_Double = Double.parseDouble(coord.nextToken()); //get x coordinate
            			y_Double = Double.parseDouble(coord.nextToken()); //get y coordinate
            			centerDistance = x_Double*x_Double + y_Double * y_Double;
            			if(centerDistance <= 1){
                			word.set("Within Circle");
                			context.write(word, one);
				}
			}
		}
	}

	public static class CountCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class CountReducer
	extends Reducer<Text,IntWritable,Text,DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();
        private Text piText = new Text();
        private int nSimulations;

        @Override
        protected void setup(Reducer<Text,IntWritable,Text,DoubleWritable>.Context context) throws IOException, InterruptedException {
            // get the number of simulations from configuration
            this.nSimulations = context.getConfiguration().getInt("pi.num.simulations", -1);
        }

		public void reduce(Text key, Iterable<IntWritable> values,	Context context) throws IOException, InterruptedException {
			int sum = 0;
            double pi = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
		String title = nSimulations + "simulations, pi = ";
            piText.set(title);
            pi = ((double)sum / nSimulations)*4.0;
			result.set(pi);
			context.write(piText, result);
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

    public static class PiInputFormat extends InputFormat<Text, Text> {
        public static final String NUM_SIMULATIONS = "pi.num.simulations";
        public static final String NUM_MAP_TASKS = "pi.map.tasks";
        public static final String NUM_RECORDS_PER_TASK = "pi.num.records.per.map.task";
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
            PiRecordReader rr = new PiRecordReader();
            rr.initialize(split, context);
            return rr;
        }

        public static void setNumSimulations(Job job, int i) {
            job.getConfiguration().setInt(NUM_SIMULATIONS, i);
            setNumMapTasks(job, i);
        }

        public static void setNumMapTasks(Job job, int nSimulations) {
            int numMapTasks = 2;
            if(nSimulations > 1000000)
                numMapTasks = 4;
            job.getConfiguration().setInt(NUM_MAP_TASKS, numMapTasks);
            setNumRecordPerTask(job, nSimulations, numMapTasks);

        }

        public static void setNumRecordPerTask(Job job, int nSimulations, int numMapTasks) {
            int numRecPerTask = nSimulations / numMapTasks;
            job.getConfiguration().setInt(NUM_RECORDS_PER_TASK, numRecPerTask);
        }
    }

    public static class PiRecordReader extends RecordReader<Text, Text> {
        private int numRecordsToCreate = 0;
        private int createdRecords = 0;
        private Text key = new Text();
        private Text value = new Text();

        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            // Get the number of records to create from the configuration
            this.numRecordsToCreate = context.getConfiguration().getInt("pi.num.records.per.map.task", -1);
        }

        public boolean nextKeyValue() throws IOException,InterruptedException {
            // If we still have records to create
            if (createdRecords < numRecordsToCreate) {
                // Generate random data
                Double x_value = Math.random();
                Double y_value = Math.random();
                // Create a string of text from the random coordinates
                String coord = x_value.toString() + " " + y_value.toString();
                key.set(coord);
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
        Configuration conf = new Configuration();
        //args[0] = nSimulations
        int numSimulations = Integer.parseInt(args[0]);
        //args[1] = output directory
        Path outputDir = new Path(args[1]);
        Job job = new Job(conf, "ComputePi");
        job.setNumReduceTasks(1);
        job.setJarByClass(ComputePi.class);
        job.setMapperClass(RadiusMapper.class);
	job.setCombinerClass(CountCombiner.class);
        job.setReducerClass(CountReducer.class);
        job.setInputFormatClass(PiInputFormat.class);
	PiInputFormat.setNumSimulations(job, numSimulations);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
        FileOutputFormat.setOutputPath(job, outputDir);

        System.exit(job.waitForCompletion(true) ? 0 : 2);

    }
}
