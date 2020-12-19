package edu.cmu.scs.cc.q3ValidTweets;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * MapReduce job workflow in hadoop for Twitter data analysis to:
 * 1. Filter out tweets that are in not applicable languages, are duplicate or are malformed.
 * 2. Process only certain fields of the tweet class and output only them in csv
 *
 * <p>
 * 1. Set the key/value class for the map output data.
 * 2. Set the key/value class for the job output data.
 * 3. Set the Mapper and Reducer class for the job.
 * 4. Set the number of Reducer tasks.
 * 5. Submit the job to the cluster and wait for it to finish.
 */
public class TwitterDataAnalysis 
{
	   /**
     * MapReduce job workflow for Twitter Data Analysis
     * @param args input args, where the 1st arg is the input path and
     *             the 2nd arg is the output path here.
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "twitter");
        
        job.setJarByClass(TwitterDataAnalysis.class);
        job.setMapperClass(TwitterMapper.class);
        job.setReducerClass(TwitterReducer.class);
        job.setNumReduceTasks(24);
        
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
