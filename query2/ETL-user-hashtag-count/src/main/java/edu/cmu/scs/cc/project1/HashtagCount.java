package edu.cmu.scs.cc.project1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HashtagCount {

	/**
	 * MapReduce job workflow for word count.
	 * @param args input args, where the 1st arg is the input path and
	 *			 the 2nd arg is the output path here.
	 * @throws Exception if exception occurs
	 */
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "user");
		job.setJarByClass(HashtagCount.class);
		job.setMapperClass(HashtagCountMapper.class);
		job.setReducerClass(HashtagCountReducer.class);
		//job.setCombinerClass(UserReducer.class);
		job.setNumReduceTasks(5);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(VIntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(VIntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
