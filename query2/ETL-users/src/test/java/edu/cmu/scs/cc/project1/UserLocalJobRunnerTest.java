package edu.cmu.scs.cc.project1;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Test;

public class UserLocalJobRunnerTest {

	@Test
	public void run() throws Exception {

		Configuration conf = new Configuration();

		// use LocalJobRunner
		conf.set("mapred.job.tracker", "local");
		// set the filesystem to be local
		conf.set("fs.default.name", "file:///");
		// read the input from the local folder "input"
		Path inputPath = new Path("input");
		// write the output to the local file "output"
		Path outputPath = new Path("output");

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		// run the local MapReduce job
		Job job = runJob(conf, inputPath, outputPath);
		// assert the job is successful
		assertTrue(job.isSuccessful());
		// (optional) you may read and verify the job output
	}

	/**
	 * Local job runner for word count MapReduce workflow.
	 * @param conf hadoop configuration
	 * @param inputPath input path of local job runner
	 * @param outputPath output path of local job runner
	 * @return MapReduce job
	 * @throws ClassNotFoundException if class-not-found exception occurs
	 * @throws IOException if io exception occurs
	 * @throws InterruptedException if interrupted exception occurs
	 */
	public Job runJob(Configuration conf, Path inputPath, Path outputPath)
			throws ClassNotFoundException, IOException, InterruptedException {
		Job job = Job.getInstance(conf, "user");

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(UserMapper.class);
		job.setReducerClass(UserReducer.class);

		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(false);
		return job;
	}
}