package edu.cmu.scs.cc.project1;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class HashtagCountMapReduceTest {

	private Mapper<Object, Text, Text, VIntWritable> mapper;
	private Reducer<Text, VIntWritable, Text, VIntWritable> reducer;
	private MapReduceDriver<Object, Text, Text, VIntWritable, Text, VIntWritable> driver;

	/**
	 * Setup the mapper for word count.
	 */
	@Before
	public void setUp() {
		mapper = new HashtagCountMapper();
		reducer = new HashtagCountReducer();
		driver = new MapReduceDriver<>(mapper, reducer);
	}

	@Test
	public void testWordCountMapReduce() throws IOException {
		/*
		driver.withInput(new Text(""),
				new Text("bar hey whatever literally randomly typing whatever"))
				.withOutput(new Text("bar"), new VIntWritable(1))
				.withOutput(new Text("hey"), new VIntWritable(1))
				.withOutput(new Text("whatever"), new VIntWritable(2))
				.withOutput(new Text("literally"), new VIntWritable(1))
				.withOutput(new Text("typing"), new VIntWritable(1))
				.withOutput(new Text("randomly"), new VIntWritable(1))
				.runTest(false);
		*/
	}
}
