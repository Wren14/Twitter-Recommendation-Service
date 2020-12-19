package edu.cmu.scs.cc.project1;

import java.io.IOException;
import org.junit.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mrunit.mapreduce.*;
import java.util.*;

public class HashtagCountReduceTest {

	private Reducer<Text, VIntWritable, Text, VIntWritable> reducer;
	private ReduceDriver<Text, VIntWritable, Text, VIntWritable> driver;

	/**
	 * Setup the reducer for word count.
	 */
	@Before
	public void setUp() {
		reducer = new HashtagCountReducer();
		driver = new ReduceDriver<>(reducer);
	}

	/**
	 * {@code ReduceDriver.runTest(false)}: the order does not matter.
	 *
	 * @throws IOException if io exception occurs
	 */
	@Test
	public void testWordCountReducer() throws IOException {
	
		String userId = "1416350234";
		
		String inputKey1 = String.format("%s,%s", userId, "metoo");
		String inputKey2 = String.format("%s,%s", userId, "cmu");
		String inputKey3 = String.format("%s,%s", userId, "cc");
		
		String outputKey1 = inputKey1;
		String outputKey2 = inputKey2;
		String outputKey3 = inputKey3;
		
		driver.withInput(new Text(inputKey1), Arrays.asList(new VIntWritable(1) ))
				.withInput(new Text(inputKey2), Arrays.asList(new VIntWritable(1), new VIntWritable(1) ) )
				.withInput(new Text(inputKey3), Arrays.asList(new VIntWritable(1) ) )
				.withOutput(new Text(inputKey1), new VIntWritable(1) )
				.withOutput(new Text(inputKey2), new VIntWritable(2) )
				.withOutput(new Text(inputKey3), new VIntWritable(1) )

			.runTest(false);

		
	}
}
