package edu.cmu.scs.cc.project1;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import static edu.cmu.scs.cc.project1.UserMapper.JOINER;
import static edu.cmu.scs.cc.project1.UserReducer.LINE_SEPARATOR;

public class UserReduceTest {

	private Reducer<Text, Text, Text, Text> reducer;
	private ReduceDriver<Text, Text, Text, Text> driver;

	/**
	 * Setup the reducer for word count.
	 */
	@Before
	public void setUp() {
		reducer = new UserReducer();
		driver = new ReduceDriver<>(reducer);
	}

	/**
	 * {@code ReduceDriver.runTest(false)}: the order does not matter.
	 *
	 * @throws IOException if io exception occurs
	 */
	@Test
	public void testWordCountReducer() throws IOException {
	
		
		String inputKey1 = "451953314234904576";
		String newerTs = "1546300800";
		String userScreenName1 = "Shoshaa_14";
		String userDescription1 = "\"I am cool!\"";
		
		String olderTs = "1546300700";
		String userScreenName2 = "Old_screen_name";
		String userDescription2 = "\"Very old\"";
		
		
		String inputKey2 = "451953314234904588";
		
		String inputKey3 = "451953314234904577";
		String userScreenName3 = "Shoshaa_14";
		String userDescription3 = "\"I am cool!\"";
		
		driver.withInput(new Text(inputKey1), Arrays.asList(
												new Text(String.format("%s%s%s%s%s%s%b", userScreenName1, JOINER, userDescription1, JOINER, newerTs, JOINER, false)),
												new Text(String.format("%s%s%s%s%s%s%b", userScreenName2, JOINER, userDescription2, JOINER, olderTs, JOINER, false))
											)
						)
		
				.withInput(new Text(inputKey2), Arrays.asList(new Text(String.format("%s%s%s%s%s%s%b", "", JOINER,  "", JOINER, newerTs, JOINER, true))))
				
				.withInput(new Text(inputKey3), Arrays.asList(
						new Text(String.format("%s%s%s%s%s%s%b", userScreenName3, JOINER,  userDescription3, JOINER,  olderTs, JOINER,  false)),
						new Text(String.format("%s%s%s%s%s%s%b", "", JOINER,  "", JOINER,  newerTs, JOINER,  true))
											)
						)
				
				.withOutput(new Text(inputKey1), new Text(String.format("%s\t%s%s", userScreenName1, userDescription1, LINE_SEPARATOR)))
				.withOutput(new Text(inputKey2), new Text(String.format("%s\t%s%s", "", "", LINE_SEPARATOR)))
				.withOutput(new Text(inputKey3), new Text(String.format("%s\t%s%s", userScreenName3, userDescription3, LINE_SEPARATOR)))
				.runTest(false);
		
		
	}
}
