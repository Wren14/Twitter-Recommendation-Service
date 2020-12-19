package edu.cmu.scs.cc.project1;

import java.io.IOException;
import java.util.*;

import org.apache.commons.codec.DecoderException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import static edu.cmu.scs.cc.project1.UserMapper.UTF8;
import static edu.cmu.scs.cc.project1.UserMapper.JOINER;

public class UserMapTest {

	private Mapper<Object, Text, Text, Text> mapper;
	private MapDriver<Object, Text, Text, Text> driver;

	@Before
	public void setUp() {
		mapper = new UserMapper();
		driver = new MapDriver<>(mapper);
	}

	/**
	 * {@code MapDriver.runTest(false)}: the order does not matter.
	 *
	 * @throws IOException if io exception occurs
	 */
	@Test
	public void testWordCountMapper() throws IOException {
		
		String inputKey = "451953314234904576";
		
		String createdAt = "1546300800";
		String text = "\"Hello, World!\"";
		String userId = "1416350234";
		String userScreenName = "Shoshaa_14";
		String userDescription = "\"I am cool!\"";
		String inReplyToUserId = "1416350235";
		
		String retweetUserId = "1138390430";
		String retweetUserScreenName = "tyousef63";
		String retweetUserDescription = "\"Yoyoyo\"";
		String hashtags = "";
		
		String[] fields = new String[] {createdAt, text, userId, userScreenName, userDescription, inReplyToUserId,
										retweetUserId, retweetUserScreenName, retweetUserDescription, hashtags};
		
		StringJoiner inputValueJoiner = new StringJoiner(JOINER);
		for (String field : fields) {
			inputValueJoiner.add(field);
		}
		
		
		
		String outputKey1 = "1416350234";
		String outputKey2 = "1138390430";
		String outputKey3 = "1416350235";
		String timestamp = createdAt;//"1546300800";
		String outputValue1 = String.format("%s%s%s%s%s%s%b", userScreenName, JOINER, userDescription, JOINER, timestamp, JOINER, false);
		String outputValue2 = String.format("%s%s%s%s%s%s%b", retweetUserScreenName, JOINER, retweetUserDescription, JOINER, timestamp, JOINER, false);
		String outputValue3 = String.format("%s%s%s%s%s%s%b", "", JOINER, "", JOINER, timestamp, JOINER, true);
		
		BytesWritable inputValue = new BytesWritable( inputValueJoiner.toString().getBytes(UTF8));

		
		driver.withInput(new Text(inputKey), new Text(inputKey + " " + inputValue.toString()) )
				.withOutput(new Text(outputKey1), new Text(outputValue1))
				.withOutput(new Text(outputKey2), new Text(outputValue2))
				.withOutput(new Text(outputKey3), new Text(outputValue3))
				.runTest(false);
		
	}
}
