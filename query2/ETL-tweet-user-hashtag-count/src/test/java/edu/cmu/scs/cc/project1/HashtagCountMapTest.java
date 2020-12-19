package edu.cmu.scs.cc.project1;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import static edu.cmu.scs.cc.project1.HashtagCountMapper.UTF8;
import static edu.cmu.scs.cc.project1.HashtagCountMapper.LINE_SEPARATOR;

public class HashtagCountMapTest {

	private Mapper<Object, Text, Text, VIntWritable> mapper;
	private MapDriver<Object, Text, Text, VIntWritable> driver;

	@Before
	public void setUp() {
		//TODO - make this mapperand the mapper for users take text and convert it into utf8 strings
		//TODO - separate columns by tabs in reducer
		//TODO - hashtags are now separated by spaces
		mapper = new HashtagCountMapper();
		driver = new MapDriver<>(mapper);
	}

	/**
	 * {@code MapDriver.runTest(false)}: the order does not matter.
	 *
	 * @throws IOException if io exception occurs
	 */
	@Test
	public void testWordCountMapper() throws IOException {
		
		String createdAt = "1546300800";
		String text = "\"Hello, World!\"";
		String userId = "1416350234";
		String userScreenName = "Shoshaa_14";
		String userDescription = "\"I am cool!\"";
		String inReplyToUserId = "1416350235";
		
		String retweetUserId = "1138390430";
		String retweetUserScreenName = "tyousef63";
		String retweetUserDescription = "\"Yoyoyo\"";
		String hashtags = "metoo cmu cc cmu";
		
		String[] fields = new String[] {createdAt, text, userId, userScreenName, userDescription, inReplyToUserId,
										retweetUserId, retweetUserScreenName, retweetUserDescription, hashtags};
		
		StringJoiner inputValueJoiner = new StringJoiner("{TEAMLET'SDOIT!JOINER}");
		
		for (String field : fields) {
			inputValueJoiner.add(field);
		}
		
		String inputKey = "451953314234904576";
		
		String outputKey1 = String.format("%s\t%s\t%s\t%s\t%s%s", inputKey, userId, inReplyToUserId, retweetUserId, "metoo", LINE_SEPARATOR);
		String outputKey2 = String.format("%s\t%s\t%s\t%s\t%s%s", inputKey, userId, inReplyToUserId, retweetUserId,"cmu", LINE_SEPARATOR);
		String outputKey3 = String.format("%s\t%s\t%s\t%s\t%s%s", inputKey, userId, inReplyToUserId, retweetUserId,"cc", LINE_SEPARATOR);
		String outputKey4 = String.format("%s\t%s\t%s\t%s\t%s%s", inputKey, userId, inReplyToUserId, retweetUserId,"cmu", LINE_SEPARATOR);
		
		int outputValue1 = 1, outputValue2 = 1, outputValue3 = 1, outputValue4 = 1;
		
		BytesWritable inputValue = new BytesWritable(inputValueJoiner.toString().getBytes(UTF8));
		
		driver.withInput(new Text(""), new Text(inputKey + " " + inputValue.toString()) )
				.withOutput(new Text(outputKey1), new VIntWritable(outputValue1))
				.withOutput(new Text(outputKey2), new VIntWritable(outputValue2))
				.withOutput(new Text(outputKey3), new VIntWritable(outputValue3))
				.withOutput(new Text(outputKey4), new VIntWritable(outputValue4))
				.runTest(false);
		
	}
}
