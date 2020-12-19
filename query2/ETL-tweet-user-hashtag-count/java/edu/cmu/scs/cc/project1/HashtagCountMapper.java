package edu.cmu.scs.cc.project1;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.StringTokenizer;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper Utility for word count.
 */
public class HashtagCountMapper
		extends Mapper<Object, Text, Text, VIntWritable> {

	final static Charset UTF8 = Charset.forName("UTF-8");
	
	private final static int CREATED_AT = 0;
	private final static int TEXT = 1;
	private final static int USER_ID = 2;
	private final static int USER_SCREEN_NAME = 3;
	private final static int USER_DESCRIPTION = 4;
	private final static int IN_REPLY_TO_USER_ID = 5;
	private final static int RETWEET_USER_ID = 6;
	private final static int RETWEET_USER_SCREEN_NAME = 7;
	private final static int RETWEET_USER_DESCRIPTION = 8;
	private final static int HASHTAGS = 9;
	
	private final static int NUM_COLUMNS = 10;
	
	final static String LINE_SEPARATOR = "TEAMLETSDOITEOLJEYRAJWRE";
	
	//static final String DELIMITER = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
	static final String DELIMITER = "\\{TEAMLET'SDOIT!JOINER\\}";
	
	private final VIntWritable outputValue = new VIntWritable();
	private final Text outputKey = new Text();

	/**
	 * Mapper for word count example.
	 * output key - <user,hashtag>
	 * output value - hashtag count
	 * @param key input key of mapper
	 * @param value input value of mapper
	 * @param context output key/value pair of mapper
	 * @throws IOException if io exception occurs
	 * @throws InterruptedException if interrupted exception occurs
	 */
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {

		String input = null;
		
		StringBuilder tweetId = new StringBuilder();
		
		try {
			input = convertToUTF8(value.toString(), tweetId);
		} catch (DecoderException e) {
			e.printStackTrace();
			return;
		}
		
		String[] columns = input.split(DELIMITER, -5);
		

		
		if (columns.length != NUM_COLUMNS) {
			System.err.println(Arrays.toString(columns));
			System.err.printf("There should be %d columns in value instead of %d\n",  NUM_COLUMNS, columns.length);
			return;
		}
		
		//Ignore non-contact tweets
		if (columns[IN_REPLY_TO_USER_ID].equals("null") && columns[RETWEET_USER_ID].equals("null") )
			return;
				
		String []hashtags = columns[HASHTAGS].split("\\s+");
		
		for (String hashtag : hashtags) {
			outputKey.set(String.format("%s\t%s\t%s\t%s\t%s%s", tweetId.toString(), columns[USER_ID], columns[IN_REPLY_TO_USER_ID], columns[RETWEET_USER_ID], hashtag, LINE_SEPARATOR));
			outputValue.set(1);
			//System.err.printf("<%s,%s,%s>\t%d\n", key.toString(), columns[USER_ID], hashtag, 1);
			context.write(outputKey, outputValue);
		}
		
		
	}
	
	public static String convertToUTF8(String input, StringBuilder tweetId) throws  DecoderException {

		String []split = input.split("\\s+", 2);
		
		if (tweetId != null)
			tweetId.append(split[0]);
		
		String value = split[1];
		value = value.replace(" ", "");
		
		byte[] byteArray = Hex.decodeHex(value);
		
		String output = new String(byteArray, UTF8);
		
		return output;
		
	}
}