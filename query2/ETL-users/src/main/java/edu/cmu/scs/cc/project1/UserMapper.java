package edu.cmu.scs.cc.project1;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.codec.binary.*;

/**
 * Mapper Utility for word count.
 */
public class UserMapper
		extends Mapper<Object, Text, Text, Text> {

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
	
	//static final String DELIMITER = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"; //",";
	static final String DELIMITER = "\\{TEAMLET'SDOIT!JOINER\\}";
	static final String JOINER = "{TEAMLET'SDOIT!JOINER}";
	
	private final Text outputValue = new Text();
	private final Text outputKey = new Text();

	/**
	 * Mapper for word count example.
	 *
	 * @param key input key of mapper
	 * @param value input value of mapper
	 * @param context output key/value pair of mapper
	 * @throws IOException if io exception occurs
	 * @throws InterruptedException if interrupted exception occurs
	 */
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {

		String input = null;
		
		try {
			input = convertToUTF8(value.toString(), null);
		} catch (DecoderException e) {
			e.printStackTrace();
			return;
		}
		
		String[] columns = input.split(DELIMITER, -5);
		
		if (columns.length != NUM_COLUMNS) {
			System.err.println(Arrays.toString(columns) );
			System.err.printf("There should be %d columns in value instead of %d\n",  NUM_COLUMNS, columns.length);
			return;
		}
		
		long timestamp = Long.parseLong(columns[CREATED_AT]);
		
		//There will always be an author of tweet - output this author's info
		outputKey.set(columns[USER_ID]);
		String userInfo = String.format("%s%s%s%s%d%s%b", columns[USER_SCREEN_NAME], JOINER, columns[USER_DESCRIPTION], JOINER,  timestamp, JOINER,  false);
		outputValue.set(userInfo);
		context.write(outputKey, outputValue);
		
		//If tweet was a retweet, the original tweet had an author - output the original tweet's author's info
		if (!columns[RETWEET_USER_ID].isEmpty() && !columns[RETWEET_USER_ID].equals("null")) {
			outputKey.set(columns[RETWEET_USER_ID]);
			userInfo = String.format("%s%s%s%s%d%s%b", columns[RETWEET_USER_SCREEN_NAME], JOINER, columns[RETWEET_USER_DESCRIPTION], JOINER, timestamp, JOINER, false);
			outputValue.set(userInfo);

			context.write(outputKey, outputValue);
		}
		
		//If tweet was a reply to someone, the original tweet had an author - output that original tweet's user ID (and empty description and screenname)
		if (!columns[IN_REPLY_TO_USER_ID].isEmpty() && !columns[IN_REPLY_TO_USER_ID].equals("null")) {
			outputKey.set(columns[IN_REPLY_TO_USER_ID]);
			userInfo = String.format("%s%s%s%s%d%s%b", "", JOINER, "", JOINER, timestamp, JOINER, true);
			outputValue.set(userInfo);

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