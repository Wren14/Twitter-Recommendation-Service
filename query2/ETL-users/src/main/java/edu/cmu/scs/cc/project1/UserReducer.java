package edu.cmu.scs.cc.project1;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import static edu.cmu.scs.cc.project1.UserMapper.DELIMITER;



public class UserReducer
		extends Reducer<Text, Text, Text, Text> {
	
	private final static int USER_SCREEN_NAME = 0;
	private final static int USER_DESCRIPTION = 1;
	private final static int CREATED_AT = 2;
	private final static int IS_REPLY = 3;
	
	private final static int NUM_COLUMNS = 4;
	
	private final Text outputValue = new Text();
	
	final static String LINE_SEPARATOR = "TEAMLETSDOITEOLJEYRAJWRE";
	
	/**
	 * Reduce function for word count example.
	 *
	 * @param key input key of reducer
	 * @param values input values of reduce which is iterable
	 * @param context output key/value pair of reducer
	 * @throws IOException if io exception occurs
	 * @throws InterruptedException if interrupted exception occurs
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String mostRecentUserInfo = null;
		long mostRecentUserInfoTs = Long.MIN_VALUE;
		
		boolean isEverRepliedTo = false;
		
		//Get most recent info about a user
		for (Text userInfo : values) {
			String[] columns = userInfo.toString().split(DELIMITER);
			
			if (columns.length != NUM_COLUMNS) {
				System.err.printf("There should be %d columns in value %s\n",  NUM_COLUMNS, Arrays.toString(columns));
				continue;
			}
			
			boolean isReply = Boolean.parseBoolean(columns[IS_REPLY]);
			
			if (!isReply) {
				
				long timestamp = Long.parseLong(columns[CREATED_AT]);
				if (timestamp > mostRecentUserInfoTs) {
					mostRecentUserInfo = String.format("%s\t%s%s", columns[USER_SCREEN_NAME], columns[USER_DESCRIPTION], LINE_SEPARATOR);
					mostRecentUserInfoTs = timestamp;
				}
			} else {
				isEverRepliedTo = true;
			}
		}
		
		//If this user has only been replied to, then no screen name and description could be determined form the tweets
		if (mostRecentUserInfo == null && isEverRepliedTo) {
			mostRecentUserInfo = String.format("%s\t%s%s", "", "", LINE_SEPARATOR);
		}
		
		if (mostRecentUserInfo != null) {
			outputValue.set(mostRecentUserInfo);
			context.write(key, outputValue);
		} else { //This should not happen
			System.err.printf("User %s could not be processed\n", key.toString());
		}
		
	}
}