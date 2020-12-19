package edu.cmu.scs.cc.project1;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import static edu.cmu.scs.cc.project1.HashtagCountMapper.DELIMITER;

import static edu.cmu.scs.cc.project1.HashtagCountMapper.UTF8;

public class HashtagCountReducer
		extends Reducer<Text, VIntWritable, Text, VIntWritable> {
	
	private final static int USER_SCREEN_NAME = 0;
	private final static int USER_DESCRIPTION = 1;
	private final static int CREATED_AT = 2;
	private final static int IS_REPLY = 3;
	
	private final static int NUM_COLUMNS = 4;
	
	private final VIntWritable outputValue = new VIntWritable();
	
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
	public void reduce(Text key, Iterable<VIntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int hashtagCount = 0;
		
		for (VIntWritable count : values) {
			hashtagCount += count.get();
		}
		
		outputValue.set(hashtagCount);
		context.write(key, outputValue);
		
		
	}
}