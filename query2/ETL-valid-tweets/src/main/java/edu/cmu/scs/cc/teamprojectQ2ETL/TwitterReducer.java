package edu.cmu.scs.cc.teamprojectQ2ETL;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TwitterReducer extends Reducer<Text, BytesWritable, Text, BytesWritable>{
//retain only one of the duplicate tweet , retain the first
	
    /**
     * The reduce function to run the Wiki Data Analysis job.
     *
     * Implement the reduce method.
     *
     * Output (word, inputFiles) key/value pair.
     *
     * inputFiles: (filename1,filename2,...)
     *
     * @param key     input key of reducer
     * @param values  input values of reduce which is iterable
     * @param context output key/value pair of reducer
     * @throws InterruptedException 
     * @throws IOException 
     * 
     * Input:
     * key: tweet_id
     * values: [created_at, "text", user_id, user_screen_name, user_description, in_reply_to_user_id,
     * retweet_user_id, retweet_user_screen_name, retweet_user_description, 
     * hashtags(each space separated)]
     */
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context)
            throws IOException, InterruptedException {
        
    	BytesWritable tweetDetail = null;
    	for (BytesWritable tweetValue:values) {
    	  	tweetDetail = tweetValue;
    	  	//String s = new String(tweetDetail.getBytes(), StandardCharsets.UTF_8);
    	  	//System.out.println("string is: " + s);
    	  	break;
    	}
        context.write(key , tweetDetail);
    }
    
}
