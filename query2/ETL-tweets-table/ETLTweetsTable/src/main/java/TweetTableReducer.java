import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TweetTableReducer extends Reducer<Text, Text, Text, Text>{

	private final Text outputValue = new Text();
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
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
    	Text tweetDetail = null;
    	// ideally we should never have length of values greater than 1
    	// because there are no duplicate tweets
    	for (Text value: values) {
    		tweetDetail = value;
    	  	break;
    	}
    	outputValue.set(tweetDetail);
        context.write(key , outputValue);
    }
    
}
