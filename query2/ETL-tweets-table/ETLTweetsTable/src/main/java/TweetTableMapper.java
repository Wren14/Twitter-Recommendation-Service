import java.io.IOException;

import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TweetTableMapper extends Mapper<Object, Text, Text, Text>{
    public Text tweetId = new Text();
    public Text tweetDetails = new Text();
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
	
	static final String DELIMITER = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"; //",";

	private final Text outputValue = new Text();
	private final Text outputKey = new Text();
    /**
     * 
     *
     * @param key input key of mapper
     * @param value input value of mapper
     * @param context output key/value pair of mapper
     * @throws IOException if io exception occurs
     * @throws InterruptedException if interrupted exception occurs
     * 
     * input: 
     * value: created_at, "text", user_id, user_screen_name, user_description, 
     * in_reply_to_user_id, retweet_user_id, retweet_user_screen_name, 
     * retweet_user_description, "hashtags"(each space separated),
     * 
     * output:
     * key: tweet_id  
     * value: user_id, replied_to_user_id, retweeted_to_user_id, text
     */
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	
    	String input = value.toString();
    	
        input = input.trim();
    	//System.out.println("\ninput is: " + input);
        String []split = input.split("\\s+", 2);
    	
    	String tweetId = split[0];
    	
    	String details = split[1];
    	
    	//System.out.println("\nvalue before: " + details);
    	details = details.replace(" ", "");
    	//System.out.println("\nvalue after: " + details);
    	
    	byte[] byteArray=null;
		try {
			byteArray = Hex.decodeHex(details);
		} catch (DecoderException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	String output = new String(byteArray, StandardCharsets.UTF_8);
    	
    	//System.out.println(output);
    	    	
	  	String[] columns = output.split("\\{TEAMLET'SDOIT!JOINER\\}");

	  	/*
		for (String column : columns) {
			System.out.println(column);
		}*/
		
		
		if (columns.length != NUM_COLUMNS) {
			System.out.printf("There should be %d columns in value instead of %d\n",  NUM_COLUMNS, columns.length);
			return;
		}
		
		String userId = columns[USER_ID];
		//System.out.println("user id is " + userId);
	    String repliedToUserId = columns[IN_REPLY_TO_USER_ID];
	    //System.out.println("replied uid " + repliedToUserId);
	    String retweetedToUserId = columns[RETWEET_USER_ID];
	    //System.out.println("retweeted uid " + retweetedToUserId);
	    String text = columns[TEXT];
	    //System.out.println("text " + text);
	    //System.out.println(!repliedToUserId.equals("null"));
	    //System.out.println(!retweetedToUserId.equals("null"));
	    if(!repliedToUserId.equals("null") || !retweetedToUserId.equals("null"))
	    {
	    	StringJoiner tweetDetailsJoiner = new StringJoiner("\t");
	    	tweetDetailsJoiner.add(userId);   
	    	tweetDetailsJoiner.add(repliedToUserId);
	    	tweetDetailsJoiner.add(retweetedToUserId);
	    	tweetDetailsJoiner.add(text);
              
	    	//System.out.println("key is " + tweetId);
	    	//System.out.println("value is " + tweetDetailsJoiner.toString());
	    	String tweetRecord = tweetDetailsJoiner.toString().concat("TEAMLETSDOITEOLJEYRAJWRE");
	    	tweetDetails.set(tweetRecord);
            
	    	outputKey.set(tweetId);
	    	context.write(outputKey, tweetDetails);
	    }
    }
}
