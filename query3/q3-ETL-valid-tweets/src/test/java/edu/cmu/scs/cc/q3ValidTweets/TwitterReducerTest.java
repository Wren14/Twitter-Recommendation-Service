package edu.cmu.scs.cc.q3ValidTweets;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TwitterReducerTest {

    private Reducer<Text, BytesWritable, Text, BytesWritable> reducer;
    private ReduceDriver<Text, BytesWritable, Text, BytesWritable> driver;
    
    /**
     * Setup the reducer for Wikipedia data analysis.
     */
    @Before
    public void setUp() {
        reducer = new TwitterReducer();
        driver = new ReduceDriver<>(reducer);
    }
    
    @Test
    public void testWikiReducer() throws IOException {
    	String joinedStr = "\"this is sample text\",1416350234,\"Shoshaa_14\",\"Just remember when life knocks you down on your knees in the perfect position to pray ;Student of English Language sixth grade in KAU\",null,1138390430,\"tyousef63\",\"هلالي ثم هلالي ثم هلالي. وبعد هلالي\",\"airnews\"";
        byte arr[] = joinedStr.getBytes("UTF8");
        BytesWritable tweetDetails = new BytesWritable();;
        tweetDetails.set(arr,0,arr.length);
        /*
        driver.withInput(new Text("451953314234904576"), Arrays.asList( tweetDetails, tweetDetails))
        .withOutput(new Text("451953314234904576"), tweetDetails)
        .runTest(false);
        */
    }
}