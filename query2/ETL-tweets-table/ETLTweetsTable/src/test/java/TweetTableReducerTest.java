import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TweetTableReducerTest {
	   private Reducer<Text, Text, Text, Text> reducer;
	   private ReduceDriver<Text, Text, Text, Text> driver;
	    
	    /**
	     * Setup the reducer for Wikipedia data analysis.
	     */
	    @Before
	    public void setUp() {
	        reducer = new TweetTableReducer();
	        driver = new ReduceDriver<>(reducer);
	    }
	    
	    @Test
	    public void testWikiReducer() throws IOException {
	        
	        driver.withInput(new Text("47028681249017858"), Arrays.asList(new Text("260559433\tnull\t533116340\tRT @gyutano6o6: ～種族を超えた愛が、今ここに～　 #一番目にリプきたキャラに二番目にリプきたキャラへキスさせる http://t.co/Agm4LUmzS7TEAMLETSDOITEOLJEYRAJWRE")))
	        .withOutput(new Text("47028681249017858"), new Text("260559433\tnull\t533116340\tRT @gyutano6o6: ～種族を超えた愛が、今ここに～　 #一番目にリプきたキャラに二番目にリプきたキャラへキスさせる http://t.co/Agm4LUmzS7TEAMLETSDOITEOLJEYRAJWRE"))
	        .runTest(false);
	        
	    }
}
