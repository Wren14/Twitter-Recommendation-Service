package edu.cmu.scs.cc.q3ValidTweets;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TwitterMapReduceTest {

    private Mapper<Object, Text, Text, BytesWritable> mapper;
    private Reducer<Text, BytesWritable, Text, BytesWritable> reducer;
    private MapReduceDriver<Object, Text, Text, BytesWritable, Text, BytesWritable> driver;

    /**
     * Setup the mapper for inverted index.
     */
    @Before
    public void setUp() {
        mapper = new TwitterMapper();
        reducer = new TwitterReducer();
        driver = new MapReduceDriver<>(mapper, reducer);
    }

    @Test
    public void testInvertedIndexMapReduce() throws IOException {
    	String joinedStr = "\"this is sample text\",1416350234,\"Shoshaa_14\",\"Just remember when life knocks you down on your knees in the perfect position to pray ;Student of English Language sixth grade in KAU\",null,1138390430,\"tyousef63\",\"هلالي ثم هلالي ثم هلالي. وبعد هلالي\",\"airnews\"";
        byte arr[] = joinedStr.getBytes("UTF8");
        BytesWritable tweetDetails = new BytesWritable();;
        tweetDetails.set(arr,0,arr.length);
        /*
        driver.withInput(new Text(""), 
        				new Text("{\"created_at\":\"Fri Apr 04 05:24:01 +0000 2014\",\"id\":451953314234904576,\"id_str\":\"451953314234904576\",\"text\":\"this is sample text\",\"source\":\"\\u003ca href=\\\"http:\\/\\/twitter.com\\/download\\/iphone\\\" rel=\\\"nofollow\\\"\\u003eTwitter for iPhone\\u003c\\/a\\u003e\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":1416350234,\"id_str\":\"1416350234\",\"name\":\"\\ufbaa\\ufba9\\ufee0\\u0622\\ufedf\\ufb55\\ufba7 \\ufee3\\ufee0\\ufb95\\ufb55\\ufba7 \\u265b\",\"screen_name\":\"Shoshaa_14\",\"location\":\"\",\"url\":null,\"description\":\"Just remember when life knocks you down on your knees in the perfect position to pray ;Student of English Language sixth grade in KAU\",\"protected\":false,\"followers_count\":2351,\"friends_count\":2568,\"listed_count\":7,\"created_at\":\"Thu May 09 20:02:18 +0000 2013\",\"favourites_count\":92,\"utc_offset\":null,\"time_zone\":null,\"geo_enabled\":false,\"verified\":false,\"statuses_count\":5300,\"lang\":\"ar\",\"contributors_enabled\":false,\"is_translator\":false,\"is_translation_enabled\":false,\"profile_background_color\":\"C0DEED\",\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_tile\":false,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/443632702458433536\\/xUvg52zz_normal.jpeg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/443632702458433536\\/xUvg52zz_normal.jpeg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/1416350234\\/1396466890\",\"profile_link_color\":\"0084B4\",\"profile_sidebar_border_color\":\"C0DEED\",\"profile_sidebar_fill_color\":\"DDEEF6\",\"profile_text_color\":\"333333\",\"profile_use_background_image\":true,\"default_profile\":true,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null,\"retweeted_status\":{\"created_at\":\"Fri Apr 04 05:22:50 +0000 2014\",\"id\":451953017819246592,\"id_str\":\"451953017819246592\",\"text\":\"\\u0623\\u062e\\u0631 \\u062f\\u0642\\u0627\\u0626\\u0642 \\u0645\\u0628\\u0627\\u0631\\u0627\\u0629 \\u0627\\u0644\\u0647\\u0644\\u0627\\u0644 \\u0648\\u0627\\u0644\\u063a\\u0631\\u0627\\u0641\\u0629 \\u0628\\u0635\\u0648\\u062a \\u0641\\u0627\\u0631\\u0633 \\u0639\\u0648\\u0636 http:\\/\\/t.co\\/Rtg2PyHgFx\",\"source\":\"\\u003ca href=\\\"http:\\/\\/www.apple.com\\\" rel=\\\"nofollow\\\"\\u003eiOS\\u003c\\/a\\u003e\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":1138390430,\"id_str\":\"1138390430\",\"name\":\"\\u0637\\u0627\\u0631\\u0642 \\u064a\\u0648\\u0633\\u0641 \\u0627\\u0644\\u062e\\u0644\\u0641(S9)\",\"screen_name\":\"tyousef63\",\"location\":\"\\u0627\\u0644\\u0631\\u064a\\u0627\\u0636\",\"url\":null,\"description\":\"\\u0647\\u0644\\u0627\\u0644\\u064a \\u062b\\u0645 \\u0647\\u0644\\u0627\\u0644\\u064a \\u062b\\u0645 \\u0647\\u0644\\u0627\\u0644\\u064a. \\u0648\\u0628\\u0639\\u062f \\u0647\\u0644\\u0627\\u0644\\u064a\",\"protected\":false,\"followers_count\":2119,\"friends_count\":2050,\"listed_count\":2,\"created_at\":\"Fri Feb 01 01:22:54+0000 2013\",\"favourites_count\":20,\"utc_offset\":null,\"time_zone\":null,\"geo_enabled\":false,\"verified\":false,\"statuses_count\":2252,\"lang\":\"en\",\"contributors_enabled\":false,\"is_translator\":false,\"is_translation_enabled\":false,\"profile_background_color\":\"C0DEED\",\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_tile\":false,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/378800000692188761\\/7c784af8572ba888cc819a2cca2ed908_normal.jpeg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/378800000692188761\\/7c784af8572ba888cc819a2cca2ed908_normal.jpeg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/1138390430\\/1383545639\",\"profile_link_color\":\"0084B4\",\"profile_sidebar_border_color\":\"C0DEED\",\"profile_sidebar_fill_color\":\"DDEEF6\",\"profile_text_color\":\"333333\",\"profile_use_background_image\":true,\"default_profile\":true,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null,\"contributors\":null,\"retweet_count\":1,\"favorite_count\":0,\"entities\":{\"hashtags\":[{\"text\":\"airnews\",\"indices\":[128,136]}],\"symbols\":[],\"urls\":[{\"url\":\"http:\\/\\/t.co\\/Rtg2PyHgFx\",\"expanded_url\":\"http:\\/\\/youtu.be\\/wIq1g-mgvps\",\"display_url\":\"youtu.be\\/wIq1g-mgvps\",\"indices\":[47,69]}],\"user_mentions\":[]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"lang\":\"ar\"},\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[{\"text\":\"airnews\",\"indices\":[128,136]}],\"symbols\":[],\"urls\":[{\"url\":\"http:\\/\\/t.co\\/Rtg2PyHgFx\",\"expanded_url\":\"http:\\/\\/youtu.be\\/wIq1g-mgvps\",\"display_url\":\"youtu.be\\/wIq1g-mgvps\",\"indices\":[62,84]}],\"user_mentions\":[{\"screen_name\":\"tyousef63\",\"name\":\"\\u0637\\u0627\\u0631\\u0642 \\u064a\\u0648\\u0633\\u0641 \\u0627\\u0644\\u062e\\u0644\\u0641(S9)\",\"id\":1138390430,\"id_str\":\"1138390430\",\"indices\":[3,13]}]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"medium\",\"lang\":\"ar\"}"))
                .withOutput(new Text("451953314234904576"), tweetDetails)
                .runTest(false);
                */
    }
}
