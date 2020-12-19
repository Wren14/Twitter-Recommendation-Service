import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.nio.charset.Charset;
import java.util.*;
import java.lang.Math;
import scala.Tuple2;

public class UserInfo {
    private final static Charset UTF8 = Charset.forName("UTF-8");
    private final static String DELIMITER = "\\{TEAMLET'SDOIT!JOINER\\}";
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
    private final static String TWEETTEXTDELIMITER = "\\{DELIMITERTWEETTEXT\\}";

    static void getUserInfo(String inputPath, String outputPath, Boolean isLocal) {
        //System.out.println("***********Hello World Interaction score");
        SparkConf conf = new SparkConf().setAppName("userInfo");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> tweetsEncodedRDD = sc.textFile(inputPath);

        //1. Create a RDD of pair <tweet_id, tweet_details>
        JavaPairRDD<String, String> pairsRDD = tweetsEncodedRDD.mapToPair(line -> {
            String[] split = line.split("\\s+", 2);
            String tweetId = split[0];
            String details = split[1];
            details = details.replace(" ", "");
            byte[] byteArray = null;
            try {
                byteArray = Hex.decodeHex(details.toCharArray());
            } catch (DecoderException e) {
                e.printStackTrace();
            }
            String output = new String(byteArray, UTF8);
            //System.out.println("=====================");
            //System.out.println(output);
            //System.out.println("========================");
            return new Tuple2<>(tweetId, output);
        });

        //2. create RDD of (<user_id>, <user_screen_name, user_description, created_at_tweet) by reading 1 tweet
        JavaPairRDD<String, String> userInfoRDD = pairsRDD.flatMapToPair(pair -> {
            //System.out.println("pair 2 is " + pair._2());
            String[] columns = pair._2().split(DELIMITER);
            /*
            for(int i=0; i<columns.length; i++) {
                System.out.println("pair 2 column is " + columns[i]);
            }*/
            ArrayList<Tuple2<String,String>> entries = new ArrayList<>();
            String userId = columns[USER_ID];
            String userScreenName = columns[USER_SCREEN_NAME];
            String userDescription = columns[USER_DESCRIPTION];
            String createdAt = columns[CREATED_AT];
            StringJoiner joiner = new StringJoiner("{TEAMLET'SDOIT!JOINER}");
            joiner.add(userScreenName);
            joiner.add(userDescription);
            joiner.add(createdAt);
            entries.add(new Tuple2<>(userId, joiner.toString()));

            String repliedUserId = columns[IN_REPLY_TO_USER_ID];
            StringJoiner joinerRepliedUser = new StringJoiner("{TEAMLET'SDOIT!JOINER}");
            joinerRepliedUser.add(userScreenName);
            joinerRepliedUser.add(userDescription);
            joinerRepliedUser.add(createdAt);
            entries.add(new Tuple2<>(repliedUserId, joinerRepliedUser.toString()));

            String retweetedUserId = columns[RETWEET_USER_ID];
            String retweetedUserScreenName = columns[RETWEET_USER_SCREEN_NAME];
            String retweetedUserDescription = columns[RETWEET_USER_DESCRIPTION];
            StringJoiner joinerRetweetedUser = new StringJoiner("{TEAMLET'SDOIT!JOINER}");
            joinerRetweetedUser.add(retweetedUserScreenName);
            joinerRetweetedUser.add(retweetedUserDescription);
            joinerRetweetedUser.add(createdAt);
            entries.add(new Tuple2<>(retweetedUserId, joinerRepliedUser.toString()));

            return entries.iterator();
        });
        System.out.println("**************user latest info count= " + userInfoRDD.count());
        userInfoRDD.foreach(pair -> {
            System.out.println("*******user latest info 1 tweet id : " + pair._1() + " value : " + pair._2());
        });

        //3. reduce by key - user_id
        //based in lastest created at - store latest user_screen_name, user_description
        userInfoRDD = userInfoRDD.reduceByKey(
                (Function2<String, String, String>) (userDetails1, userDetails2) -> {
                    String[] user1Details = userDetails1.split(DELIMITER);
                    String user1ScreenName = user1Details[0];
                    String user1Description = user1Details[1];
                    String user1CreatedAt = user1Details[2];

                    String[] user2Details = userDetails2.split(DELIMITER);
                    String user2ScreenName = user2Details[0];
                    String user2Description = user2Details[1];
                    String user2CreatedAt = user2Details[2];

                    if (Long.parseLong(user1CreatedAt) > Long.parseLong(user2CreatedAt)) {
                        StringJoiner joiner = new StringJoiner("{TEAMLET'SDOIT!JOINER}");
                        joiner.add(user1ScreenName);
                        joiner.add(user1Description);
                        joiner.add(user1CreatedAt);
                        return joiner.toString();
                    }
                    StringJoiner joiner = new StringJoiner("{TEAMLET'SDOIT!JOINER}");
                    joiner.add(user2ScreenName);
                    joiner.add(user2Description);
                    joiner.add(user2CreatedAt);
                    return joiner.toString();
                });
        System.out.println("**************user latest info count= " + userInfoRDD.count());
        userInfoRDD.foreach(pair -> {
            System.out.println("*******user latest info all tweets id : " + pair._1() + " value : " + pair._2());
        });

        //4. mapToPair
        // (<user_id>, <user_screen_name, user_description>
        JavaPairRDD<String, String> userLatestInfoRDD = userInfoRDD.mapToPair(pair -> {
            String[] user1Details = pair._2().split(DELIMITER);
            String user1ScreenName = user1Details[0];
            String user1Description = user1Details[1];
            StringJoiner joiner = new StringJoiner("{TEAMLET'SDOIT!JOINER}");
            joiner.add(user1ScreenName);
            joiner.add(user1Description);
            return new Tuple2<>(pair._1(), joiner.toString());

        });

        System.out.println("**************user latest info count= " + userLatestInfoRDD.count());
        userLatestInfoRDD.foreach(pair -> {
            System.out.println("*******user latest info id : " + pair._1() + " value : " + pair._2());
        });

    }
}