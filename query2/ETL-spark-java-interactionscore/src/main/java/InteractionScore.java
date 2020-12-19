import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.lang.Math;
import scala.Tuple2;

public class InteractionScore {

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

    static void calculateInteractionScore(String inputPath, String outputPath, Boolean isLocal) {
        //System.out.println("***********Hello World Interaction score");
        SparkConf conf = new SparkConf().setAppName("interactionScore");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> tweetsEncodedRDD = sc.textFile(inputPath);
        //System.out.println("Number of lines in file = " + tweetsEncodedRDD.count());

        //1. Create a RDD of pair <tweet_id, tweet_details>
        JavaPairRDD<String, String> pairsRDD = tweetsEncodedRDD.mapToPair(line -> {
            String []split = line.split("\\s+", 2);
            String tweetId = split[0];
            String details = split[1];
            details = details.replace(" ", "");
            byte[] byteArray=null;
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

        /*
        System.out.println("**************non-filtered RDD count = " + pairsRDD.count());
        pairsRDD.foreach(pair ->{
            System.out.println("tweet id : " + pair._1() + " label " + pair._2());
        });*/


        // 2. Filter out non-contact tweets - those that have both reply to user id and retweet to user id as null
        pairsRDD = pairsRDD.filter(pair -> {
            String[] columns = pair._2().split(DELIMITER);
            if (columns.length != NUM_COLUMNS) {
                //System.out.printf("There should be %d columns in value instead of %d\n",  NUM_COLUMNS, columns.length);
                return false;
            }
            String repliedToUserId = columns[IN_REPLY_TO_USER_ID];
            String retweetedToUserId = columns[RETWEET_USER_ID];
            if(repliedToUserId.equals("null") && retweetedToUserId.equals("null")) {
                return false;
            }
            return true;
        });
        /*
        System.out.println("**************filtered RDD count = " + pairsRDD.count());
        System.out.println("********filtered RDD");
        pairsRDD.foreach(pair ->{
            System.out.println("*****filtered tweet id : " + pair._1() + " label : " + pair._2());
        });*/


        //3.
        JavaPairRDD<String, String> interactingUsersRDD = pairsRDD.mapToPair(pair -> {
            //System.out.println("pair 2 is " + pair._2());
            String[] columns = pair._2().split(DELIMITER);
            /*
            for(int i=0; i<columns.length; i++) {
                System.out.println("pair 2 column is " + columns[i]);
            }*/

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
            String hashtags = columns[HASHTAGS];
            String createdAt = columns[CREATED_AT];

            if(!repliedToUserId.equals("null"))
            {
                StringJoiner users1 = new StringJoiner(",");
                if (Long.parseLong(userId) < Long.parseLong(repliedToUserId)) {
                    users1.add(userId);
                    users1.add(repliedToUserId);
                }
                else {
                    users1.add(repliedToUserId);
                    users1.add(userId);
                }
                String usersKey1 = users1.toString();

                StringJoiner tweetDetailsJoiner = new StringJoiner("{TEAMLET'SDOIT!JOINER}");
                tweetDetailsJoiner.add(text);
                tweetDetailsJoiner.add(hashtags);
                tweetDetailsJoiner.add(null);
                tweetDetailsJoiner.add(null);
                tweetDetailsJoiner.add(createdAt);
                tweetDetailsJoiner.add(text);
                String tweetDetails = tweetDetailsJoiner.toString();

                //System.out.println("key1 is " + usersKey1);
                //System.out.println("value is " + tweetDetails);
                return new Tuple2<>(usersKey1, tweetDetails);
            }
            StringJoiner users1 = new StringJoiner(",");
            if (Long.parseLong(userId) < Long.parseLong(retweetedToUserId)) {
                users1.add(userId);
                users1.add(retweetedToUserId);
            }
            else {
                users1.add(retweetedToUserId);
                users1.add(userId);
            }
            String usersKey1 = users1.toString();

            StringJoiner tweetDetailsJoiner = new StringJoiner("{TEAMLET'SDOIT!JOINER}");
            tweetDetailsJoiner.add(null);
            tweetDetailsJoiner.add(null);
            tweetDetailsJoiner.add(text);
            tweetDetailsJoiner.add(hashtags);
            tweetDetailsJoiner.add(createdAt);
            tweetDetailsJoiner.add(text);
            String tweetDetails = tweetDetailsJoiner.toString();

            return new Tuple2<>(usersKey1, tweetDetails);
            //return:
            //key: user1, user2
            //value: reply text, reply hashtag, retweet text, retweet hashtag, created at, text(this text will be used in reduce job to store latest contact tweet)
        });
        /*
        System.out.println("**************interacting users RDD  count= " + interactingUsersRDD.count());
        interactingUsersRDD.foreach(pair ->{
            System.out.println("******user pair : " + pair._1() + " text and hashtag : " + pair._2());
        });*/


        // 4. combine the reply texts , reply hashtags, retweet texts, retweet hashtags, latest contact tweet
        /*
        user pair : 16916428,774926515
        text and hashtag : null\{TEAMLET'SDOIT!JOINER\}
                           null\{TEAMLET'SDOIT!JOINER\}
                           RT @edwardsdusty: hacking disturbs the myth of genius; it is collaborative, community-oriented --@symbolizejwbc #4c14 #h09\{TEAMLET'SDOIT!JOINER\}
                           4c14 h09\{TEAMLET'SDOIT!JOINER\}
                           created at of latest contact tweet so far
                           text of latest contact tweet so far
         */
        JavaPairRDD<String, String> interactionScoreMapRDD = interactingUsersRDD.reduceByKey(
                (Function2<String,String, String>) (tweetDetail1, tweetDetail2) ->
        {
            /*
            System.out.println("**************inside reduce function*********");
            System.out.println("*****1st value");
            System.out.println(tweetDetail1);
            System.out.println("*****2nd value");
            System.out.println(tweetDetail2);
            System.out.println("*********splittimg 1st value with delimeter " + DELIMITER);

             */
            //System.out.println("**************************tweetDetail1:"+tweetDetail1);
            String[] tweet1Columns = tweetDetail1.split(DELIMITER);
            /*
            System.out.println("*******tweet1 columns ");
            for(int i=0; i<tweet1Columns.length; i++) {
                System.out.println("*****i is " + i);
                System.out.println("*****column is " + tweet1Columns[i]);
            }*/


            //System.out.println("*********splitting 2nd value with delimeter " + DELIMITER);
            String[] tweet2Columns = tweetDetail2.split(DELIMITER);

            // merge the reply texts
            StringJoiner replyTexts = new StringJoiner("{TWEETTEXTDELIMITER}");
            if(!tweet1Columns[0].equals("null")) {
                replyTexts.add(tweet1Columns[0]);
            }
            if(!tweet2Columns[0].equals("null")) {
                replyTexts.add(tweet2Columns[0]);
            }

            // merge the reply hashtags
            StringJoiner replyHashtags = new StringJoiner(" ");
            if(!tweet1Columns[1].equals("null")) {
                replyHashtags.add(tweet1Columns[1]);
            }
            if(!tweet2Columns[1].equals("null")) {
                replyHashtags.add(tweet2Columns[1]);
            }

            // merge the retweet texts
            StringJoiner retweetTexts = new StringJoiner("{TWEETTEXTDELIMITER}");
            if(!tweet1Columns[2].equals("null")) {
                retweetTexts.add(tweet1Columns[2]);
            }
            if(!tweet2Columns[2].equals("null")) {
                retweetTexts.add(tweet2Columns[2]);
            }

            // merge the retweet hashtags
            StringJoiner retweetHashtags = new StringJoiner(" ");
            if(!tweet1Columns[3].equals("null")) {
                retweetHashtags.add(tweet1Columns[3]);
            }
            if(!tweet2Columns[3].equals("null")) {
                retweetHashtags.add(tweet2Columns[3]);
            }

            // find the latest contact tweet
            // compare the timestamps fields of both the tweets,
            // the one with
            String latestContactTweetTxt = null;
            String lastestCreatedAt = null;
            if (Long.parseLong(tweet1Columns[4]) > Long.parseLong(tweet2Columns[4])) {
                lastestCreatedAt = tweet1Columns[4];
                latestContactTweetTxt = tweet1Columns[5];
            }
            else {
                lastestCreatedAt = tweet2Columns[4];
                latestContactTweetTxt = tweet2Columns[5];
            }
            StringJoiner tweetDetailsJoiner = new StringJoiner("{TEAMLET'SDOIT!JOINER}");
            tweetDetailsJoiner.add(replyTexts.toString());
            tweetDetailsJoiner.add(replyHashtags.toString());
            tweetDetailsJoiner.add(retweetTexts.toString());
            tweetDetailsJoiner.add(retweetHashtags.toString());
            tweetDetailsJoiner.add(lastestCreatedAt);
            tweetDetailsJoiner.add(latestContactTweetTxt);
            String tweetDetails = tweetDetailsJoiner.toString();

            return tweetDetails;

        });
        /*
        System.out.println("**************interaction score RDD  count= " + interactionScoreMapRDD.count());
        interactionScoreMapRDD.foreach(pair ->{
            System.out.println("*******interaction score map user pair : " + pair._1() + " text and hashtag : " + pair._2());
        });*/

        // 5.
        JavaPairRDD<String,String> interactionScoreCalcultatedRDD = interactionScoreMapRDD.mapToPair(pair -> {

            String[] columns = pair._2().split(DELIMITER);
            String replyText = columns[0];
            //System.out.println("**** reply text :" + replyText);
            String[] replyTexts = replyText.split(TWEETTEXTDELIMITER);
            int numReplies = replyTexts.length;
            if(replyTexts.length==1 && (replyTexts[0].equals("")||replyTexts[0].equals("null")))
                numReplies = 0;
            //System.out.println("number of replies:"+numReplies);

            String retweetText = columns[2];
            //System.out.println("**** retweet text :" + retweetText);
            String[] retweetTexts = retweetText.split(TWEETTEXTDELIMITER);
            int numRetweets = retweetTexts.length;
            if(retweetTexts.length==1 && (retweetTexts[0].equals("")||retweetTexts[0].equals("null")))
                numRetweets = 0;
            //System.out.println("number of retweets:"+numRetweets);

            double interactionScore = Math.log(1+2*numReplies+numRetweets);
            System.out.println("for user pair " + pair._1() + " numReplies " + numReplies +
                    " numRetweets " + numRetweets + " interaction score " + interactionScore);
            StringJoiner interactionDetails = new StringJoiner("{TEAMLET'SDOIT!JOINER}");
            interactionDetails.add(Double.toString(interactionScore));
            interactionDetails.add(replyText);
            interactionDetails.add(columns[1]); //reply hashtags
            interactionDetails.add(retweetText);
            interactionDetails.add(columns[3]); //retweet hashtags
            interactionDetails.add(columns[5]); //latestContactTweetTxt
            String userPair = pair._1();
            return new Tuple2<>(userPair, interactionDetails.toString());
        });

        System.out.println("**************interaction score RDD  count= " + interactionScoreCalcultatedRDD.count());
        interactionScoreCalcultatedRDD.foreach(pair ->{
            System.out.println("*******interaction score RDD user pair : " + pair._1() + " value : " + pair._2());
        });


        //6. convert to flat map - user1,user2 user2,user1
        JavaPairRDD<String, String> interactingUsersComboRDD = interactionScoreCalcultatedRDD.flatMapToPair(pair -> {
            String[] users = pair._1().split(",");
            String userCombination1 = users[0] + "," + users[1];
            String userCombination2 = users[1] + "," + users[0];
            List<Tuple2<String,String>> entries = new ArrayList<>();
            entries.add(new Tuple2<>(userCombination1, pair._2()));
            entries.add(new Tuple2<>(userCombination2, pair._2()));
            return entries.iterator();
        });
        /*
        System.out.println("**************interaction score combo RDD  count= " + interactingUsersComboRDD.count());
        interactingUsersComboRDD.foreach(pair ->{
            System.out.println("*******interaction score combo RDD user pair : " + pair._1() + " value : " + pair._2());
        });*/

    }
}
