import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.Optional;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.nio.charset.Charset;
import java.util.*;

public class Main {
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

    public static void main(String args[]) {

        //String inputPath = "input_1stfile/input_1_tweet_1st_file";
        //String inputPath = "input_1stfile/input_100_tweets_1st_file";
        //String inputPath = "input_1stfile/input_300tweets_1st_file";
        String inputPath = "input_1stfile/input_1st_file";
        //String outputPath = "output_5tweets/iteration-score-5tweets";
        Boolean isLocal = true;

        SparkConf conf = new SparkConf().setAppName("interactionScore").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //0. Read the valid tweets
        JavaRDD<String> tweetsEncodedRDD = sc.textFile(inputPath);
        //System.out.println("Number of lines in file = " + tweetsEncodedRDD.count());

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

        /*
        System.out.println("**************non-filtered RDD count = " + pairsRDD.count());
        pairsRDD.foreach(pair -> {
            System.out.println("tweet id : " + pair._1() + " label " + pair._2());
        });

         */


        // 2. Filter out non-contact tweets - those that have both reply to user id and retweet to user id as null
        pairsRDD = pairsRDD.filter(pair -> {
            String[] columns = pair._2().split(DELIMITER);
            if (columns.length != NUM_COLUMNS) {
                //System.out.printf("There should be %d columns in value instead of %d\n",  NUM_COLUMNS, columns.length);
                return false;
            }
            String repliedToUserId = columns[IN_REPLY_TO_USER_ID];
            String retweetedToUserId = columns[RETWEET_USER_ID];
            if (repliedToUserId.equals("null") && retweetedToUserId.equals("null")) {
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

            if (!repliedToUserId.equals("null")) {
                StringJoiner users1 = new StringJoiner(",");
                if (Long.parseLong(userId) < Long.parseLong(repliedToUserId)) {
                    users1.add(userId);
                    users1.add(repliedToUserId);
                } else {
                    users1.add(repliedToUserId);
                    users1.add(userId);
                }
                String usersKey1 = users1.toString();

                StringJoiner tweetDetailsJoiner = new StringJoiner("{TEAMLET'SDOIT!JOINER}");
                tweetDetailsJoiner.add(text);
                tweetDetailsJoiner.add(hashtags);
                tweetDetailsJoiner.add("null");
                tweetDetailsJoiner.add("null");
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
            } else {
                users1.add(retweetedToUserId);
                users1.add(userId);
            }
            String usersKey1 = users1.toString();

            StringJoiner tweetDetailsJoiner = new StringJoiner("{TEAMLET'SDOIT!JOINER}");
            tweetDetailsJoiner.add("null");
            tweetDetailsJoiner.add("null");
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

        // user pair : 16916428,774926515
        //text and hashtag : null\{TEAMLET'SDOIT!JOINER\}
        //                   null\{TEAMLET'SDOIT!JOINER\}
        //                   RT @edwardsdusty: hacking disturbs the myth of genius; it is collaborative, community-oriented --@symbolizejwbc #4c14 #h09\{TEAMLET'SDOIT!JOINER\}
        //                   4c14 h09\{TEAMLET'SDOIT!JOINER\}
        //                   created at of latest contact tweet so far
        //                   text of latest contact tweet so far


        JavaPairRDD<String, String> interactionScoreMapRDD = interactingUsersRDD.reduceByKey(
                (Function2<String, String, String>) (tweetDetail1, tweetDetail2) ->
                {

                    //System.out.println("**************************tweetDetail1:"+tweetDetail1);
                    String[] tweet1Columns = tweetDetail1.split(DELIMITER);


                    //System.out.println("*********splitting 2nd value with delimeter " + DELIMITER);
                    String[] tweet2Columns = tweetDetail2.split(DELIMITER);

                    // merge the reply texts
                    StringJoiner replyTexts = new StringJoiner("{TWEETTEXTDELIMITER}");
                    if (!tweet1Columns[0].equals("null")) {
                        replyTexts.add(tweet1Columns[0]);
                    }
                    if (!tweet2Columns[0].equals("null")) {
                        replyTexts.add(tweet2Columns[0]);
                    }

                    // merge the reply hashtags
                    StringJoiner replyHashtags = new StringJoiner(" ");
                    if (!tweet1Columns[1].equals("null")) {
                        replyHashtags.add(tweet1Columns[1]);
                    }
                    if (!tweet2Columns[1].equals("null")) {
                        replyHashtags.add(tweet2Columns[1]);
                    }

                    // merge the retweet texts
                    StringJoiner retweetTexts = new StringJoiner("{TWEETTEXTDELIMITER}");
                    if (!tweet1Columns[2].equals("null")) {
                        retweetTexts.add(tweet1Columns[2]);
                    }
                    if (!tweet2Columns[2].equals("null")) {
                        retweetTexts.add(tweet2Columns[2]);
                    }

                    // merge the retweet hashtags
                    StringJoiner retweetHashtags = new StringJoiner(" ");
                    if (!tweet1Columns[3].equals("null")) {
                        retweetHashtags.add(tweet1Columns[3]);
                    }
                    if (!tweet2Columns[3].equals("null")) {
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
                    } else {
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
        JavaPairRDD<String, String> interactionScoreCalcultatedRDD = interactionScoreMapRDD.mapToPair(pair -> {

            String[] columns = pair._2().split(DELIMITER);
            String replyText = columns[0];
            //System.out.println("**** reply text :" + replyText);
            String[] replyTexts = replyText.split(TWEETTEXTDELIMITER);
            int numReplies = replyTexts.length;
            if (replyTexts.length == 1 && (replyTexts[0].equals("") || replyTexts[0].equals("null")))
                numReplies = 0;
            //System.out.println("number of replies:"+numReplies);

            String retweetText = columns[2];
            //System.out.println("**** retweet text :" + retweetText);
            String[] retweetTexts = retweetText.split(TWEETTEXTDELIMITER);
            int numRetweets = retweetTexts.length;
            if (retweetTexts.length == 1 && (retweetTexts[0].equals("") || retweetTexts[0].equals("null")))
                numRetweets = 0;
            //System.out.println("number of retweets:"+numRetweets);

            double interactionScore = Math.log(1 + 2 * numReplies + numRetweets);

            System.out.println("**********for user pair " + pair._1() + " numReplies " + numReplies +
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
        /*
        System.out.println("**************interaction score RDD  count= " + interactionScoreCalcultatedRDD.count());
        interactionScoreCalcultatedRDD.foreach(pair ->{
            System.out.println("*******interaction score RDD user pair : " + pair._1() + " value : " + pair._2());
        });*/


        //6. convert to flat map - user1,user2 user2,user1
        JavaPairRDD<String, String> interactingUsersComboRDD = interactionScoreCalcultatedRDD.flatMapToPair(pair -> {
            String[] users = pair._1().split(",");
            String userCombination1 = users[0] + "," + users[1];
            String userCombination2 = users[1] + "," + users[0];
            List<Tuple2<String, String>> entries = new ArrayList<>();
            entries.add(new Tuple2<>(userCombination1, pair._2()));
            entries.add(new Tuple2<>(userCombination2, pair._2()));
            return entries.iterator();
        });
        /*
        System.out.println("**************interaction score combo RDD  count= " + interactingUsersComboRDD.count());
        interactingUsersComboRDD.foreach(pair -> {
            System.out.println("*******interaction score combo RDD user pair : " + pair._1() + " value : " + pair._2());
        });

        */


//=============================================================================================================

        JavaRDD<String> blacklistedHastagsRDD = sc.textFile("popular_hashtags.txt");
        String[] blacklistedHastags = blacklistedHastagsRDD.collect().toArray(new String[0]);

        //2. convert to flat map - (<hashtag,user>,<count>) for each tweet
        //JavaPairRDD<String, String> hashtagUserCountRDD = pairsRDD.flatMapToPair(pair -> {
        JavaPairRDD<String, Integer> hashtagUserCountRDD = pairsRDD.flatMapToPair(pair -> {
            //System.out.println("pair 2 is " + pair._2());
            String[] columns = pair._2().split(DELIMITER);

            String userId = columns[USER_ID];
            String hashtags = columns[HASHTAGS].toLowerCase(Locale.ENGLISH);
            String[] hashtagList = hashtags.split(" ");
            HashMap<String, Integer> counter = new HashMap<String, Integer>();
            // if one hashtag is used twice in 1 tweet

            for (int i = 0; i < hashtagList.length; i++) {
                // don't count the blacklisted hashtags
                if (!Arrays.stream(blacklistedHastags).anyMatch(hashtagList[i]::equals))
                    counter.put(hashtagList[i], counter.getOrDefault(hashtagList[i], 0) + 1);
            }

            ArrayList<Tuple2<String, Integer>> oneTweetHashtagUsersCount = new ArrayList<>();
            Iterator hashtagIterator = counter.entrySet().iterator();
            while (hashtagIterator.hasNext()) {
                Map.Entry<String, Integer> hashtagCount = (Map.Entry) hashtagIterator.next();
                String hashtag = hashtagCount.getKey();
                int count = hashtagCount.getValue();
                //String hashtagCountValue = userId + "," + Integer.toString(count);
                //oneTweetHashtagUsersCount.add(new Tuple2<>(hashtag, hashtagCountValue));
                String hashtagUser = hashtag + "," + userId;
                oneTweetHashtagUsersCount.add(new Tuple2<>(hashtagUser, count));
            }

            return oneTweetHashtagUsersCount.iterator();
        });
        /*
        System.out.println("**************hashtag count, 1hashtag 1 user 1 tweet= " + hashtagUserCountRDD.count());
        hashtagUserCountRDD.foreach(pair ->{
            System.out.println("*******hashtag flatmaptopair RDD hashtag and user: " + pair._1() + " count : " + pair._2());
        });*/


        // 2B. calculate the count for each hashtag for each user over multiple tweets
        // (<hashtag1,user1>,<count>)
        hashtagUserCountRDD = hashtagUserCountRDD.reduceByKey(
                (Function2<Integer, Integer, Integer>) (Integer user1Count, Integer user2Count) ->
                {
                    return user1Count + user2Count;
                });

        /*
        System.out.println("**************hashtag 1hashtag 1 user multiple tweets count= " + hashtagUserCountRDD.count());
        hashtagUserCountRDD.foreach(pair ->{
            System.out.println("*******hashtag reducebykey RDD hashtag : " + pair._1() + " value : " + pair._2());
        });*/


        // 2C. MapToPair (<hashtag1,user1>,<count>) -> (<hashtag1><user1,count>)
        JavaPairRDD<String, String> hashtagAndUserCountRDD = hashtagUserCountRDD.mapToPair(pair -> {
            //String hashtagUser = pair._1();
            String hashtag = pair._1().split(",")[0];
            String user = pair._1().split(",")[1];
            String userCount = user + "," + Integer.toString(pair._2());
            return new Tuple2<>(hashtag, userCount);
        });

        /*
        System.out.println("**************hashtag: 1 hashtag 1 user multiple tweets " + hashtagUserCountRDD.count());
        hashtagUserCountRDD.foreach(pair ->{
            System.out.println("*******hashtag flatmaptopair RDD hashtag : " + pair._1() + " user and count : " + pair._2());
        });*/


        // 3. calculate the count for each hashtag used by different users over multiple tweets
        // (<hashtag1>,<user1,count1 user2,count2>)
        JavaPairRDD<String, String> hashtagUserCountAllTweetsRDD = hashtagAndUserCountRDD.reduceByKey(
                (Function2<String, String, String>) (String user1Count, String user2Count) ->
                {
                    StringJoiner joiner = new StringJoiner(" ");
                    joiner.add(user1Count);
                    joiner.add(user2Count);
                    return joiner.toString();
                });

        /*
        System.out.println("**************each hashtag reducer count= " + hashtagUserCountAllTweetsRDD.count());
        hashtagUserCountAllTweetsRDD.foreach(pair ->{
            System.out.println("*******hashtag reduce RDD hashtag : " + pair._1() + " value : " + pair._2());
        });*/


        //4. flatMap - for all users - each row is for one hashtag
        //(<user1,user2>,<total-count>
        JavaPairRDD<String, Integer> userPairHashtagTotalRDD = hashtagUserCountAllTweetsRDD.flatMapToPair(pair -> {
            String hashtag = pair._1();
            String userCountList = pair._2();
            String[] userAndCount = userCountList.split(" ");
            //System.out.println("*** hashtag: " + hashtag + " userAndCount length " + userAndCount.length);
            ArrayList<Tuple2<String, Integer>> entries = new ArrayList<>();
            //each user with every user
            for (int i = 0; i < userAndCount.length; i++) {
                String user1 = userAndCount[i].split(",")[0];
                String count1 = userAndCount[i].split(",")[1];
                for (int j = i + 1; j < userAndCount.length; j++) {
                    String user2 = userAndCount[j].split(",")[0];
                    String count2 = userAndCount[j].split(",")[1];
                    int totalCount = Integer.parseInt(count1) + Integer.parseInt(count2);
                    String userPair = user1 + "," + user2;
                    //String hashtagAndCount = hashtag + "," + Integer.toString(totalCount);
                    //entries.add(new Tuple2<>(userPair, hashtagAndCount));
                    //test write only count for one paticular hashtag
                    entries.add(new Tuple2<>(userPair, totalCount));
                }
            }
            return entries.iterator();
        });

        /*
        System.out.println("**************userPairHashtagTotalRDD  count= " + userPairHashtagTotalRDD.count());
        userPairHashtagTotalRDD.foreach(pair ->{
            System.out.println("*******userPairHashtagTotalRDD user pair : " + pair._1() + " hashtag and count : " + pair._2());
        });*/


        // 5. calculate same tag count for all user pairs over same hashtag
        //(<user1,user2>,<samehashtagcount>)
        JavaPairRDD<String, Integer> userPairSameTagCountRDD = userPairHashtagTotalRDD.reduceByKey(
                (Function2<Integer, Integer, Integer>) (Integer hashtagCount1, Integer hashtagCount2) -> {
                    //System.out.println("****in reduce by key for same user pair hashtagCount1:" + hashtagCount1);
                    //System.out.println("****in reduce by key for same user pair hashtagCount2:" + hashtagCount2);
                    return (hashtagCount1 + hashtagCount2);
                });

        /*
        System.out.println("**************usameTagCountRDD  count= " + userPairSameTagCountRDD.count());
        userPairSameTagCountRDD.foreach(pair ->{
            System.out.println("*******sameTagCountRDD user pair : " + pair._1() + " same tag count : " + pair._2());
        });*/


        // 6. Calculate hashtag score for all user pairs
        // (<user1,user2>,<hashtagscore>)
        JavaPairRDD<String, String> userPairHashTagScoreRDD = userPairSameTagCountRDD.mapToPair(pair -> {
            int count = pair._2();
            double hashtagScore = 1;
            if (count > 10) {
                hashtagScore = 1 + Math.log(1 + count - 10);
            }
            return new Tuple2<>(pair._1(), Double.toString(hashtagScore));
        });

        /*
        System.out.println("**************userPairHashTagScoreRDD  count= " + userPairHashTagScoreRDD.count());
        userPairHashTagScoreRDD.foreach(pair ->{
            System.out.println("*******userPairHashTagScoreRDD user pair : " + pair._1() + " hashtag score : " + pair._2());
        });*/


        // 7. convert to flat map - user1,user2 user2,user1
        userPairHashTagScoreRDD = userPairHashTagScoreRDD.flatMapToPair(pair -> {
            String[] users = pair._1().split(",");
            String userCombination1 = users[0] + "," + users[1];
            String userCombination2 = users[1] + "," + users[0];
            List<Tuple2<String, String>> entries = new ArrayList<>();
            entries.add(new Tuple2<>(userCombination1, pair._2()));
            entries.add(new Tuple2<>(userCombination2, pair._2()));
            return entries.iterator();
        });
        /*
        System.out.println("**************userPairHashTagScoreRDD combo count= " + userPairHashTagScoreRDD.count());
        userPairHashTagScoreRDD.foreach(pair -> {
            System.out.println("*******userPairHashTagScoreRDD combo user pair : " + pair._1() + " hashtag score : " + pair._2());
        });*/


//=============================================================================================================
        //2. create RDD of (<user_id>, <user_screen_name, user_description, created_at_tweet) by reading 1 tweet

        JavaPairRDD<String, String> userInfoRDD = pairsRDD.flatMapToPair(pair -> {
            //System.out.println("pair 2 is " + pair._2());
            String[] columns = pair._2().split(DELIMITER);
            ArrayList<Tuple2<String, String>> entries = new ArrayList<>();
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
            if (!repliedUserId.equals("null")) {
                StringJoiner joinerRepliedUser = new StringJoiner("{TEAMLET'SDOIT!JOINER}");
                joinerRepliedUser.add("null");
                joinerRepliedUser.add("null");
                joinerRepliedUser.add(createdAt);
                entries.add(new Tuple2<>(repliedUserId, joinerRepliedUser.toString()));
            }

            String retweetedUserId = columns[RETWEET_USER_ID];
            if (!retweetedUserId.equals("null")) {
                String retweetedUserScreenName = columns[RETWEET_USER_SCREEN_NAME];
                String retweetedUserDescription = columns[RETWEET_USER_DESCRIPTION];
                StringJoiner joinerRetweetedUser = new StringJoiner("{TEAMLET'SDOIT!JOINER}");
                joinerRetweetedUser.add(retweetedUserScreenName);
                joinerRetweetedUser.add(retweetedUserDescription);
                joinerRetweetedUser.add(createdAt);
                entries.add(new Tuple2<>(retweetedUserId, joinerRetweetedUser.toString()));
            }

            return entries.iterator();
        });
        /*
        System.out.println("**************user latest info count= " + userInfoRDD.count());
        userInfoRDD.foreach(pair -> {
            System.out.println("*******user latest info 1 user id : " + pair._1() + " value : " + pair._2());
        });*/

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
        /*
        System.out.println("**************user latest info all tweet count= " + userInfoRDD.count());
        userInfoRDD.foreach(pair -> {
            System.out.println("*******user latest info all user id : " + pair._1() + " value : " + pair._2());
        });

         */

        //4. mapToPair - remove created at
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
        /*
        System.out.println("**************user latest info only needed count= " + userLatestInfoRDD.count());
        userLatestInfoRDD.foreach(pair -> {
            System.out.println("*******user latest info only needed id : " + pair._1() + " value : " + pair._2());
        });*/
//===========================================================================================================

        //Join interaction score and hashtag score
        JavaPairRDD<String, Tuple2<Optional<String>, Optional<String>>> ScoresRDD =
                userPairHashTagScoreRDD.fullOuterJoin(interactingUsersComboRDD);

        /*
        System.out.println("**************ScoresRDD count= " + ScoresRDD.count());
        ScoresRDD.foreach(pair -> {
            System.out.println("*******ScoresRDD id : " + pair._1() + " detail : " + pair._2());
        });*/

        //<user1><,user2,hashtag score, interaction score, ...>
        JavaPairRDD<String, String> userScoresRDD = ScoresRDD.mapToPair(pair -> {
            String[] usersString = pair._1().split(",");
            String user1 = usersString[0];
            String user2 = usersString[1];
            Tuple2<Optional<String>, Optional<String>> scores = pair._2();
            String value;
            //default hashtag score = 1
            if (scores._1().toString().equals("Optional.empty"))
                value = user2 + "{TEAMLET'SDOIT!JOINER}" + "1" + "{TEAMLET'SDOIT!JOINER}" + scores._2().get();
            //default interaction score = 0
            else if (scores._2().toString().equals("Optional.empty"))
                value = user2 + "{TEAMLET'SDOIT!JOINER}" + scores._1().get() + "{TEAMLET'SDOIT!JOINER}" + "0" +
                        "{TEAMLET'SDOIT!JOINER}" + "null" + "{TEAMLET'SDOIT!JOINER}" + "null" +
                        "{TEAMLET'SDOIT!JOINER}" + "null" + "{TEAMLET'SDOIT!JOINER}" + "null" +
                        "{TEAMLET'SDOIT!JOINER}" + "null";
            else
                value = user2 + "{TEAMLET'SDOIT!JOINER}" + scores._1().get() + "{TEAMLET'SDOIT!JOINER}" + scores._2().get();
            return new Tuple2<>(user1, value);
        });
        /*
        System.out.println("**************user1 and user2 and score string= " + userScoresRDD.count());
        userScoresRDD.foreach(pair -> {
            System.out.println("*******user1 and user2 and score   id : " + pair._1() + " detail : " + pair._2());
        });*/

        //Join with the users info to get name and description for user1
        //<user1>,(<,user2,hashtag score, interaction score, ...>,<user1 name, user1 desc>)
        JavaPairRDD<String, Tuple2<String, String>> user1AllDetailsRDD = userScoresRDD.join(userLatestInfoRDD);
        /*
        System.out.println("**************user1AllDetailsRDD= " + user1AllDetailsRDD.count());
        user1AllDetailsRDD.foreach(pair -> {
            System.out.println("*******user1AllDetailsRDD user1 : " + pair._1() + " detail : " + pair._2());
        });*/

        //<user1,user2,hashtag score, interaction score, ...,user1 name, user1 desc>
        JavaRDD<String> dbRDD = user1AllDetailsRDD.map(pair -> {
            Tuple2<String, String> value = pair._2();
            String part1 = value._1();
            String part2 = value._2();
            String newValue = pair._1() + "{TEAMLET'SDOIT!JOINER}" + part1 + "{TEAMLET'SDOIT!JOINER}" + part2;
            return newValue;
        });

        System.out.println("**************dbRDD= " + dbRDD.count());
        for (String line : dbRDD.collect())
            System.out.println(line);

//=====================================================================================================

        /*
        String schemaString = "user1 user2 hashtagscore interactionscore replyTexts replyHashtags " +
                "retweetTexts retweetHashtags lastestContactTweet user1Name user1Description";

        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = dbRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String record) throws Exception {
                String[] attributes = record.split(DELIMITER);
                return RowFactory.create(attributes[0],attributes[1],attributes[2],attributes[3],attributes[4],
                        attributes[5],attributes[6],attributes[7],attributes[8],attributes[9],attributes[10]);
            }
        });



        // Apply the schema to the RDD
        SQLContext sqlContext = new SQLContext(sc);
        Dataset<Row> rowDataFrame = sqlContext.createDataFrame(rowRDD, schema);
        */

        /*
        System.out.println("Size of row data frame is : " + rowDataFrame.count());
        rowDataFrame.printSchema();
        rowDataFrame.show();

         */


        /*
        String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        String DB_NAME = "query2";
        String DB_HOSTNAME = "";
        String URL = "jdbc:mysql://"+DB_HOSTNAME+":3306/query2";
        */

        /*
        // Saving data to a JDBC source
        rowDataFrame.write()
                .format("jdbc")
                .option("url", "jdbc:mysql:///query2")
                .option("dbtable", "q2")
                .option("user", "")
                .option("password", "")
                .option("createTableOptions","CHARACTER SET utf8")
                //.option("driver", "com.mysql.jdbc.Driver")
                .save();

         */

        /*jdbcDF2.write()
                .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);*/

        // Specifying create table column data types on write
        //rowDataFrame.write()
        //        .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
        //        .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties);
        sc.stop();
    }


}

