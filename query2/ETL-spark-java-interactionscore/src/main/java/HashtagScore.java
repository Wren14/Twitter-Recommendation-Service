import org.apache.avro.hadoop.io.AvroKeyValue;
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

public class HashtagScore {
    private final static Charset UTF8 = Charset.forName("UTF-8");
    private final static String DELIMITER = "\\{TEAMLET'SDOIT!JOINER\\}";
    private final static int USER_ID = 2;
    private final static int HASHTAGS = 9;

    static void calculateHashtagScore(String inputPath, String outputPath, Boolean isLocal) {
        System.out.println("*******Hello World Hashtag score");
        SparkConf conf = new SparkConf().setAppName("hashtagScore");
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

        JavaRDD<String> blacklistedHastagsRDD = sc.textFile("popular_hashtags.txt");
        String[] blacklistedHastags = blacklistedHastagsRDD.collect().toArray(new String[0]);
        /*
        for(String tag:blacklistedHastagsRDD.collect()) {
            System.out.println("**************:"+tag);
        };

        String hashtag = "借金あるからギャンブルしてくる";
        System.out.println(hashtag.toLowerCase(Locale.ENGLISH));
        System.out.println(Arrays.stream(blacklistedHastags).anyMatch(hashtag::equals));
        */

        //2. convert to flat map - (<hashtag,user>,<count>) for each tweet
        //JavaPairRDD<String, String> hashtagUserCountRDD = pairsRDD.flatMapToPair(pair -> {
        JavaPairRDD<String, Integer> hashtagUserCountRDD = pairsRDD.flatMapToPair(pair -> {
            //System.out.println("pair 2 is " + pair._2());
            String[] columns = pair._2().split(DELIMITER);
            /*
            for(int i=0; i<columns.length; i++) {
                System.out.println("pair 2 column is " + columns[i]);
            }*/

            String userId = columns[USER_ID];
            String hashtags = columns[HASHTAGS].toLowerCase(Locale.ENGLISH);
            String[] hashtagList = hashtags.split(" ");
            HashMap <String, Integer> counter = new HashMap<String, Integer>();
            // if one hashtag is used twice in 1 tweet

            for(int i=0; i<hashtagList.length; i++) {
                // don't count the blacklisted hashtags
                if(!Arrays.stream(blacklistedHastags).anyMatch(hashtagList[i]::equals))
                    counter.put(hashtagList[i],counter.getOrDefault(hashtagList[i],0)+1);
            }

            ArrayList<Tuple2<String, Integer>> oneTweetHashtagUsersCount = new ArrayList<>();
            Iterator hashtagIterator = counter.entrySet().iterator();
            while(hashtagIterator.hasNext()) {
                Map.Entry<String, Integer> hashtagCount = (Map.Entry)hashtagIterator.next();
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
        });

         */


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
        });

         */

        // 2C. MapToPair (<hashtag1,user1>,<count>) -> (<hashtag1><user1,count>)
        JavaPairRDD<String,String> hashtagAndUserCountRDD = hashtagUserCountRDD.mapToPair(pair -> {
           //String hashtagUser = pair._1();
           String hashtag = pair._1().split(",")[0];
           String user = pair._1().split(",")[1];
           String userCount = user + "," + Integer.toString(pair._2());
           return new Tuple2<>(hashtag,userCount);
        });

        /*
        System.out.println("**************hashtag: 1 hashtag 1 user multiple tweets " + hashtagUserCountRDD.count());
        hashtagUserCountRDD.foreach(pair ->{
            System.out.println("*******hashtag flatmaptopair RDD hashtag : " + pair._1() + " user and count : " + pair._2());
        });

         */

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
        });

         */


        //4. flatMap - for all users - each row is for one hashtag
        //(<user1,user2>,<total-count>
        JavaPairRDD<String, Integer> userPairHashtagTotalRDD = hashtagUserCountAllTweetsRDD.flatMapToPair(pair -> {
           String hashtag = pair._1();
           String userCountList = pair._2();
           String[] userAndCount = userCountList.split(" ");
           //System.out.println("*** hashtag: " + hashtag + " userAndCount length " + userAndCount.length);
           ArrayList<Tuple2<String,Integer>> entries= new ArrayList<>();
           //each user with every user
           for(int i=0; i<userAndCount.length; i++) {
               String user1 = userAndCount[i].split(",")[0];
               String count1 = userAndCount[i].split(",")[1];
               for(int j=i+1; j<userAndCount.length; j++) {
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
        });

         */

        // 5. calculate same tag count for all user pairs over same hashtag
        //(<user1,user2>,<samehashtagcount>)
        JavaPairRDD<String,Integer> userPairSameTagCountRDD = userPairHashtagTotalRDD.reduceByKey(
                (Function2<Integer, Integer, Integer>) (Integer hashtagCount1, Integer hashtagCount2) ->{
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
        JavaPairRDD<String,String> userPairHashTagScoreRDD = userPairSameTagCountRDD.mapToPair(pair ->{
            int count = pair._2();
            double hashtagScore = 1;
            if(count >10) {
                hashtagScore = 1 + Math.log(1 + count - 10);
            }
            return new Tuple2<>(pair._1(), Double.toString(hashtagScore));
        });

        /*
        System.out.println("**************userPairHashTagScoreRDD  count= " + userPairHashTagScoreRDD.count());
        userPairHashTagScoreRDD.foreach(pair ->{
            System.out.println("*******userPairHashTagScoreRDD user pair : " + pair._1() + " hashtag score : " + pair._2());
        });
         */


        // 7. convert to flat map - user1,user2 user2,user1
        userPairHashTagScoreRDD = userPairHashTagScoreRDD.flatMapToPair(pair -> {
            String[] users = pair._1().split(",");
            String userCombination1 = users[0] + "," + users[1];
            String userCombination2 = users[1] + "," + users[0];
            List<Tuple2<String,String>> entries = new ArrayList<>();
            entries.add(new Tuple2<>(userCombination1, pair._2()));
            entries.add(new Tuple2<>(userCombination2, pair._2()));
            return entries.iterator();
        });

        System.out.println("**************userPairHashTagScoreRDD combo count= " + userPairHashTagScoreRDD.count());
        userPairHashTagScoreRDD.foreach(pair ->{
            System.out.println("*******userPairHashTagScoreRDD combo user pair : " + pair._1() + " hashtag score : " + pair._2());
        });


    }
}
