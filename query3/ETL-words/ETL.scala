//This code was written on Zeppelin notebook
import java.util.Locale
import java.util.StringJoiner
import scala.collection.immutable.HashSet
import scala.math._

val TWEET_ID = sc.broadcast(0)
val TIMESTAMP = sc.broadcast(0)
val TWEET_TEXT = sc.broadcast(1)
val AUTHOR = sc.broadcast(2)

val FOLLOWERS_COUNT = sc.broadcast(5)
val FRIENDS_COUNT = sc.broadcast(6)
val FAVORITE_COUNT = sc.broadcast(7)
val RETWEET_COUNT = sc.broadcast(13)

val INPUT_COLUMN_DELIMITER = sc.broadcast("\\{TEAMLET'SDOIT!JOINER\\}")
val OUTPUT_COLUMN_DELIMITER = sc.broadcast("\t")
val LINE_DELIMITER = sc.broadcast("Q\t\t\tQ")

val stopWordsLink : String = "gs://cc_final_temp_bucket/stopwords.txt";
val stopWordsRDD = sc.textFile(stopWordsLink);
val stopWordsHashSet : HashSet[String] = HashSet[String](stopWordsRDD.collect():_*);
val stopWordsHashSetBroadcast = sc.broadcast(stopWordsHashSet)

val bannedWords = "gs://cc_final_temp_bucket/banned_words.txt"
val bannedWordsRDD = sc.textFile(bannedWords)
val bannedWordsHashSet : HashSet[String] = HashSet[String](bannedWordsRDD.collect():_*);
val bannedWordsHashSetBroadcast = sc.broadcast(bannedWordsHashSet);

def hex2string(hex: String): String = {
        new String( hex.sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte), "UTF-8" )
    }

    
//Function for removing short links
val shortLinkRegex = "(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*"
val removeShortLinks = (tweet : String) => {
    tweet.replaceAll(shortLinkRegex, "") : String
}

val sanitizeWord = (word : String) => {
    val output : StringBuilder = new StringBuilder(word);
    
    for (i <- 1 until word.length - 1)
        output.setCharAt(i, '*');
        
    output.toString()
} : String

val sanitizeTweet = (tweet : String) => {
    val regex = "((?<=[^a-zA-Z0-9])|(?=[^a-zA-Z0-9]))"; //Split by non-alphanumeric characters, preserve delimiters
    val words = tweet.split(regex);
    
    var i : Int = 0;
    for (i <- 0 until words.length) {
        
        if (!words(i).matches("\\W") && bannedWordsHashSetBroadcast.value.contains(words(i).toLowerCase() ) ) {
            words(i) = sanitizeWord(words(i))
        }
    }
    
    words.mkString("")
} : String

//Split tweet into words
val splitTweetIntoWords = (tweet : String) => {
    val words = tweet.split("[^a-zA-Z0-9'-]").toList
    words.filter(x => x.matches(".*[a-zA-Z]+.*")).map(x => x.toLowerCase())
    //words.filter(x => x.toLowerCase().matches(".*[a-zA-Z]+.*"))
} : List[String]


val prepareOutputForReference = (tweet_id : String, created_at : String, user_id : String, censored_text : String, impact_score : String) => {
    "{" +
    "\"tweet_id\":" +"\"" + tweet_id + "\"" + "," +
    "\"created_at\":" +"\"" + created_at.substring(0, created_at.length() - 3) + "\"" + "," +
    "\"user_id\":" +"\"" + user_id + "\"" + "," +
    "\"censored_text\":" +"\"" + censored_text + "\"" + "," +
    "\"impactScore\":" + "\"" + impact_score + "\"" +
    "}"
} : String

val checkCorrectness = (x: String) => {
    val rawData = x.split("\t");
    val tweetID : String = rawData(TWEET_ID.value);
    val tweetDataHex = rawData(1).replaceAll(" ", "");
    val utf8Value = hex2string(tweetDataHex);

    val tweetData : Array[String] = utf8Value.split(INPUT_COLUMN_DELIMITER.value);
    
    val tweetText = tweetData(TWEET_TEXT.value)
    val tweetTextSanitized = sanitizeTweet(tweetText)
    val tweetTextNoLinks : String = removeShortLinks(tweetText);
    val author : String = tweetData(AUTHOR.value);
    val ts : String = tweetData(TIMESTAMP.value);
    val followersCount : Int = tweetData(FOLLOWERS_COUNT.value).toInt
    val retweetCount : Int = tweetData(RETWEET_COUNT.value).toInt
    val favoriteCount : Int = tweetData(FAVORITE_COUNT.value).toInt
    
    val words : List[String] = splitTweetIntoWords(tweetTextNoLinks);
    
    var ewc : Int = 0;
    
    for (i <- 0 until words.length) {
        if (!stopWordsHashSetBroadcast.value.contains(words(i)) )
            ewc = ewc + 1;
    }
    
    val impactScore : Int = ewc * (followersCount + retweetCount + favoriteCount);
    
    prepareOutputForReference(tweetID, ts, author, tweetTextSanitized, impactScore.toString())
}

val extractTweetData = (x : String) => {
    
    val rawData = x.split("\t");
    val tweetID : String = rawData(TWEET_ID.value);
    val tweetDataHex = rawData(1).replaceAll(" ", "");
    val utf8Value = hex2string(tweetDataHex);

    val tweetData : Array[String] = utf8Value.split(INPUT_COLUMN_DELIMITER.value);
    
    val tweetText = tweetData(TWEET_TEXT.value)
    val tweetTextSanitized = sanitizeTweet(tweetText)
    val tweetTextNoLinks : String = removeShortLinks(tweetText);
    val author : String = tweetData(AUTHOR.value);
    val ts : Long = tweetData(TIMESTAMP.value).toLong / 1000L;
    val followersCount : Int = tweetData(FOLLOWERS_COUNT.value).toInt
    val retweetCount : Int = tweetData(RETWEET_COUNT.value).toInt
    val favoriteCount : Int = tweetData(FAVORITE_COUNT.value).toInt
    
    /*
    val splitTweet : List[String] = splitTweetIntoWords(tweetTextNoLinks);
    val words : List[String] =  if (!splitTweet.isEmpty)
                                    splitTweet
                                else
                                    List[String]("")
    */
    val words : List[String] = splitTweetIntoWords(tweetTextNoLinks);
    
    val effectiveWords = words.filter(word => !stopWordsHashSetBroadcast.value.contains(word) );
    
    
    
    var ewc : Int = effectiveWords.length;
    
    var impactScore : Int = ewc * (followersCount + retweetCount + favoriteCount);
    if (impactScore < 0)
        impactScore = 0;
        
    val impactScoreLog1p : Double = log1p(impactScore.toDouble);

        
    //val wordsWithCount = effectiveWords.groupBy(x=>x).map(x => (x._1, x._2.length));
    val wordsWithCount = words.groupBy(x=>x).map(x => (x._1, x._2.length));

    val output = wordsWithCount.map{ case (word, count) => {
        
            val isStop = stopWordsHashSetBroadcast.value.contains(word);
        
            val wordSanitized =
                if (!isStop && bannedWordsHashSetBroadcast.value.contains(word) )
                    sanitizeWord(word)
                else
                    word
        
            (
                (word, tweetID),
                (tweetText, author, impactScore, impactScoreLog1p, count, words.length, count / words.length.toDouble, wordSanitized, tweetTextSanitized, isStop, ts)
            )
        }
    };

    if (!output.isEmpty)
        output
    else
        List((
            ("", tweetID),
            (tweetText, author, impactScore, impactScoreLog1p, 0, words.length, 0.0, "", tweetTextSanitized, true, ts)
        )).toMap
}


val inputPath = "gs://cc_final_temp_bucket/part-r-00000.txt";
//val inputPath = "gs://cc_final_temp_bucket/input/"
//val inputPath = "gs://q3-etl-html-decoding-in-valid-tweets-map-reduce-file0/part-r-00000";
val outputPath = "gs://cc_final_temp_bucket/output"
val tweetsRDD = sc.textFile(inputPath).flatMap(extractTweetData)


val modifiedTweetsRDD = tweetsRDD.map{
    case (
            (word, tweetID),
            (tweetText, author, impactScore, impactScoreLog1p, thisWordCount, totalWordCount, termFreq, wordSanitized, tweetTextSanitized, isStop, ts)
        ) => {
            val output = new StringJoiner(OUTPUT_COLUMN_DELIMITER.value);
            
            output.add(tweetID);
            output.add(word);
            output.add(tweetText);
            output.add(author);
            output.add(impactScore.toString());
            output.add(impactScoreLog1p.toString());
            output.add(thisWordCount.toString());
            output.add(totalWordCount.toString());
            output.add(termFreq.toString());
            output.add(wordSanitized);
            output.add(tweetTextSanitized);
            output.add(if (isStop) "1" else "0")
            output.add(ts.toString());
            
            output.toString() + LINE_DELIMITER.value
        }
    
}


val startTs = System.currentTimeMillis()/1000L;
modifiedTweetsRDD.saveAsTextFile(outputPath)
val endTs = System.currentTimeMillis()/1000L;
println("Took " + (endTs - startTs) + " seconds")
