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

val shortLinkRegex = "(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*"

val stopWordsLink : String = "gs://cc-final/stopwords.txt";
val stopWordsRDD = sc.textFile(stopWordsLink);
val stopWordsHashSet : HashSet[String] = HashSet[String](stopWordsRDD.collect():_*);
val stopWordsHashSetBroadcast = sc.broadcast(stopWordsHashSet)

val bannedWords = "gs://cc-final/banned_words.txt"
val bannedWordsRDD = sc.textFile(bannedWords)
val bannedWordsHashSet : HashSet[String] = HashSet[String](bannedWordsRDD.collect():_*);
val bannedWordsHashSetBroadcast = sc.broadcast(bannedWordsHashSet);

def hex2string(hex: String): String = {
        new String( hex.sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte), "UTF-8" )
}

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
    val words = tweet.toLowerCase().split("[^a-zA-Z0-9'-]").toList
     words.filter(x => x.matches(".*[a-zA-Z]+.*"))
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

val checkCorrectness = (x : String) => {

    val rawData = x.split("\t");
    val tweetID : String = rawData(TWEET_ID.value);
    val tweetDataHex = rawData(1).replaceAll(" ", "");
    val utf8Value = hex2string(tweetDataHex);

    val tweetData : Array[String] = utf8Value.split(INPUT_COLUMN_DELIMITER.value);

    val tweetText = tweetData(TWEET_TEXT.value)
    val tweetTextSanitized = sanitizeTweet(tweetText)
    val tweetTextNoLinks : String = removeShortLinks(tweetText);
    val author : String = tweetData(AUTHOR.value);
    val ts : Long = tweetData(TIMESTAMP.value).toLong;
    val followersCount : Int = tweetData(FOLLOWERS_COUNT.value).toInt
    val retweetCount : Int = tweetData(RETWEET_COUNT.value).toInt
    val favoriteCount : Int = tweetData(FAVORITE_COUNT.value).toInt

    val words : List[String] = splitTweetIntoWords(tweetTextNoLinks);


    val effectiveWords = words.filter(word => !stopWordsHashSetBroadcast.value.contains(word) );
    var ewc : Int = effectiveWords.length;

    var impactScore : Int = ewc * (followersCount + retweetCount + favoriteCount);
    if (impactScore < 0)
        impactScore = 0;

    prepareOutputForReference(tweetID, ts.toString(), author, tweetTextSanitized, impactScore.toString())

} : String

val extractTweetData = (x : String) => {

    val rawData = x.split("\t");
    val tweetID : String = rawData(TWEET_ID.value);
    val tweetDataHex = rawData(1).replaceAll(" ", "");
    val utf8Value = hex2string(tweetDataHex);


    val tweetData : Array[String] = utf8Value.split(INPUT_COLUMN_DELIMITER.value);

    val tweetText = tweetData(TWEET_TEXT.value)
    val tweetTextNoLinks : String = removeShortLinks(tweetText);
    val author : String = tweetData(AUTHOR.value);
    val ts : Long = tweetData(TIMESTAMP.value).toLong;
    val followersCount : Int = tweetData(FOLLOWERS_COUNT.value).toInt
    val retweetCount : Int = tweetData(RETWEET_COUNT.value).toInt
    val favoriteCount : Int = tweetData(FAVORITE_COUNT.value).toInt

    val words : List[String] = splitTweetIntoWords(tweetTextNoLinks);


    val effectiveWords = words.filter(word => !stopWordsHashSetBroadcast.value.contains(word) );
    var ewc : Int = effectiveWords.length;

    var impactScore : Int = ewc * (followersCount + retweetCount + favoriteCount);
    if (impactScore < 0)
        impactScore = 0;

    val output = (tweetID, impactScore, author, tweetText, words.length, ts);
    output

}

val inputPath = "gs://cc-final/";
val outputPath = "gs://cc-final/output_small"
val tweetsRDD = sc.textFile(inputPath).map(extractTweetData)

val modifiedTweetsRDD = tweetsRDD.map{
    case ( (tweetID, impactScore, author, tweetText, totalWordCount, ts) ) => {
            val output = new StringJoiner(OUTPUT_COLUMN_DELIMITER.value);

            output.add(tweetID);
            output.add(impactScore.toString());
            output.add(author);
            output.add(tweetText);
            output.add(totalWordCount.toString());
            output.add( (ts / 1000L).toString() );

            output.toString() + LINE_DELIMITER.value
        }

}


val startTs = System.currentTimeMillis()/1000L;
modifiedTweetsRDD.saveAsTextFile(outputPath)
val endTs = System.currentTimeMillis()/1000L;
println("Took " + (endTs - startTs) + " seconds")
