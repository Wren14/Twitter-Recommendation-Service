package edu.cmu.cc.group.q3;

import java.io.*;
import java.sql.SQLException;
import java.util.*;

import edu.cmu.cc.group.ClientSQL;

public class DriverQ3 {

	private final static String TEAM_ID = "Let'sDoIt!";
	private final static String TEAM_AWS_ACCOUNT_ID = "586374175774";
	
	public final static HashSet<String> bannedWords = BannedWords.getBannedWords();
	public final static HashSet<String> stopWords = StopWords.getStoppedWords();
	
	//Handle query
	public String handleQ3(ClientSQL client, String timeStartStr, String timeEndStr, String uidStartStr, String uidEndStr, String n1Str,
			String n2Str) throws SQLException {
		
		//If an input parameters has been omitted, it is going to be null
		//Input is not valid in this case
		if (timeStartStr == null || timeEndStr == null || uidStartStr == null || uidEndStr == null || n1Str == null || n2Str == null)
			return TEAM_ID + "," + TEAM_AWS_ACCOUNT_ID;
		
		long timeStart, timeEnd, uidStart, uidEnd;
		int n1, n2;
		
		try {
			timeStart = Long.parseLong(timeStartStr);
			timeEnd = Long.parseLong(timeEndStr);
			uidStart = Long.parseLong(uidStartStr);
			uidEnd = Long.parseLong(uidEndStr);
			n1 = Integer.parseInt(n1Str);
			n2 = Integer.parseInt(n2Str);
		} catch(NumberFormatException e) {
			return TEAM_ID + "," + TEAM_AWS_ACCOUNT_ID;
		}
		
		//If time or author intervals are not valid, do not proceed with the request
		if (timeStart > timeEnd || uidStart > uidEnd)
			return TEAM_ID + "," + TEAM_AWS_ACCOUNT_ID;
		
		//Get data from DB
		List<Record> data = client.getData(timeStart, timeEnd, uidStart, uidEnd);
		
		//Map each words to all the tweets where thsi word is used
		HashMap<String, Map<Long, Tweet>> wordTweetMap = new HashMap<>();

		//Priority queue for all the words
		PriorityQueue<TopicWord> topicWordsPQ = new PriorityQueue<>(new Comparator<TopicWord> () {

			@Override
			public int compare(TopicWord o1, TopicWord o2) {
				int topicScoreComparison = Double.compare(o2.getTopicScore(), o1.getTopicScore());
				if (topicScoreComparison == 0)
					return o1.getWord().compareTo(o2.getWord());
				else
					return topicScoreComparison;
			}

		});
		
		processData(data, wordTweetMap, topicWordsPQ);
		
		List<String> topicWords = new ArrayList<>(n1);
		List<String> impactfulTweets = new ArrayList<>(n2);
		
		extractTopicWords(wordTweetMap, topicWordsPQ, topicWords, impactfulTweets, n1, n2);
		
		String response = prepareResponse(topicWords, impactfulTweets);
		
		return response;
	}

	
	private String prepareResponse(List<String> topicWords, List<String> impactfulTweets) {
		StringJoiner topicWordsOutput = new StringJoiner("\t");
		
		for (String topicWord : topicWords)
			topicWordsOutput.add(topicWord);
		
		StringJoiner impactfulTweetsOutput = new StringJoiner("\n");
		
		for (String tweet : impactfulTweets)
			impactfulTweetsOutput.add(tweet);
		
		StringBuilder output = new StringBuilder(TEAM_ID + "," + TEAM_AWS_ACCOUNT_ID);
		output.append("\n");
		output.append(topicWordsOutput.toString());
		output.append("\n");
		output.append(impactfulTweetsOutput.toString());
		
		return output.toString();
	}
	
	
	/*
	private String prepareResponse(List<String> topicWords, List<String> impactfulTweets) {
		
		StringBuilder topicWordsOutput = new StringBuilder();
		
		for (int i = 0; i < topicWords.size() - 1; i++) {
			topicWordsOutput.append(topicWords.get(i));
			topicWordsOutput.append("\t");
		}
		
		topicWordsOutput.append(topicWords.get(topicWords.size() - 1));
		
		StringBuilder impactfulTweetsOutput = new StringBuilder();
		
		for (int i = 0; i < impactfulTweets.size() - 1; i++) {
			impactfulTweetsOutput.append(impactfulTweets.get(i));
			impactfulTweetsOutput.append("\n");
		}
		
		impactfulTweetsOutput.append(impactfulTweets.get(impactfulTweets.size() - 1));
		
		StringBuilder output = new StringBuilder(topicWordsOutput.length() + impactfulTweetsOutput.length() + 50);
		output.append(TEAM_ID + "," + TEAM_AWS_ACCOUNT_ID);
		output.append("\n");
		output.append(topicWordsOutput.toString());
		output.append("\n");
		output.append(impactfulTweetsOutput.toString());
		
		return output.toString();
	}
	*/

	private void processData(List<Record> data, HashMap<String, Map<Long, Tweet>> wordTweetMap, PriorityQueue<TopicWord> topicWordsPQ) {
		
		final int numTweetsInRange = data.size();
		
		groupWords(data, wordTweetMap);
		calculateTopicScores(numTweetsInRange, wordTweetMap, topicWordsPQ);
	}
	
	private void extractTopicWords(HashMap<String, Map<Long, Tweet>> wordTweetMap,
			PriorityQueue<TopicWord> topicWordsPQ,
			List<String> topicWords,
			List<String> impactfulTweets, int n1, int n2) {

		TreeSet<Tweet> tweets = new TreeSet<>(new Comparator<Tweet> () {

			@Override
			public int compare(Tweet o1, Tweet o2) {
				int impactScoreComparison = Integer.compare(o2.getImpactScore(), o1.getImpactScore());
				
				if (impactScoreComparison == 0)
					return Long.compare(o2.getTweetID(), o1.getTweetID());
				else
					return impactScoreComparison;
			}
			
		});

		//Get top-n1 topic words
		final int pqSize = topicWordsPQ.size();
		for (int i = 0; i < pqSize && i < n1; i++) {
			TopicWord topicWord = topicWordsPQ.remove();
			String word = topicWord.getWord();
			String sanitizedWord = Sanitizer.sanitizeText(word);

			topicWords.add(String.format("%s:%.2f", sanitizedWord, topicWord.getTopicScore() ) );

			Map<Long, Tweet> allTweets = wordTweetMap.get(word);

			tweets.addAll(allTweets.values());
		}

		//Get top-n2 most impactful tweets containing topic words
		int numTweetsRetrieved = 0;
		for (Iterator<Tweet> it = tweets.iterator(); it.hasNext() && numTweetsRetrieved < n2; numTweetsRetrieved++) {
			Tweet tweet = it.next();
			
			String sanitizedTweetText = Sanitizer.sanitizeText(tweet.getTweetText());
			
			impactfulTweets.add(String.format("%d\t%d\t%s", tweet.getImpactScore(), tweet.getTweetID(), sanitizedTweetText ) );
		}

	}

	private void groupWords(List<Record> data, HashMap<String, Map<Long, Tweet>> wordTweetMap) {
		
		for (Record record : data) {
			String tweetText = record.getTweetText();
			
			String tweetTextNoURLs = tweetText.replaceAll("(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*", "");
			
		    String[] splitTweet = tweetTextNoURLs.toLowerCase().split("[^a-zA-Z0-9'-]");
		    
		    //Find out which words are in the tweet
		    for (String potentialWord : splitTweet) {

		    	//Word needs to contain a letter
		    	if (potentialWord.isEmpty() || !potentialWord.matches(".*[a-zA-Z]+.*") || stopWords.contains(potentialWord))
		    		continue;
		    	
		    	//Put this word into the map
		    	updateMap(potentialWord, record.getTweetId(), tweetText,
						record.getImpactScore(), record.getWordCount(), wordTweetMap);
		    }

		}
	
	}
		
	private void calculateTopicScores(final int numTweetsInRange, HashMap<String, Map<Long, Tweet>> wordTweetMap, PriorityQueue<TopicWord> topicWordsPQ) {
		
		for (Map.Entry<String, Map<Long, Tweet>> entry : wordTweetMap.entrySet() ) {
			
			String word = entry.getKey();
			
			//Tweets where this word has been encountered
			Map<Long, Tweet> encounteredTweets = entry.getValue();
			
			double topicScore = calculateTopicScore(encounteredTweets, numTweetsInRange);
			TopicWord topicWord = new TopicWord(word, topicScore);
			topicWordsPQ.add(topicWord);
		}
		
	}
	
	
	private double calculateTopicScore(Map<Long, Tweet> encounteredTweets, final int numTweetsInRange) {
		
		final int numTweetsWithWord = encounteredTweets.size();
		
		//Inverse document frequency
		final double IDF = Math.log((double) numTweetsInRange / numTweetsWithWord);
		
		//Topic score is going to be zero if IDF is zero
		if (IDF == 0.0)
			return 0.0;
		
		double topicScore = 0.0;
		
		for (Tweet tweet : encounteredTweets.values()) {
			
			//Number of times this word was encountered in the tweet
			int numTimesWordused = tweet.getNumTimesWordUsed();
			int wordCount = tweet.getTotalWordCount();
			
			//Term frequency
			double TF = (double) numTimesWordused / wordCount;
			
			double impactScoreLog1p = tweet.getImpactScoreLog1p();
			
			topicScore += TF * impactScoreLog1p;
			
		}
		
		topicScore *= IDF;
		
		return topicScore;
		
	}
	

	private void updateMap(String word,
							long tweetID,
							String tweetText,
							int impactScore,
							int totalWordCount,
							HashMap<String, Map<Long, Tweet>> wordTweetMap) {
		
		Map<Long, Tweet> tweetMap = wordTweetMap.getOrDefault(word, new HashMap<>() );
		
		if (tweetMap.containsKey(tweetID)) {
			//This word has already been encountered in this tweet
			Tweet tweet = tweetMap.get(tweetID);
			tweet.incrementNumTimesWordUsed();
		} else {
			Tweet tweet = new Tweet(tweetID, tweetText, impactScore, totalWordCount);
			tweetMap.put(tweetID, tweet);
		}
		
		if (!wordTweetMap.containsKey(word))
			wordTweetMap.put(word, tweetMap);
		
		
	}
		
	
	
}
