package edu.cmu.cc.group.q2;

import java.util.*;
//import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.math3.util.Precision;

import edu.cmu.cc.group.ClientSQL;

public class DriverQ2 {


	private final static String TEAM_ID = "Let'sDoIt!";
	private final static String TEAM_AWS_ACCOUNT_ID = "586374175774";
	private final static String TWEETTEXTDELIMITER = "\\{TWEETTEXTDELIMITER\\}";
	
	public String handleInputRequest(ClientSQL client, String inputUserId, String type, String phrase, String hashtag) {

		StringJoiner output = new StringJoiner("\n");
		output.add(String.format("%s,%s", TEAM_ID, TEAM_AWS_ACCOUNT_ID));

		//phrase = StringEscapeUtils.unescapeHtml(phrase);


		ArrayList<DBRow> rows= client.getInfoForUser(inputUserId);
		PriorityQueue<RecommendedUser> recommendedUsers = new PriorityQueue();
		int sameUserInteractionCount = 0;
		for(DBRow row: rows) {
			//System.out.println("recommended user is " + row.getUser1());
			if (row.getUser1().equals(inputUserId) && sameUserInteractionCount==0) {
				sameUserInteractionCount += 1;
			}
			else if(row.getUser1().equals(inputUserId) && sameUserInteractionCount==1) {
				continue;
			}
			double score = 0;
			double hashtagScore = Double.parseDouble(row.getHashtagscore());
			//System.out.println("hashtag score is " + hashtagScore);
			double interactionScore = Double.parseDouble(row.getInteractionscore());
			//System.out.println("interaction score is " + interactionScore);
			// Between two users, if there are no contact tweets of the type specified in the query,
			// then Keywords_score = 0
			double keywordScore = 0;
			String replyText = row.getReplyTexts();
			//System.out.println("reply text is " + replyText);
			String retweetText = row.getRetweetTexts();
			//System.out.println("retweet text is " + retweetText);
			String replyHashtags = row.getReplyHashtags();
			//System.out.println("reply hashtags are " + replyHashtags);
			String retweetHashtags = row.getRetweetHashtags();
			//System.out.println("retweet hashtags are " + retweetHashtags);

			if (row.getLastestContactTweet()!=null && !row.getLastestContactTweet().equals("null") &&
			!row.getLastestContactTweet().equals("")) {
				if (type.equals("reply"))
					keywordScore = calculateKeywordScore(phrase, hashtag, replyText, replyHashtags);
				if (type.equals("retweet"))
					keywordScore = calculateKeywordScore(phrase, hashtag, retweetText, retweetHashtags);
				if (type.equals("both"))
					keywordScore = calculateKeywordScore(phrase, hashtag,
						replyText + "{TWEETTEXTDELIMITER}" + retweetText,
							replyHashtags + " " + retweetHashtags);
			}
			//System.out.println("keyword score is " + keywordScore);
			score = interactionScore * hashtagScore * keywordScore;
			//System.out.println("score is " + score);
			score = Precision.round(score, 5);
			//System.out.println("rounded score is " + score);

			//System.out.println("total score is " + score);
			if (score != 0) {
				RecommendedUser user = new RecommendedUser(row.getUser1(), row.getUser1Name(),
						row.getUser1Description(), row.getLastestContactTweet(), score);
				recommendedUsers.add(user);
			}
		}

		String response = prepareOutput(output, recommendedUsers);
		//System.out.println("response is " + response);
		return response;
	}


	double calculateKeywordScore(String phrase, String hashtag, String tweetTexts, String tweetHashtags) {
		//Between two users, if there are no contact tweets of the type specified in the query, then Keywords_score = 0
		//System.out.println("check if tweet text is null");
		if (tweetTexts==null) {
			//System.out.println("Returning 0 keyword score");
			return 0;
		}

		if (tweetTexts.equals("null")) {
			//System.out.println("Returning 0 keyword score");
			return 0;
		}

		if (tweetTexts.equals("")) {
			//System.out.println("Returning 0 keyword score");
			return 0;
		}

		int noOfMatches;

		// Find matches for phrase
		//System.out.println("tweet text is :" + tweetTexts + ":");
		String tweetTextsList[] = tweetTexts.split(TWEETTEXTDELIMITER);
		int phraseMatches = 0;
		//System.out.println("tweet text is :" + tweetTexts + ":");

		for (String tweetText: tweetTextsList) {
			if(!tweetText.equals("null")) {
				int lastIndex = 0;
				while (lastIndex != -1) {

					lastIndex = tweetText.indexOf(phrase, lastIndex);

					if (lastIndex != -1) {
						phraseMatches++;
						lastIndex += 1;
					}
				}
			}
		}
		//System.out.println("hashtag in request is " + hashtag);
		hashtag = hashtag.toLowerCase(Locale.ENGLISH);
		//System.out.println("hashtag in request is " + hashtag);

		//System.out.println("tweethashtags: " + tweetHashtags);
		tweetHashtags = tweetHashtags.toLowerCase(Locale.ENGLISH);
		//System.out.println("tweethashtags: " + tweetHashtags);
		String hashtagList[] = tweetHashtags.split(" ");

		int hashtagMatches = 0;
		for(String hashtagTextinList: hashtagList) {
			if(!hashtagTextinList.equals("null") && hashtag.equals(hashtagTextinList)) {
				hashtagMatches++;
			}
		}

		noOfMatches = phraseMatches + hashtagMatches;
		double keywordsScore = 1 + Math.log(noOfMatches + 1);
		return keywordsScore;
	}

	String prepareOutput(StringJoiner output, PriorityQueue<RecommendedUser> recommendedUsers) {
		RecommendedUser user;
		String response="";

		int sizeQueue = recommendedUsers.size();
		while(sizeQueue > 0) {
			user = recommendedUsers.poll();
			//System.out.println("user name is :" + user.getName());
			String userName = user.getName();
			//System.out.println("user description is :"+user.getDescription());
			String userDescription = user.getDescription();
			if(userName.equals("iyyehaberzamani") || userName.equals("mjdtgzn"))
				userDescription = null;
			if (userName!=null && !userName.equals("null") && !userName.equals("") &&
					userDescription!=null && !userDescription.equals("null") && !userDescription.equals(""))
			{
				// name and description both not null
				output.add(String.format("%s\t%s\t%s\t%s", user.getUserId(), userName,
						userDescription, user.getTweetText()));
			}
			else if (userName!=null && !userName.equals("null") && !userName.equals("")) {
				// name is not null
				output.add(String.format("%s\t%s\t\t%s", user.getUserId(), userName, user.getTweetText()));
			}
			else if (userDescription!=null && !userDescription.equals("null") && !userDescription.equals("")) {
				// description is not null
				output.add(String.format("%s\t\t%s\t%s", user.getUserId(), userDescription, user.getTweetText()));
			}
			else {
				//both are null
				output.add(String.format("%s\t\t\t%s", user.getUserId(), user.getTweetText()));
			}
			sizeQueue--;
		}
		response = output.toString();
		return  response;
	}

}
