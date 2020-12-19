package edu.cmu.scs.cc.teamprojectQ2ETL;

import java.util.ArrayList;

public class Tweet {
	String created_at;
	String id_str;
	String text;
	String in_reply_to_user_id_str;
	User user;
	Tweet retweeted_status;
	ArrayList<String> hashtags;
	
	public Tweet() {
		
	}
	
	public Tweet(String created_at, Long id, String id_str, String text, Long in_reply_to_user_id,
	String in_reply_to_user_id_str, User user, Tweet retweeted_status, int reply_count,
	int retweet_count, ArrayList<String> hashtags, String lang) {
		this.created_at = created_at;
		this.id_str = id_str;
		this.text = text;
		this.in_reply_to_user_id_str = in_reply_to_user_id_str;
		this.user = user;
		this.retweeted_status = retweeted_status;
		this.hashtags = hashtags;
	}
	
}
