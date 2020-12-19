package edu.cmu.scs.cc.teamprojectQ2ETL;

import java.util.Arrays;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class DataFilter {

	private static final String[] ALLOWED_LANGUAGES = {"ar", "en", "fr", "in", "pt",
			"es", "tr", "ja"};
	
	static boolean checkValidLanguage(JSONObject tweet) {	
		try {
			String language = tweet.getString("lang");
			//System.out.println("language is " + language);
			if (!Arrays.stream(ALLOWED_LANGUAGES).anyMatch(language::equals))
				return false;
			//System.out.println("validated language");
			
			return true;
		} catch(JSONException e) {
			//System.out.println("json exception");
			return false;
		}
	}
	
	static boolean checkValidId(JSONObject tweet) {
		Long id=null;
		String idStr=null;
		try {
			id = tweet.getLong("id");
		} catch(JSONException e) {
			try {
				idStr = tweet.getString("id_str");
			}
			catch (JSONException ex) {
				return false;
			}
		}
		if(id==null && idStr==null) {
			return false;
		}
		//System.out.println("validated id");
		return true;
	}
	
	static boolean checkValidUserId(JSONObject tweet) {
		JSONObject user = tweet.getJSONObject("user");
		Long id=null;
		String idStr=null;
		try {
			id = user.getLong("id");
		} catch(JSONException e) {
			try {
				idStr = user.getString("id_str");
			}
			catch (JSONException ex) {
				return false;
			}
		}
		if(id==null && idStr==null) {
			return false;
		}
		//System.out.println("validated user id");
		return true;		
	}

	static boolean checkCreatedAtNotMissing(JSONObject tweet) {
		try {
			String createdAt = tweet.getString("created_at");
			if (createdAt==null)
				return false;
			//System.out.println("validated created at");
			return true;
		} catch(JSONException e) {
			return false;
		}	
	}
	
	static boolean checkTextNotMissing(JSONObject tweet) {
		try {
			String text = tweet.getString("text");
			if (text==null)
				return false;
			if(text.isEmpty())
				return false;
			//System.out.println("validated text not missing");
			return true;
		} catch(JSONException e) {
			return false;
		}
	}	
	
	static boolean checkHashTagArrayValid(JSONObject tweet) {
		try {
			JSONObject entities = tweet.getJSONObject("entities");
			//System.out.println("entities are " + entities);
			JSONArray hashtags = entities.getJSONArray("hashtags");
			//System.out.println("hashtags are " + hashtags);
			if (hashtags.length()==0)
				return false;
			//System.out.println("validated hashtag");
			return true;
		} catch(JSONException e) {
			return false;
		}
	}
	
	static boolean isTweetValid(JSONObject tweet) {
		//System.out.println("validatimg tweet");
		return checkValidLanguage(tweet) && 
				checkValidId(tweet) && 
				checkValidUserId(tweet) &&
				checkCreatedAtNotMissing(tweet) && 
				checkTextNotMissing(tweet) && 
				checkHashTagArrayValid(tweet);
	}
	
}
