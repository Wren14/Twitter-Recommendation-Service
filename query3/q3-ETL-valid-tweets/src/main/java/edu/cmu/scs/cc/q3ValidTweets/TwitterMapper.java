package edu.cmu.scs.cc.q3ValidTweets;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.io.IOException;
import java.util.StringJoiner;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.*;



public class TwitterMapper extends Mapper<Object, Text, Text, BytesWritable>{
    public Text tweetId = new Text();
    public BytesWritable tweetDetails = new BytesWritable();
    
    /**
     * Mapper for Wiki data analysis example.
     *
     * @param key input key of mapper
     * @param value input value of mapper
     * @param context output key/value pair of mapper
     * @throws IOException if io exception occurs
     * @throws InterruptedException if interrupted exception occurs
     * 
     * output key: tweet_id
     * output value: created_at, "text", user_id, user_screen_name, user_description,
	 * user_followers_count, user_friends_count, user_favourites_count,
     * in_reply_to_user_id, retweet_user_id, retweet_user_screen_name, 
     * retweet_user_description, "hashtags"(each comma separated), 
     */
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // value id the line for the file, a json string, needs to be converted to json obj   	
    	JSONObject tweet = null;
    	try {
            //System.out.println("value to string: " + value.toString());
    		tweet = new JSONObject(value.toString());
    	} catch (JSONException e) {
    		return;
    	}

        
        // Filter and get the valid data from DataFilter
        if (DataFilter.isTweetValid(tweet)) {
            // Extract the required information
    		Long id = null;
    		String idStr = null;
    		try {
    			id = tweet.getLong("id");
    			idStr = id.toString();
    		} catch(JSONException e) {
    			try {
    				idStr = tweet.getString("id_str");
    			}
    			catch (JSONException ex) {
    			} 
    		}


        	tweetId.set(idStr);
        	
        	// Sample format in string: "Wed Oct 10 20:19:24 +0000 2018"
        	String createdAt = null;
        	Long timestamp = null;
        	try {
        		createdAt = tweet.getString("created_at");
        		DateTimeFormatter format  = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss Z yyyy");
        		ZonedDateTime zonedTime  = ZonedDateTime.parse(createdAt, format); 
        		timestamp = zonedTime.toInstant().toEpochMilli();
        	} catch (JSONException e) {
        		
        	} catch (IllegalArgumentException ex) {
        		
        	} catch (DateTimeParseException exp) {}
        	
        	
            String text = null;
            try {
            	text = tweet.getString("text");
				//System.out.println("text before HTML decode: " + text);
				//text = StringEscapeUtils.unescapeHtml(text);
				//System.out.println("text after HTML decode: " + text);
            } catch (JSONException e) {
            }

			String userIdStr=null, userScreenName=null, userDescription=null;
            Long userFollowersCount=0L, userFriendsCount=0L, userFavCount=0L;
            try {
            	JSONObject user = tweet.getJSONObject("user");
				//System.out.println("user object: " + user);

            	Long userId = null;
            	userIdStr = null;
            	try {
            		userId = user.getLong("id");
            		userIdStr = userId.toString();
            	} catch(JSONException e) {  // test this
            		try {
            			userIdStr = user.getString("id_str");
            		}
            		catch (JSONException ex) { //test this
            		} 
            	}
				//System.out.println("user id: " + userIdStr);
            	userScreenName = user.getString("screen_name");
				//userScreenName = StringEscapeUtils.unescapeHtml(userScreenName);
				//System.out.println("user screen name: " + userScreenName);
				// user description can be nullable
				try {
					userDescription = user.getString("description");
					//userDescription = StringEscapeUtils.unescapeHtml(userDescription);
				} catch (JSONException e) {
					System.out.println("JSON excpetption " + e);
					//System.out.println("user info: ");
					//System.out.println(user);
				}
				//System.out.println("user description: " + userDescription);
            	userFollowersCount = user.getLong("followers_count");
				//System.out.println("user followers count: " + userFollowersCount);
            	userFriendsCount = user.getLong("friends_count");
				//System.out.println("user friends count " + userFriendsCount);
				//userFavCount = user.getLong("favourites_count");
				//System.out.println("user favorites count: " + userFavCount);
            } catch (JSONException e) {
				System.out.println("JSON excpetption " + e);
            }
    		
    		
    		// A tweet could be normal tweet, reply or retweet
    		// If a tweet is a reply (e.g. A replies to B), then the ID of user B is 
    		// in in_reply_to_user_id or in_reply_to_user_id_str
    		Long inReplyToUserIdLong = null;
    		String inReplyToUserId = null;
    		try {
    			inReplyToUserId = tweet.getString("in_reply_to_user_id_str");
    		}
    		catch (JSONException e) {
    			try {
    				inReplyToUserIdLong = tweet.getLong("in_reply_to_user_id");
					inReplyToUserId = inReplyToUserIdLong.toString();
    			}
    			catch (JSONException ex) {
    			}
    		}
    		
    		// If a tweet is a retweet, the original tweet object is stored in retweeted_status
    		
    		String retweetedUserIdStr=null, retweetedUserScreenName=null, retweetedUserDescription=null;
    		try {
    		JSONObject retweet = tweet.getJSONObject("retweeted_status");
    		//System.out.println("retweet: " + retweet);
    		JSONObject retweetedUser = retweet.getJSONObject("user");
    		//System.out.println("retweet user: " + retweetedUser);
 
    		Long retweetedUserId = null;
    		retweetedUserIdStr = null;
    		try {
    			retweetedUserId = retweetedUser.getLong("id");
    			//System.out.println("retweeted user id is " + retweetedUserId);
    			retweetedUserIdStr = retweetedUserId.toString();
    			//System.out.println("retweeted user id str to long is " + retweetedUserIdStr);
    		} catch(JSONException e) {  // test this
    			try {
    				retweetedUserIdStr = retweetedUser.getString("id_str");
    				//System.out.println("retweeted user id str is " + retweetedUserIdStr);
    			}
    			catch (JSONException ex) { //test this
    			} 
    		}    
    		//System.out.println("retweetedUserIdStr: " + retweetedUserIdStr);
    		
    		retweetedUserScreenName = retweetedUser.getString("screen_name");
    		//retweetedUserScreenName = StringEscapeUtils.unescapeHtml(retweetedUserScreenName);
    		//System.out.println("retweetedUserScreenName: " + retweetedUserScreenName);
    		
    		retweetedUserDescription = retweetedUser.getString("description");
    		//retweetedUserDescription = StringEscapeUtils.unescapeHtml(retweetedUserDescription);
    		//System.out.println("retweetedUserDescription: " + retweetedUserDescription);
    		
    		} catch (JSONException e) {
    			
    		}
    		
    		StringJoiner hashtags = new StringJoiner(" ");
			JSONObject entities = tweet.getJSONObject("entities");
			//System.out.println("entities are " + entities);
			JSONArray hashtagsArray = entities.getJSONArray("hashtags");
    		for(int i=0; i<hashtagsArray.length(); i++) {
    			JSONObject hashtagEntry = hashtagsArray.getJSONObject(i);
    			String hashtagText = hashtagEntry.getString("text");
    			hashtags.add(hashtagText);
    		}
            String hashTagsString = hashtags.toString();

			int retweetCount = 0;
			try {
				retweetCount = tweet.getInt("retweet_count");
			} catch (JSONException e) {

			}

			int favoriteCount = 0;
			try {
				favoriteCount = tweet.getInt("favorite_count");
			} catch (JSONException e) {

			}


            // Set the value in the context as the access count of the wiki page and the filename
            StringJoiner tweetDetailsJoiner = new StringJoiner("{TEAMLET'SDOIT!JOINER}");
            tweetDetailsJoiner.add(timestamp.toString());
            tweetDetailsJoiner.add(text);
            tweetDetailsJoiner.add(userIdStr);
            tweetDetailsJoiner.add(userScreenName);
            tweetDetailsJoiner.add(userDescription);
            tweetDetailsJoiner.add(Long.toString(userFollowersCount));
            tweetDetailsJoiner.add(Long.toString(userFriendsCount));
            tweetDetailsJoiner.add(Integer.toString(favoriteCount));
            tweetDetailsJoiner.add(inReplyToUserId);
            tweetDetailsJoiner.add(retweetedUserIdStr);
            tweetDetailsJoiner.add(retweetedUserScreenName);
            tweetDetailsJoiner.add(retweetedUserDescription);
            tweetDetailsJoiner.add(hashTagsString);
            tweetDetailsJoiner.add(Integer.toString(retweetCount));
           
            //System.out.println("key is " + idStr);
            //System.out.println("value is " + tweetDetailsJoiner.toString());
            byte arr[] = tweetDetailsJoiner.toString().getBytes("UTF8");
            tweetDetails.set(arr,0,arr.length);
            
            // Write to the context for the reducers
            context.write(tweetId, tweetDetails);
        }
    }
}
