package edu.cmu.cc.group.query1;

import static edu.cmu.cc.group.query1.ClientQ1.CCHASH_NUM_DIGITS;
import static edu.cmu.cc.group.query1.ClientQ1.VERT_BAR;

import java.util.Iterator;
import java.util.StringJoiner;

import org.apache.commons.codec.digest.DigestUtils;
import org.json.*;

public class RewardTransaction extends Transaction {
	
	public RewardTransaction(JSONObject transaction) throws JSONException {
		super(transaction);
		
		//Make sure that reward tx has no fields associated with standard tx
		for(Iterator<String> it = transaction.keys(); it.hasNext(); ) {
			String key = it.next();
			switch(key) {
				case "send": 
				case "fee":
				case "sig":
					throw new JSONException("Invalid field - " + key);
				default:
					break;
			}
		}
	}
	
	public RewardTransaction(long recipient, int amount, String timestamp, String hash) {
		super(recipient, amount, timestamp, hash);
	}
	
	public static String generateHash2(String timestamp, long recipient, int amount) {
		StringJoiner joiner = new StringJoiner(VERT_BAR);
		joiner.add(timestamp)
		.add("")
		.add(Long.toString(recipient))
		.add(Integer.toString(amount))
		.add("");
		
		
		String hashHex = DigestUtils.sha256Hex(joiner.toString());
		String CChashHex = hashHex.substring(0, CCHASH_NUM_DIGITS);
		
		return CChashHex;
	}
	
}
