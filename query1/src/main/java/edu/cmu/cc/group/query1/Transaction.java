package edu.cmu.cc.group.query1;

import static edu.cmu.cc.group.query1.ClientQ1.AMOUNT;
import static edu.cmu.cc.group.query1.ClientQ1.CCHASH_NUM_DIGITS;
import static edu.cmu.cc.group.query1.ClientQ1.RECIPIENT;
import static edu.cmu.cc.group.query1.ClientQ1.TIMESTAMP;
import static edu.cmu.cc.group.query1.ClientQ1.TRANSACTION_HASH;

import org.apache.commons.codec.digest.DigestUtils;
import org.json.*;

public class Transaction {

	final long recipient;
	final int amount;
	final String timestamp;
	final String hash;
	
	protected Transaction(JSONObject transaction) throws JSONException {
		this.recipient = transaction.getLong(RECIPIENT);
		this.amount = transaction.getInt(AMOUNT);
		this.timestamp = transaction.getString(TIMESTAMP);
		this.hash = transaction.getString(TRANSACTION_HASH);
	}
	
	protected Transaction(long recipient, int amount, String timestamp, String hash) {
		this.recipient = recipient;
		this.amount = amount;
		this.timestamp = timestamp;
		this.hash = hash;
	}
	
	protected String generateHash(String hashText) {
		String hashHex = DigestUtils.sha256Hex(hashText);
		String CChashHex = hashHex.substring(0, CCHASH_NUM_DIGITS);
		return CChashHex;
	}
	
	public String getHash() {
		return this.hash;
	}
	
	public long getRecipient() {
		return this.recipient;
	}
	
	public int getAmount() {
		return this.amount;
	}
	
	public String getTimestamp() {
		return this.timestamp;
	}
	
}
