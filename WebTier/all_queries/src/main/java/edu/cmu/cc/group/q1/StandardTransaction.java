package edu.cmu.cc.group.q1;

import java.util.StringJoiner;

import org.apache.commons.codec.digest.DigestUtils;
import org.json.*;

import static edu.cmu.cc.group.q1.DriverQ1.CCHASH_NUM_DIGITS;
import static edu.cmu.cc.group.q1.DriverQ1.D;
import static edu.cmu.cc.group.q1.DriverQ1.E;
import static edu.cmu.cc.group.q1.DriverQ1.FEE;
import static edu.cmu.cc.group.q1.DriverQ1.N;
import static edu.cmu.cc.group.q1.DriverQ1.SENDER;
import static edu.cmu.cc.group.q1.DriverQ1.SIGNATURE;
import static edu.cmu.cc.group.q1.DriverQ1.VERT_BAR;

import java.math.*;

public class StandardTransaction extends Transaction{
	
	final long sender;
	final int fee;
	final long signature;
	
	public StandardTransaction(JSONObject transaction) throws JSONException {
		super(transaction);
		this.sender = transaction.getLong(SENDER);
		this.fee = transaction.getInt(FEE);
		this.signature = transaction.getLong(SIGNATURE);
	}
	
	public StandardTransaction(long sender, long recipient, int amount, int fee, String timestamp, String hash) {
		super(recipient, amount, timestamp, hash);
		this.sender = sender;
		this.fee = fee;
		this.signature = sign(hash);
	}

	public static String generateHash(String timestamp, long sender, long recipient, int amount, int fee) {
		StringJoiner joiner = new StringJoiner(VERT_BAR);
		joiner.add(timestamp)
		.add(Long.toString(sender))
		.add(Long.toString(recipient))
		.add(Integer.toString(amount))
		.add(Integer.toString(fee));
		
		String hashHex = DigestUtils.sha256Hex(joiner.toString());
		String CChashHex = hashHex.substring(0, CCHASH_NUM_DIGITS);
		
		return CChashHex;
	}
	
	/**
	 * Checks if signature is valid by decrypting signature with a
	 * public key used by everyone (15619) and N equal to sender's account #
	 * @return
	 */
	public boolean isSignatureValid() {
		BigInteger signBigInt = new BigInteger(Long.toString(signature));
		BigInteger EBigInt = new BigInteger(Long.toString(E));
		BigInteger NBigInt = new BigInteger(Long.toString(sender));
		BigInteger result = signBigInt.modPow(EBigInt, NBigInt);
		String myHash = result.toString(16);
		
		//Make sure that hash consists of 8 hex digits
		StringBuilder zeroPad = new StringBuilder();
		for (int i = 8; i > myHash.length(); i--) {
			zeroPad.append("0");
		}
		
		myHash = zeroPad + myHash;
		
		return myHash.equals(this.hash);
	}
	
	
	public long generateSignature() {
		long hash = Long.parseLong(this.hash, 16);
		
		BigInteger hashBigInt = new BigInteger(Long.toString(hash));
		BigInteger DBigInt = new BigInteger (Long.toString(D));
		BigInteger NBigInt = new BigInteger (Long.toString(N) );
		BigInteger result = hashBigInt.modPow(DBigInt, NBigInt);
		return Long.parseLong(result.toString());		 
	}
	
	public static Long sign(String hashStr) {
		long hash = Long.parseLong(hashStr, 16);
		
		BigInteger hashBigInt = new BigInteger(Long.toString(hash));
		BigInteger DBigInt = new BigInteger (Long.toString(D));
		BigInteger NBigInt = new BigInteger (Long.toString(N) );
		BigInteger result = hashBigInt.modPow(DBigInt, NBigInt);
		return Long.parseLong(result.toString());	
	}
	
	public long getSender() {
		return this.sender;
	}
	
	public int getFee( ) {
		return this.fee;
	}
	
}
