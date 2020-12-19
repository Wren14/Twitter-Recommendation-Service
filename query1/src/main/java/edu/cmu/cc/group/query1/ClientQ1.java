package edu.cmu.cc.group.query1;

import org.apache.commons.lang3.*;
import org.json.*;

import java.nio.charset.Charset;
import java.util.*;
import java.util.Base64.*;
import java.util.zip.*;
import java.util.concurrent.TimeUnit;

//Questions
//Can chain contain no transactions at all??

public class ClientQ1 {

	//RSA Constants used for signing
	final static long	N = 1284110893049L, 
						E = 15619L,
						D = 379666662787L;
	
	final static CharSequence VERT_BAR = "|";
	
	//Number of bytes that cchash() takes from output of sha-256
	final static int CCHASH_NUM_DIGITS = 8;
	
	private final static Charset UTF8 = Charset.forName("UTF-8");
	
	private final static String TEAM_ID = "Let'sDoIt!";
	private final static String TEAM_AWS_ACCOUNT_ID = "586374175774";
	
	//Size of buffer to use for decompressing zlib-compressed string
	private final static int BUFFER_SIZE = 1024;
	
	//Max amount of amount in reward transaction
	final static int MAX_REWARD = 500000000;
	
	//Keys of all fields in JSON file
	final static String
		CHAIN = "chain",
		
		ALL_TRANSACTIONS = "all_tx",
		
		SENDER = "send",
		RECIPIENT = "recv",
		FEE = "fee",
		SIGNATURE = "sig",
		AMOUNT = "amt",
		TIMESTAMP = "time",
		TRANSACTION_HASH = "hash",
		
		BLOCK_ID = "id",
		BLOCK_HASH = "hash",
		PROOF_OF_WORK = "pow",
		
		NEW_TRANSACTION = "new_tx";
	
	//Proof of Work goal - black hash should start with 0
	final static String BLOCK_HASH_START = "0";
	
	final static String PREV_BLOCK_HASH_FOR_FIRST_BLOCK = "00000000";
	
	/**
	 * Mines a block
	 * @param base64URLInput base-64 encoded zlib-compressed json string
	 * @return message of format "TEAMID,TEAM_AWS_ACCOUNT_ID\n<INVALID|>"
	 * if input string does not represent a well-formed json string that
	 * represents a valid blockchain and request.
	 * Returns message of format "TEAMID,TEAM_AWS_ACCOUNT_ID\n<SIGNATURE|PoW>"
	 * if input request is valid.
	 */
	public String handleInputRequest(String base64URLInput) {
		
		//Decode from base64
		Decoder decoder = Base64.getUrlDecoder();
		byte[] zlibCompressed = null;
		
		try {
			zlibCompressed = decoder.decode(base64URLInput);
		} catch (IllegalArgumentException e) {
			return errorReply("Invalid base64 input"); 
		}
		
		StringBuilder jsonRequestBuilder = new StringBuilder();
		
		//Decompress string
		Inflater decompresser = null;
		try {
			decompresser = new Inflater();
			decompresser.setInput(zlibCompressed);
			
			byte []buffer = new byte[BUFFER_SIZE];
			
			while (decompresser.getRemaining() > 0) {
				Arrays.fill(buffer, (byte) 0);
				decompresser.inflate(buffer);
				
				String temp = new String(buffer, UTF8);
				jsonRequestBuilder.append(temp);
			}
			
		} catch (DataFormatException e) {
			return errorReply("Invalid compressed data");
		} finally {
			if (decompresser != null)
				decompresser.end();
		}
		
		
		String jsonRequest = jsonRequestBuilder.toString();
		
		//Parse blockchain JSON string into blockchain object
		JSONObject jsonInput = new JSONObject(jsonRequest);
		Blockchain blockchain = null;
		
		try {
			blockchain = new Blockchain(jsonInput.getJSONArray(CHAIN));
		} catch (JSONException e) {
			return errorReply("Malformed blockchain in request");
		}
		
		 Map<Long, Long> accountBalanceMap = new HashMap<>();
		if (!Validator.isBlockchainValid(blockchain, accountBalanceMap))
			return errorReply("Invalid blockchain in request");
		
		//Make sure the new transaction has all the necessary fields
		JSONObject newTx = jsonInput.getJSONObject(NEW_TRANSACTION);
		if (!Validator.isNewTxValid(newTx, accountBalanceMap))
			return errorReply("Malformed new transaction in request");
		
		//mine a block
		String response = this.mineBlock(jsonRequest, blockchain.getBlocks());
		return response;
		
	}
	
	/**
	 * Generates an error message
	 * @param debugMsg debug message to be appended to error message
	 * @return message of format "TEAMID,TEAM_AWS_ACCOUNT_ID\n<INVALID|>"
	 */
	private String errorReply(String debugMsg) {
		
		if (debugMsg == null)
			debugMsg = "";
		
		return String.format("%s,%s\n<INVALID|%s>", TEAM_ID, TEAM_AWS_ACCOUNT_ID, debugMsg);
	}
	
	/**
	 * Creates a response to a valid request
	 * @param signature signature of a newly created transaction
	 * @param pow Proof of Work
	 * @return message of format "TEAMID,TEAM_AWS_ACCOUNT_ID\n<SIGNATURE|PoW>"
	 */
	private String createResponse(String signature, String pow) {
		String response = String.format("%s,%s\n<%s|%s>", TEAM_ID, TEAM_AWS_ACCOUNT_ID, signature, pow);
		return response;
	}
	
	/**
	 * Mines a block
	 * @param newTx new transaction to approve???
	 * @return new block with completed standard transaction and reward transaction
	 */

	private String mineBlock(String request, List<Block> blockChain) {
		
		JSONObject jsonInput = new JSONObject(request);
		JSONArray chain = jsonInput.getJSONArray(CHAIN);
		int chainLength = chain.length();
		
		// Create the new transaction
		JSONObject newTx = jsonInput.getJSONObject(NEW_TRANSACTION);
		long recipient = newTx.getLong(RECIPIENT);
		int amount = newTx.getInt(AMOUNT);
		String timestamp = newTx.getString(TIMESTAMP);
		
		//Sender is going to be us (our public key)
		long sender = N;
		int fee = 0;
		String hash = StandardTransaction.generateHash(timestamp, sender, recipient, amount, fee);
		StandardTransaction newTxObj = new StandardTransaction(sender, recipient, amount, fee, timestamp, hash);
		
		// Create the reward transaction
		long newTxTimestamp = Long.parseLong(timestamp);
		long rewardTxTimestamp = newTxTimestamp + TimeUnit.MINUTES.toNanos(10);
		String rewardTimestamp = Long.toString(rewardTxTimestamp);
		String rewardTxHash = RewardTransaction.generateHash2(rewardTimestamp, N, MAX_REWARD);
		RewardTransaction newRwTxObj = new RewardTransaction(N, MAX_REWARD, rewardTimestamp, rewardTxHash);
		
		String prevBlockHash = blockChain.get(chainLength-1).getHash();
		int newBlockId = chainLength;
		
		List<Transaction> transactions = new ArrayList<>(2);
		transactions.add(newTxObj);
		transactions.add(newRwTxObj);
		
		
		String CChashHex;
		String pow;
		
		//mine the block until its hash starts with "0"
		do
		{
			int length = 10;
			boolean useLetters = true;
			boolean useNumbers = false;
			pow = RandomStringUtils.random(length, useLetters, useNumbers);
			CChashHex = Block.generateHash(newBlockId, transactions, pow, prevBlockHash);
		} while (!CChashHex.startsWith(BLOCK_HASH_START));
		
		String sigString = Long.toString(newTxObj.signature);
		return this.createResponse(sigString, pow);
	}
	
}
