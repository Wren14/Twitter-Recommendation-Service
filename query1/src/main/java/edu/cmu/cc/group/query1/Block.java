package edu.cmu.cc.group.query1;

import org.json.*;

import static edu.cmu.cc.group.query1.ClientQ1.ALL_TRANSACTIONS;
import static edu.cmu.cc.group.query1.ClientQ1.BLOCK_HASH;
import static edu.cmu.cc.group.query1.ClientQ1.BLOCK_ID;
import static edu.cmu.cc.group.query1.ClientQ1.CCHASH_NUM_DIGITS;
import static edu.cmu.cc.group.query1.ClientQ1.PROOF_OF_WORK;
import static edu.cmu.cc.group.query1.ClientQ1.VERT_BAR;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import org.apache.commons.codec.digest.DigestUtils;

public class Block {
	
	final private int id;
	final private String hash;
	final private String pow;
	final private List<Transaction> transactions = new ArrayList<Transaction>();
	
	final long miner;
	
	public Block(JSONObject block) throws JSONException {
		
		this.id = block.getInt(BLOCK_ID);
		this.hash = block.getString(BLOCK_HASH);
		this.pow = block.getString(PROOF_OF_WORK);
		
		JSONArray allTransactions = block.getJSONArray(ALL_TRANSACTIONS);
		final int numTransactions = allTransactions.length();
		
		for (int i = 0; i < numTransactions - 1; i++) {
			JSONObject transactionJson = allTransactions.getJSONObject(i);
			Transaction standardTransaction = new StandardTransaction(transactionJson);
			transactions.add(standardTransaction);
		}
		
		JSONObject transactionJson = allTransactions.getJSONObject(numTransactions - 1);
		Transaction rewardTransaction = new RewardTransaction(transactionJson);
		transactions.add(rewardTransaction);
		this.miner = rewardTransaction.getRecipient();
	}
	
	public static String generateHash(int blockId, List<Transaction> transactions, String pow, String prevBlockHash) {
		StringJoiner joiner = new StringJoiner(VERT_BAR);
		joiner.add(Integer.toString(blockId)).add(prevBlockHash);
		
		for(Transaction tx: transactions) {
			joiner.add(tx.getHash());
		}
		
		String hashHex = DigestUtils.sha256Hex(joiner.toString());
		String CChashHex = hashHex + pow;
		CChashHex = DigestUtils.sha256Hex(CChashHex);
		CChashHex = CChashHex.substring(0, CCHASH_NUM_DIGITS);
		return CChashHex;
	}
	
	public static String generateHash(Block block, String prevBlockHash) {
		StringJoiner joiner = new StringJoiner(VERT_BAR);
		joiner.add(Integer.toString(block.getId())).add(prevBlockHash);
		
		List<Transaction> transactions = block.getTransactions();
		
		for(Transaction tx: transactions) {
			joiner.add(tx.getHash());
		}
		
		String hashHex = DigestUtils.sha256Hex(joiner.toString());
		String CChashHex = hashHex + block.getPow();
		CChashHex = DigestUtils.sha256Hex(CChashHex);
		CChashHex = CChashHex.substring(0, CCHASH_NUM_DIGITS);
		return CChashHex;
	}

	public String getPow() {
		return this.pow;
	}
	
	public void addTransaction(Transaction tx) {
		this.transactions.add(tx);
	}
	
	public int getId() {
		return id;
	}
	
	public String getHash() {
		return this.hash;
	}
	
	public List<Transaction> getTransactions() {
		return this.transactions;
	}
	
	public long getMiner() {
		return this.miner;
	}
	
}
