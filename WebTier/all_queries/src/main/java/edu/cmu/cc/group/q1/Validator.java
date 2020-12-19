package edu.cmu.cc.group.q1;

import org.json.JSONException;
import org.json.JSONObject;

import static edu.cmu.cc.group.q1.DriverQ1.*;

import java.util.*;


public class Validator {
	
	private static long prevTs = 0L;
	
	public static boolean isBlockchainValid(Blockchain blockchain, Map<Long, Long> accountBalanceMap) {
				
		List<Block> blocks = blockchain.getBlocks();
		
		final int numBlocks = blocks.size();
		
		String prevBlockHash = PREV_BLOCK_HASH_FOR_FIRST_BLOCK;
		
		//Data structure to hold CChashes of all blocks
		Set<String> cchashSet = new HashSet<>(numBlocks);
		
		prevTs = 0L;
		
		for (int i = 0; i < numBlocks; i++) {
			Block block = blocks.get(i);
			
			//Make sure each block is valid
			if (!isBlockValid(block, i, prevBlockHash))
				return false;
			
			prevBlockHash = block.getHash();
			
			//Make sure no 2 blocks have the same CCHash
			if (cchashSet.contains(block.getHash()))
				return false;
			else
				cchashSet.add(block.getHash());
			
			//Ensure that timestamps in all transactions are strictly in increasing order
			List<Transaction> transactions = block.getTransactions();
			for (Transaction tx : transactions) {
				long ts = Long.parseLong(tx.getTimestamp());
				if (ts <= prevTs)
					return false;
				
				prevTs = ts;
			}
			
			//Make sure that the sender in each transaction has enough money
			//to send the necessary (amount + fee)
			long totalFee = 0L; //Fee to be paid to miner by senders
			
			for (Transaction tx : transactions) {
				
				if (tx instanceof StandardTransaction) {
					
					int amountToBeSent = tx.getAmount();
					int fee = ((StandardTransaction) tx).getFee();
					
					//Check if sender has enough money (amount + fee)
					long sender = ((StandardTransaction) tx).getSender();
					long senderCurBalance = accountBalanceMap.getOrDefault(sender, 0L);
					if (senderCurBalance < (amountToBeSent + fee) )
						return false;
					
					senderCurBalance -= (amountToBeSent + fee);
					accountBalanceMap.put(sender, senderCurBalance);
					
					//Send amount to recipient
					long recipient = tx.getRecipient();
					long recipientCurBalance = accountBalanceMap.getOrDefault(recipient, 0L);
					recipientCurBalance += amountToBeSent;
					accountBalanceMap.put(recipient, recipientCurBalance);
					
					//Send fee to miner
					totalFee += fee;
					
				} else if (tx instanceof RewardTransaction) {
					int amountRewarded = tx.getAmount();
					long rewardRecipient = tx.getRecipient();
					
					long curBalance = accountBalanceMap.getOrDefault(rewardRecipient, 0L);
					curBalance += (amountRewarded + totalFee);
					accountBalanceMap.put(rewardRecipient, curBalance);
				} else {
					//This should not happen
					return false;
				}
			}
			
		}
		
		return true;
	}
	
	private static boolean isBlockValid(Block block, int expectedBlockId, String prevBlockHash) {

		//Ensure that this block has correct block ID
		if (block.getId() != expectedBlockId)
			return false;
		
		//check if block hash is valid
		if (!isBlockHashValid(block, prevBlockHash))
			return false;
		

		List<Transaction> transactions = block.getTransactions();
		
		//There has to be at least 1 transaction in a block
		if (transactions == null || transactions.isEmpty())
			return false;
		
		final int numTransactions = transactions.size();

		//Make sure that the first block has only 1 reward tx
		if (expectedBlockId == 0 && numTransactions != 1)
			return false;
		
		Set<String> cchashSet = new HashSet<>(numTransactions);
		
		//Ensure that all transactions except for the last one are valid
		//standard transactions
		for (int i = 0; i < numTransactions - 1; i++) {
			StandardTransaction transaction = (StandardTransaction) transactions.get(i);
			if (!isValidStandardTransaction(transaction))
				return false;
			
			if (cchashSet.contains(transaction.getHash()))
				return false;
			else
				cchashSet.add(transaction.getHash());
		}
		
		//Make sure that the last transaction in the block is a valid reward transaction
		RewardTransaction rewardTransaction = (RewardTransaction) transactions.get(numTransactions - 1);
		if (!isValidRewardTransaction(rewardTransaction))
			return false;
		
		if (cchashSet.contains(rewardTransaction.getHash()))
			return false;
		else
			cchashSet.add(rewardTransaction.getHash());
		
		return true;
	}
	
	private static boolean isBlockHashValid(Block block, String prevBlockHash) {
		
		//Block has to start with a zero
		if (!block.getHash().startsWith(BLOCK_HASH_START))
			return false;
		
		//Generate hash yourself
		//Make sure hash in block is correct
		if (!block.getHash().equals(Block.generateHash(block, prevBlockHash)) )
			return false;
		
		return true;
	}
	
	/**
	 * Checks if standard transaction is valid
	 * @param transaction input transaction
	 * @return true if transaction is valid, false otherwise
	 */
	private static boolean isValidStandardTransaction(StandardTransaction transaction) {
		
		//Standard transaction is valid if its hash is correct is signature is valid 
		String hash = transaction.getHash();
		
		if (!hash.equals(StandardTransaction.generateHash(transaction.getTimestamp(), transaction.getSender(), transaction.getRecipient(), transaction.getAmount(), transaction.getFee())))
			return false;
		
		//Check if transaction signature is valid
		if (!transaction.isSignatureValid())
			return false;
		
		//If amount is negative, reject tx
		if (transaction.getAmount() < 0 || transaction.getFee() < 0)
			return false;
		
		return true;
	}
	
	private static boolean isValidRewardTransaction(RewardTransaction transaction) {
		
		//Reward transaction is valid if its hash is correct
		String hash = transaction.getHash();
		
		if (!hash.equals(RewardTransaction.generateHash2(transaction.getTimestamp(), transaction.getRecipient(), transaction.getAmount())))
			return false;
		
		//If amount is negative or is greater than max. allowed reward amount, reject tx
		if (transaction.getAmount() < 0 || transaction.getAmount() > MAX_REWARD)
			return false;
		
		return true;
	}
	
	/**
	 * Checks if new transaction specified in input request is valid
	 * @param newTx new transaction to be added to blockchain
	 * @return true if new transaction is valid, false otherwise
	 */
	public static boolean isNewTxValid(JSONObject newTx, Map<Long, Long> accountBalanceMap) {
		
		try {
			newTx.getLong(RECIPIENT);
			int amount = newTx.getInt(AMOUNT);
			
			if (amount < 0)
				return false;
			
			//Since money is going to be withdrawn from our account,
			//make sure we have the money
			long ourBalance = accountBalanceMap.getOrDefault(N, 0L);
			if (ourBalance < amount)
				return false;
			
			ourBalance -= amount;
			ourBalance += MAX_REWARD;
			
			String tsStr = newTx.getString(TIMESTAMP);
			long ts = Long.parseLong(tsStr);
			if (ts <= prevTs)
				return false;
			prevTs = ts;
			
		} catch(JSONException e) {
			return false;
		}
		
		return true;
	}
}
