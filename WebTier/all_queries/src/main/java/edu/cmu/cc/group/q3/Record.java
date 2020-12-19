package edu.cmu.cc.group.q3;

public class Record {
	
	private final long tweetId;
	private final int impactScore;
	private final String tweetText;
	private final int wordCount;

	
	public Record(long tweetId, int impactScore, String tweetText, int wordCount) {
		this.tweetId = tweetId;
		this.impactScore = impactScore;
		this.tweetText = tweetText;
		this.wordCount = wordCount;
	}
	
	public long getTweetId() {
		return tweetId;
	}
	
	public final int getImpactScore() {
		return impactScore;
	}
	
	public final String getTweetText() {
		return tweetText;
	}
	
	public final int getWordCount() {
		return wordCount;
	}
	
	@Override
	public String toString() {
		return Long.toString(this.tweetId) + "\t" + tweetText;
	}
}