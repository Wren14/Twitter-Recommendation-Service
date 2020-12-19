package edu.cmu.cc.group.q3;

public class Tweet {
	
	private int numTimesWordUsed;
	private final long tweetID;
	private final String tweetText;
	private final int impactScore;
	private final double impactScoreLog1p;
	private final int totalWordCount;
	
	public Tweet(long tweetID,
				String tweetText,
				int impactScore,
				int totalWordCount) {
		
		this.tweetID = tweetID;
		this.tweetText = tweetText;
		this.impactScore = impactScore;
		this.impactScoreLog1p = Math.log1p(impactScore);
		this.totalWordCount = totalWordCount;
		
		
		this.numTimesWordUsed = 1;
	}
	
	public void incrementNumTimesWordUsed() {
		this.numTimesWordUsed++;
	}
	
	public int getNumTimesWordUsed() {
		return this.numTimesWordUsed;
	}
	
	public int getTotalWordCount() {
		return this.totalWordCount;
	}
	
	public int getImpactScore() {
		return this.impactScore;
	}
	
	public double getImpactScoreLog1p() {
		return this.impactScoreLog1p;
	}
	
	public long getTweetID() {
		return this.tweetID;
	}
	
	public String getTweetText() {
		return this.tweetText;
	}
	
	@Override
	public int hashCode() {
		return Long.hashCode(tweetID);
	}
	
	@Override
	public boolean equals(Object o) {
		Tweet other = (Tweet) o;
		return this.tweetID == other.tweetID;
	}
}
