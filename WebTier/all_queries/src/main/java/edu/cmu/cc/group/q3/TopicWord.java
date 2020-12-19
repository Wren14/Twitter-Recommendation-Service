package edu.cmu.cc.group.q3;

public class TopicWord {

	private final double topicScore;
	private final String word;
	
	public TopicWord(String word, double topicScore) {
		this.topicScore = topicScore;
		this.word = word;
	}
	
	public double getTopicScore() {
		return this.topicScore;
	}
	
	public String getWord() {
		return this.word;
	}
	
}
