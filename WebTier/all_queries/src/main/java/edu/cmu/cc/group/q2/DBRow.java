package edu.cmu.cc.group.q2;

public class DBRow {
    private String user1;
    private String hashtagscore;
    private String interactionscore;
    private String replyTexts;
    private String replyHashtags;
    private String retweetTexts;
    private String retweetHashtags;
    private String lastestContactTweet;
    private String user1Name;
    private String user1Description;

    public String getUser1() {
        return user1;
    }

    public void setUser1(String user1) {
        this.user1 = user1;
    }

    public String getHashtagscore() {
        return hashtagscore;
    }

    public void setHashtagscore(String hashtagscore) {
        this.hashtagscore = hashtagscore;
    }

    public String getInteractionscore() {
        return interactionscore;
    }

    public void setInteractionscore(String interactionscore) {
        this.interactionscore = interactionscore;
    }

    public String getReplyTexts() {
        return replyTexts;
    }

    public void setReplyTexts(String replyTexts) {
        this.replyTexts = replyTexts;
    }

    public String getReplyHashtags() {
        return replyHashtags;
    }

    public void setReplyHashtags(String replyHashtags) {
        this.replyHashtags = replyHashtags;
    }

    public String getRetweetTexts() {
        return retweetTexts;
    }

    public void setRetweetTexts(String retweetTexts) {
        this.retweetTexts = retweetTexts;
    }

    public String getRetweetHashtags() {
        return retweetHashtags;
    }

    public void setRetweetHashtags(String retweetHashtags) {
        this.retweetHashtags = retweetHashtags;
    }

    public String getLastestContactTweet() {
        return lastestContactTweet;
    }

    public void setLastestContactTweet(String lastestContactTweet) {
        this.lastestContactTweet = lastestContactTweet;
    }

    public String getUser1Name() {
        return user1Name;
    }

    public void setUser1Name(String user1Name) {
        this.user1Name = user1Name;
    }

    public String getUser1Description() {
        return user1Description;
    }

    public void setUser1Description(String user1Description) {
        this.user1Description = user1Description;
    }

    public DBRow(String user1, String hashtagscore, String interactionscore, String replyTexts, String replyHashtags,
          String retweetTexts, String retweetHashtags, String lastestContactTweet, String user1Name,
          String user1Description){

        //System.out.println("Creating db row with user1 = "+user1);
        //System.out.println("Creating db row with hashtagscore = "+hashtagscore);
        //System.out.println("Creating db row with interactionscore = "+interactionscore);
        //System.out.println("Creating db row with replyTexts = "+replyTexts);
        //System.out.println("Creating db row with replyHashtags = "+replyHashtags);
        //System.out.println("Creating db row with retweetTexts = "+retweetTexts);
        //System.out.println("Creating db row with retweetHashtags = "+retweetHashtags);
        //System.out.println("Creating db row with lastestContactTweet = "+lastestContactTweet);
        //System.out.println("Creating db row with user1Name = "+user1Name);
        //System.out.println("Creating db row with user1Description = "+user1Description);
        this.user1 = user1;
        this.hashtagscore = hashtagscore;
        this.interactionscore = interactionscore;
        this.replyTexts = replyTexts;
        this.replyHashtags = replyHashtags;
        this.retweetTexts = retweetTexts;
        this.retweetHashtags = retweetHashtags;
        this.lastestContactTweet = lastestContactTweet;
        this.user1Name = user1Name;
        this.user1Description = user1Description;
    }
}