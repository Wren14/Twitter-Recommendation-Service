package edu.cmu.cc.group.q2;

public class RecommendedUser implements Comparable<RecommendedUser>{
    private double score;
    private String userId;
    private String name;
    private String description;
    private String tweetText;

    public RecommendedUser(String userId, String name, String description, String tweetText, Double score) {
        this.userId = userId;
        this.name = name;
        this.description = description;
        this.tweetText = tweetText;
        this.score = score;
    }

    public int compareTo(RecommendedUser u) {
        // Your web service should return the most updated information of users and their latest contact tweet
        // with the user in the query ordered by the score calculated above.
        // Break ties by the decimal order of user id.
        int scoreCompare = Double.compare(score,u.getScore());
        if (scoreCompare !=0)
            return (scoreCompare * -1);
        // tie in score - break by decimal order of user id
        long myUserId = Long.parseLong(userId);
        long otherUserId = Long.parseLong(u.getUserId());
        return (Long.compare(myUserId, otherUserId) * -1);
    }

    public double getScore() {
        return score;
    }

    public String getUserId() {
        return userId;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getTweetText() {
        return tweetText;
    }
}
