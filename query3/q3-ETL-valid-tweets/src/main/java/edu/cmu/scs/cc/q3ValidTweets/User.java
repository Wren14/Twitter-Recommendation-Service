package edu.cmu.scs.cc.q3ValidTweets;

public class User {
	String id_str;
	String screen_name;
	String description;

    public User(){
        
    }
    
    public User(Long id, String id_str, String screen_name, String description, String created_at){
    	this.id_str = id_str;
        this.screen_name = screen_name;
        this.description = description;
    }

}


