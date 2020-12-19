#Create DB
CREATE DATABASE IF NOT EXISTS final;

#Use DB
USE final;

#Set default engine
SET default_storage_engine=INNODB;

#Table for tweets
DROP TABLE IF EXISTS tweet;

CREATE TABLE tweet (
	tweetId bigint NOT NULL,
	impactScore int NOT NULL,
	author bigint NOT NULL,
	tweetText varchar(1400) NOT NULL,
	wordCount int NOT NULL,
	ts bigint NOT NULL
) DEFAULT CHARSET=utf8mb4;
