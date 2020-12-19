create database cc if not exists;
use cc;

SET NAMES 'utf8';

drop table tweet;

CREATE TABLE if not exists `tweet` (
  `tweetID` varchar(640) NOT NULL,
  `userID` varchar(640) NOT NULL,
  `repliedTo` varchar(640) DEFAULT NULL,
  `retweetedTo` varchar(640) DEFAULT NULL,
  `text` varchar(12000) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



load data local
infile 'part-r-00000-MR_tweets.txt'
into table tweet
CHARACTER SET 'utf8mb4'
columns terminated by '\t'
LINES TERMINATED BY 'TEAMLETSDOITEOLJEYRAJWRE\n'
(tweetID, userID, repliedTo, retweetedTo, text );
