create database final if not exists;
use final;


SET NAMES 'utf8';

#Load from local file system
load data local
infile '~/db_input/part-00000'
into table word
CHARACTER SET 'utf8mb4'
columns terminated by '\t' escaped by ''
LINES TERMINATED BY 'Q\t\t\tQ\n'
(tweetId,word,tweetText,author,impactScore,impactScoreLog1p,wordCount,totalWordCount,termFreq,wordSanitized,tweetTextSanitized,isStop,ts);

#Load from S3
load data from s3 PREFIX
's3://cc-final-q3/q3-etl-output/'
into table word
CHARACTER SET 'utf8mb4'
columns terminated by '\t' escaped by ''
LINES TERMINATED BY 'Q\t\t\tQ\n'
(tweetId,word,tweetText,author,impactScore,impactScoreLog1p,wordCount,totalWordCount,termFreq,wordSanitized,tweetTextSanitized,isStop,ts);





load data from s3 PREFIX
's3://cc-final-q3/q3-etl-output-new/'
into table tweet
CHARACTER SET 'utf8mb4'
columns terminated by '\t' escaped by ''
LINES TERMINATED BY 'Q\t\t\tQ\n'
(tweetID, impactScore, author, tweetText, wordCount, ts);





load data from s3 PREFIX
's3://cc-final-q3/q3-etl-output-new-small/'
into table tweet
CHARACTER SET 'utf8mb4'
columns terminated by '\t' escaped by ''
LINES TERMINATED BY 'Q\t\t\tQ\n'
(tweetID, impactScore, author, tweetText, wordCount, ts);
