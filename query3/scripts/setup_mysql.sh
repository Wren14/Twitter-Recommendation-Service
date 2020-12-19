#This script is for installing MySQL on EC2 instance, download data from GCP buckets, upload data into DB
#
sudo apt update
sudo apt install mysql-server --assume-yes
sudo service mysql start
sudo mysql_secure_installation

sudo mysql -uroot -pmysql123
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'ADD_PASSWORD';
FLUSH PRIVILEGES;
exit

cd /etc/mysql/mysql.conf.d
sudo nano mysqld.cnf
#Change bind-address to 0.0.0.0

sudo service mysql restart

#Install gsutil
sudo apt-get install apt-transport-https ca-certificates --assume-yes
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
sudo apt-get update && sudo apt-get install google-cloud-sdk --assume-yes

#Download files from bucket. Load into DB
mkdir ~/db_input
cd ~/db_input
rm part*
gsutil cp -r  gs://cc_final_temp_bucket/output/* ./

date +%s
for f in  part-*; do         sudo mysql -uroot -pmysql123 -e "USE final; load data local infile '"$f"' into table word CHARACTER SET 'utf8mb4' columns terminated by '\t' escaped by '' LINES TERMINATED BY 'Q\t\t\tQ\n' (tweetId,word,tweetText,author,impactScore,impactScoreLog1p,wordCount,totalWordCount,termFreq,wordSanitized,tweetTextSanitized,isStop,ts);"; done
date +%s
