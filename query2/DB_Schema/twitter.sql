-- phpMyAdmin SQL Dump
-- version 4.8.5
-- https://www.phpmyadmin.net/
--
-- Host: localhost
<<<<<<< Updated upstream
-- Generation Time: Oct 19, 2019 at 04:35 AM
=======
-- Generation Time: Oct 23, 2019 at 05:26 PM
>>>>>>> Stashed changes
-- Server version: 10.1.40-MariaDB
-- PHP Version: 7.1.29

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET AUTOCOMMIT = 0;
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `testProject`
--

-- --------------------------------------------------------

--
<<<<<<< Updated upstream
-- Table structure for table `hashtag_tweet_mapping`
--

CREATE TABLE `hashtag_tweet_mapping` (
  `id` int(11) NOT NULL,
  `tweet_id` varchar(64) NOT NULL,
  `hashtag_name` varchar(500) NOT NULL
=======
-- Table structure for table `tweet`
--

CREATE TABLE `tweet` (
  `tweetID` varchar(64) NOT NULL,
  `userID` varchar(64) NOT NULL,
  `repliedTo` varchar(64) DEFAULT NULL,
  `retweetedTo` varchar(64) DEFAULT NULL,
  `text` varchar(150) NOT NULL
>>>>>>> Stashed changes
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `tweet`
--

INSERT INTO `tweet` (`tweetID`, `userID`, `repliedTo`, `retweetedTo`, `text`) VALUES
('1', '1', '2', NULL, 'cloud computing'),
('2', '1', '2', NULL, 'computers'),
('3', '2', NULL, '1', 'cloud cloud cmu'),
('4', '2', '1', NULL, 'cmu');

-- --------------------------------------------------------

--
-- Table structure for table `tweet_user_hashtag_count`
--

<<<<<<< Updated upstream
CREATE TABLE `tweet` (
  `id_str` varchar(64) NOT NULL,
  `created_at` varchar(25) NOT NULL,
  `text` varchar(150) NOT NULL,
  `in_reply_to_user_id` varchar(25) DEFAULT NULL,
  `lang` varchar(2) NOT NULL,
  `retweet_to_id` varchar(64) DEFAULT NULL,
  `user_id` varchar(64) DEFAULT NULL
=======
CREATE TABLE `tweet_user_hashtag_count` (
  `id` int(11) NOT NULL,
  `tweetID` varchar(64) NOT NULL,
  `userID` varchar(64) NOT NULL,
  `replyTo` varchar(64) DEFAULT NULL,
  `retweetedTo` varchar(64) DEFAULT NULL,
  `hashtag` varchar(500) NOT NULL,
  `count` int(11) NOT NULL
>>>>>>> Stashed changes
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- --------------------------------------------------------

--
-- Table structure for table `user`
--

CREATE TABLE `user` (
<<<<<<< Updated upstream
  `id_str` varchar(64) NOT NULL,
  `name` varchar(50) NOT NULL,
  `screen_name` varchar(20) NOT NULL,
  `description` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Indexes for dumped tables
--
=======
  `userID` varchar(64) NOT NULL,
  `screenName` varchar(16) NOT NULL,
  `description` varchar(500) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Dumping data for table `user`
--

INSERT INTO `user` (`userID`, `screenName`, `description`) VALUES
('1', 'Jeyhun', NULL),
('2', 'Rajasi', NULL);

-- --------------------------------------------------------
>>>>>>> Stashed changes

--
-- Indexes for table `hashtag_tweet_mapping`
--
ALTER TABLE `hashtag_tweet_mapping`
  ADD PRIMARY KEY (`id`),
  ADD KEY `fkey` (`tweet_id`);

<<<<<<< Updated upstream
--
-- Indexes for table `tweet`
--
ALTER TABLE `tweet`
  ADD PRIMARY KEY (`id_str`),
  ADD KEY `fkey_userId` (`user_id`);
=======
CREATE TABLE `user_hashtag_count` (
  `id` int(11) NOT NULL,
  `userID` varchar(64) NOT NULL,
  `hashTag` varchar(500) NOT NULL,
  `count` int(11) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
>>>>>>> Stashed changes

--
-- Indexes for table `user`
--
ALTER TABLE `user`
  ADD PRIMARY KEY (`id_str`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `hashtag_tweet_mapping`
--
ALTER TABLE `hashtag_tweet_mapping`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- Constraints for dumped tables
--

--
-- Constraints for table `hashtag_tweet_mapping`
--
ALTER TABLE `hashtag_tweet_mapping`
  ADD CONSTRAINT `fkey` FOREIGN KEY (`tweet_id`) REFERENCES `tweet` (`id_str`);

--
-- Constraints for table `tweet`
--
ALTER TABLE `tweet`
<<<<<<< Updated upstream
  ADD CONSTRAINT `fkey_userId` FOREIGN KEY (`user_id`) REFERENCES `twitter`.`user` (`id_str`);
=======
  ADD PRIMARY KEY (`tweetID`),
  ADD KEY `fkey_userId` (`userID`);

--
-- Indexes for table `tweet_user_hashtag_count`
--
ALTER TABLE `tweet_user_hashtag_count`
  ADD PRIMARY KEY (`id`),
  ADD KEY `fkey_tweetID` (`tweetID`),
  ADD KEY `foreignKey_userID` (`userID`);

--
-- Indexes for table `user`
--
ALTER TABLE `user`
  ADD PRIMARY KEY (`userID`);

--
-- Indexes for table `user_hashtag_count`
--
ALTER TABLE `user_hashtag_count`
  ADD PRIMARY KEY (`id`),
  ADD KEY `fkey_hashtag_userID` (`userID`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `tweet_user_hashtag_count`
--
ALTER TABLE `tweet_user_hashtag_count`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;

--
-- AUTO_INCREMENT for table `user_hashtag_count`
--
ALTER TABLE `user_hashtag_count`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=4;

--
-- Constraints for dumped tables
--

--
-- Constraints for table `tweet`
--
ALTER TABLE `tweet`
  ADD CONSTRAINT `fkey_userId` FOREIGN KEY (`userID`) REFERENCES `user` (`userID`);

--
-- Constraints for table `tweet_user_hashtag_count`
--
ALTER TABLE `tweet_user_hashtag_count`
  ADD CONSTRAINT `fkey_tweetID` FOREIGN KEY (`tweetID`) REFERENCES `tweet` (`tweetID`),
  ADD CONSTRAINT `foreignKey_userID` FOREIGN KEY (`userID`) REFERENCES `user` (`userID`);

--
-- Constraints for table `user_hashtag_count`
--
ALTER TABLE `user_hashtag_count`
  ADD CONSTRAINT `fkey_hashtag_userID` FOREIGN KEY (`userID`) REFERENCES `user` (`userID`);
>>>>>>> Stashed changes
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
