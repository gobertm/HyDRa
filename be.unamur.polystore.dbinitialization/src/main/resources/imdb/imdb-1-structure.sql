--
-- Table structure for table `actors`
--

DROP TABLE IF EXISTS `actors`;
CREATE TABLE `actors` (
  `id` int(11) NOT NULL default '0',
  `first_name` varchar(100) default NULL,
  `last_name` varchar(100) default NULL,
  `gender` char(1) default NULL,
  PRIMARY KEY  (`id`),
  KEY `idx_first_name` (`first_name`(15)),
  KEY `idx_last_name` (`last_name`(15))
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

--
-- Table structure for table `directors`
--

DROP TABLE IF EXISTS `directors`;
CREATE TABLE `directors` (
  `id` int(11) NOT NULL default '0',
  `first_name` varchar(100) default NULL,
  `last_name` varchar(100) default NULL,
  PRIMARY KEY  (`id`),
  KEY `idx_first_name` (`first_name`(15)),
  KEY `idx_last_name` (`last_name`(15))
) ENGINE=MyISAM DEFAULT CHARSET=utf8;


--
-- Table structure for table `directors_genres`
--

DROP TABLE IF EXISTS `directors_genres`;
CREATE TABLE `directors_genres` (
  `director_id` int(11) default NULL,
  `genre` varchar(100) default NULL,
  `prob` float default NULL,
  KEY `idx_director_id` (`director_id`),
  KEY `idx_genre` (`genre`(15))
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

--
-- Table structure for table `movies`
--

DROP TABLE IF EXISTS `movies`;
CREATE TABLE `movies` (
  `id` int(11) NOT NULL default '0',
  `name` varchar(100) default NULL,
  `year` int(11) default NULL,
  `rank` float default NULL,
  PRIMARY KEY  (`id`),
  KEY `idx_name` (`name`(10))
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

--
-- Table structure for table `movies_directors`
--

DROP TABLE IF EXISTS `movies_directors`;
CREATE TABLE `movies_directors` (
  `director_id` int(11) default NULL,
  `movie_id` int(11) default NULL,
  KEY `idx_director_id` (`director_id`),
  KEY `idx_movie_id` (`movie_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

--
-- Table structure for table `movies_genres`
--

DROP TABLE IF EXISTS `movies_genres`;
CREATE TABLE `movies_genres` (
  `movie_id` int(11) default NULL,
  `genre` varchar(100) default NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;


--
-- Table structure for table `roles`
--

DROP TABLE IF EXISTS `roles`;
CREATE TABLE `roles` (
  `actor_id` int(11) default NULL,
  `movie_id` int(11) default NULL,
  `role` varchar(100) default NULL,
  KEY `idx_actor_id` (`actor_id`),
  KEY `idx_movie_id` (`movie_id`),
  KEY `idx_role` (`role`(15))
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
