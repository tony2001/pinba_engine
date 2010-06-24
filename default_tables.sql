DROP TABLE IF EXISTS request;

CREATE TABLE `request` (
	  `id` int(11) NOT NULL DEFAULT '0',
	  `hostname` varchar(32) DEFAULT NULL,
	  `req_count` int(11) DEFAULT NULL,
	  `server_name` varchar(64) DEFAULT NULL,
	  `script_name` varchar(128) DEFAULT NULL,
	  `doc_size` float DEFAULT NULL,
	  `mem_peak_usage` float DEFAULT NULL,
	  `req_time` float DEFAULT NULL,
	  `ru_utime` float DEFAULT NULL,
	  `ru_stime` float DEFAULT NULL,
	  `timers_cnt` int(11) DEFAULT NULL,
	  `status` int(11) DEFAULT NULL,
	  PRIMARY KEY (`id`)
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='request';

DROP TABLE IF EXISTS tag;

CREATE TABLE `tag` (
	  `id` int(11) NOT NULL,
	  `name` varchar(255) NOT NULL,
	  PRIMARY KEY (`id`),
	  UNIQUE KEY `name` (`name`)
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='tag';

DROP TABLE IF EXISTS timer;

CREATE TABLE `timer` (
	  `id` int(11) NOT NULL DEFAULT '0',
	  `request_id` int(11) NOT NULL,
	  `hit_count` int(11) DEFAULT NULL,
	  `value` float DEFAULT NULL,
	  PRIMARY KEY (`id`),
	  KEY `request_id` (`request_id`)
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='timer';

DROP TABLE IF EXISTS timertag;

CREATE TABLE `timertag` (
	  `timer_id` int(11) NOT NULL,
	  `tag_id` int(11) NOT NULL,
	  `value` varchar(64) DEFAULT NULL,
	  KEY `timer_id` (`timer_id`)
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='timertag';

DROP TABLE IF EXISTS info;

CREATE TABLE `info` (
	  `req_count` int(11) DEFAULT NULL,
	  `time_total` float DEFAULT NULL,
	  `ru_utime_total` float DEFAULT NULL,
	  `ru_stime_total` float DEFAULT NULL,
	  `time_interval` int(11) DEFAULT NULL,
	  `kbytes_total` float DEFAULT NULL
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='info';

DROP TABLE IF EXISTS report_by_script_name;

CREATE TABLE `report_by_script_name` (
	  `req_count` int(11) DEFAULT NULL,
	  `req_per_sec` float DEFAULT NULL,
	  `req_time_total` float DEFAULT NULL,
	  `req_time_percent` float DEFAULT NULL,
	  `req_time_per_sec` float DEFAULT NULL,
	  `ru_utime_total` float DEFAULT NULL,
	  `ru_utime_percent` float DEFAULT NULL,
	  `ru_utime_per_sec` float DEFAULT NULL,
	  `ru_stime_total` float DEFAULT NULL,
	  `ru_stime_percent` float DEFAULT NULL,
	  `ru_stime_per_sec` float DEFAULT NULL,
	  `traffic_total` float DEFAULT NULL,
	  `traffic_percent` float DEFAULT NULL,
	  `traffic_per_sec` float DEFAULT NULL,
	  `script_name` varchar(128) DEFAULT NULL
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='report1';

DROP TABLE IF EXISTS report_by_server_name;

CREATE TABLE `report_by_server_name` (
	  `req_count` int(11) DEFAULT NULL,
	  `req_per_sec` float DEFAULT NULL,
	  `req_time_total` float DEFAULT NULL,
	  `req_time_percent` float DEFAULT NULL,
	  `req_time_per_sec` float DEFAULT NULL,
	  `ru_utime_total` float DEFAULT NULL,
	  `ru_utime_percent` float DEFAULT NULL,
	  `ru_utime_per_sec` float DEFAULT NULL,
	  `ru_stime_total` float DEFAULT NULL,
	  `ru_stime_percent` float DEFAULT NULL,
	  `ru_stime_per_sec` float DEFAULT NULL,
	  `traffic_total` float DEFAULT NULL,
	  `traffic_percent` float DEFAULT NULL,
	  `traffic_per_sec` float DEFAULT NULL,
	  `server_name` varchar(64) DEFAULT NULL
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='report2';

DROP TABLE IF EXISTS report_by_hostname;

CREATE TABLE `report_by_hostname` (
	  `req_count` int(11) DEFAULT NULL,
	  `req_per_sec` float DEFAULT NULL,
	  `req_time_total` float DEFAULT NULL,
	  `req_time_percent` float DEFAULT NULL,
	  `req_time_per_sec` float DEFAULT NULL,
	  `ru_utime_total` float DEFAULT NULL,
	  `ru_utime_percent` float DEFAULT NULL,
	  `ru_utime_per_sec` float DEFAULT NULL,
	  `ru_stime_total` float DEFAULT NULL,
	  `ru_stime_percent` float DEFAULT NULL,
	  `ru_stime_per_sec` float DEFAULT NULL,
	  `traffic_total` float DEFAULT NULL,
	  `traffic_percent` float DEFAULT NULL,
	  `traffic_per_sec` float DEFAULT NULL,
	  `hostname` varchar(32) DEFAULT NULL
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='report3';

DROP TABLE IF EXISTS report_by_server_and_script;

CREATE TABLE `report_by_server_and_script` (
	  `req_count` int(11) DEFAULT NULL,
	  `req_per_sec` float DEFAULT NULL,
	  `req_time_total` float DEFAULT NULL,
	  `req_time_percent` float DEFAULT NULL,
	  `req_time_per_sec` float DEFAULT NULL,
	  `ru_utime_total` float DEFAULT NULL,
	  `ru_utime_percent` float DEFAULT NULL,
	  `ru_utime_per_sec` float DEFAULT NULL,
	  `ru_stime_total` float DEFAULT NULL,
	  `ru_stime_percent` float DEFAULT NULL,
	  `ru_stime_per_sec` float DEFAULT NULL,
	  `traffic_total` float DEFAULT NULL,
	  `traffic_percent` float DEFAULT NULL,
	  `traffic_per_sec` float DEFAULT NULL,
	  `server_name` varchar(64) DEFAULT NULL,
	  `script_name` varchar(128) DEFAULT NULL
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='report4';

DROP TABLE IF EXISTS report_by_hostname_and_script;

CREATE TABLE `report_by_hostname_and_script` (
	  `req_count` int(11) DEFAULT NULL,
	  `req_per_sec` float DEFAULT NULL,
	  `req_time_total` float DEFAULT NULL,
	  `req_time_percent` float DEFAULT NULL,
	  `req_time_per_sec` float DEFAULT NULL,
	  `ru_utime_total` float DEFAULT NULL,
	  `ru_utime_percent` float DEFAULT NULL,
	  `ru_utime_per_sec` float DEFAULT NULL,
	  `ru_stime_total` float DEFAULT NULL,
	  `ru_stime_percent` float DEFAULT NULL,
	  `ru_stime_per_sec` float DEFAULT NULL,
	  `traffic_total` float DEFAULT NULL,
	  `traffic_percent` float DEFAULT NULL,
	  `traffic_per_sec` float DEFAULT NULL,
	  `hostname` varchar(32) DEFAULT NULL,
	  `script_name` varchar(128) DEFAULT NULL
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='report5';

DROP TABLE IF EXISTS report_by_hostname_and_server;

CREATE TABLE `report_by_hostname_and_server` (
	  `req_count` int(11) DEFAULT NULL,
	  `req_per_sec` float DEFAULT NULL,
	  `req_time_total` float DEFAULT NULL,
	  `req_time_percent` float DEFAULT NULL,
	  `req_time_per_sec` float DEFAULT NULL,
	  `ru_utime_total` float DEFAULT NULL,
	  `ru_utime_percent` float DEFAULT NULL,
	  `ru_utime_per_sec` float DEFAULT NULL,
	  `ru_stime_total` float DEFAULT NULL,
	  `ru_stime_percent` float DEFAULT NULL,
	  `ru_stime_per_sec` float DEFAULT NULL,
	  `traffic_total` float DEFAULT NULL,
	  `traffic_percent` float DEFAULT NULL,
	  `traffic_per_sec` float DEFAULT NULL,
	  `hostname` varchar(32) DEFAULT NULL,
	  `server_name` varchar(64) DEFAULT NULL
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='report6';

DROP TABLE IF EXISTS report_by_hostname_server_and_script;

CREATE TABLE `report_by_hostname_server_and_script` (
	  `req_count` int(11) DEFAULT NULL,
	  `req_per_sec` float DEFAULT NULL,
	  `req_time_total` float DEFAULT NULL,
	  `req_time_percent` float DEFAULT NULL,
	  `req_time_per_sec` float DEFAULT NULL,
	  `ru_utime_total` float DEFAULT NULL,
	  `ru_utime_percent` float DEFAULT NULL,
	  `ru_utime_per_sec` float DEFAULT NULL,
	  `ru_stime_total` float DEFAULT NULL,
	  `ru_stime_percent` float DEFAULT NULL,
	  `ru_stime_per_sec` float DEFAULT NULL,
	  `traffic_total` float DEFAULT NULL,
	  `traffic_percent` float DEFAULT NULL,
	  `traffic_per_sec` float DEFAULT NULL,
	  `hostname` varchar(32) DEFAULT NULL,
	  `server_name` varchar(64) DEFAULT NULL,
	  `script_name` varchar(128) DEFAULT NULL
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='report7';

