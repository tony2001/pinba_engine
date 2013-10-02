CREATE TABLE `%name%` (
	`req_count` int(11) DEFAULT NULL,
	`time_total` float DEFAULT NULL,
	`ru_utime_total` float DEFAULT NULL,
	`ru_stime_total` float DEFAULT NULL,
	`time_interval` int(11) DEFAULT NULL,
	`kbytes_total` float DEFAULT NULL,
	`memory_footprint` float DEFAULT NULL,
	`req_time_median` float DEFAULT NULL,
	`index_value` varchar(256) DEFAULT NULL
	%percentile_columns%
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='%name%::%conditions%:%percentiles%';
