CREATE TABLE `%name%` (
	`tag_value` varchar(64) DEFAULT NULL,
	`req_count` int(11) DEFAULT NULL,
	`req_per_sec` float DEFAULT NULL,
	`hit_count` int(11) DEFAULT NULL,
	`hit_per_sec` float DEFAULT NULL,
	`timer_value` float DEFAULT NULL,
	`timer_median` float DEFAULT NULL,
	`index_value` varchar(256) DEFAULT NULL
	%percentile_columns%
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='tag_info:%tags%:%conditions%:%percentiles%'
