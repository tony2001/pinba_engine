CREATE TABLE `%name%` (
	`index_value` varchar(256) NOT NULL,
	`segment` int(11) DEFAULT NULL,
	`time_value` float DEFAULT NULL,
	`cnt` int(11) DEFAULT NULL,
	`percent` float DEFAULT NULL,
	KEY `index_value` (`index_value`(85))
) ENGINE=PINBA DEFAULT COMMENT='hv.%parent_table_type%:%tags%:%conditions%'
