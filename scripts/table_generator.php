<?php

/* {{{ colors */
define("RED", "\033[31m");
define("BOLD", "\033[1m");
define("GREEN", "\033[32m");
define("NOCOLOR", "\033[0m");
/* }}} */

$table_types = array( /* {{{ */
	'info'			=> "overall info report",
	'report1'		=> "report aggregated by script name",
	'report2'		=> "report aggregated by domain name",
	'report3'		=> "report aggregated by hostname",
	'report4'		=> "report aggregated by domain name and script name",
	'report5'		=> "report aggregated by hostname and script name",
	'report6'		=> "report aggregated by hostname and domain name",
	'report7'		=> "report aggregated by hostname, domain name and script name",
	'report8'		=> "report aggregated by HTTP status",
	'report9'		=> "report aggregated by script name and HTTP status",
	'report10'		=> "report aggregated by domain name and HTTP status",
	'report11'		=> "report aggregated by hostname and HTTP status",
	'report12'		=> "report aggregated by hostname, script name and HTTP status",
	'report13'		=> "report aggregated by schema",
	'report14'		=> "report aggregated by schema and script name",
	'report15'		=> "report aggregated by schema and domain name",
	'report16'		=> "report aggregated by schema and hostname",
	'report17'		=> "report aggregated by schema, hostname and script name",
	'report18'		=> "report aggregated by schema, hostname and HTTP status",
	'tag_report'	=> "tag report aggregated by script name and tag value",
	'tag_report2'	=> "tag report aggregated by script name, domain name, hostname and tag value",
	'tag2_report'	=> "tag report aggregated by script name and values of 2 tags",
	'tag2_report2'	=> "tag report aggregated by script name, domain name, hostname and values of 2 tags",
	'tagN_report'	=> "tag report aggregated by script name and values of N tags",
	'tagN_report2'	=> "tag report aggregated by script name, domain name, hostname and values of 2 tags",
	'tag_info'		=> "tag report aggregated by tag value",
	'tag2_info'		=> "tag report aggregated by values of 2 tags",
	'tagN_info'		=> "tag report aggregated by values of N tags",
	'histogram'		=> "histogram data for a report"
);
/* }}} */

function read_value(&$value, $format, $allowed_values = NULL, $empty_ok = false) /* {{{ */
{
	$handle = fopen ("php://stdin","r");
	if (!$handle) {
		return false;
	}

	while(true) {
		if ($format == "%l") {
			$value = fgets($handle);
			if ($value) {
				$value = trim($value);
			}
			$ret = strlen($value);
		} else {
			$ret = fscanf($handle, $format, $value);
		}
		if ((!$empty_ok && $ret <= 0) || ($allowed_values && !in_array($value, $allowed_values))) {
			echo RED."Invalid answer '$value'.";
			if ($allowed_values) { 
				echo "Choose from the list above: ".NOCOLOR;
			} else {
				echo "Type again: ".NOCOLOR;
			}
			continue;
		}
		break;
	}
	fclose($handle);
	return $value;
}
/* }}} */

function process_template($template, $values) /* {{{ */
{
	$tpl_file = dirname(__FILE__)."/templates/$template.tpl";
	$tpl = file_get_contents($tpl_file);
	if (!$tpl) {
		echo RED."Failed to read template file ".$tpl_file."\n".NOCOLOR;
		return NULL;
	}

	return str_replace(array_keys($values), array_values($values), $tpl);
}
/* }}} */

system("clear");

echo GREEN."CREATE statements generator for Pinba.".NOCOLOR."
Choose ".BOLD."table type".NOCOLOR." from the list below:\n";

$i = 1;
foreach ($table_types as $id=>$name) {
	echo BOLD, $i, NOCOLOR, " ", $name, "\n"; 
	$i++;
}	

echo "Type your choice: ";

$table_type_num = -1;
$hv_table_type_num = -1;

$ret = read_value($table_type_num, "%d", range(1, count($table_types)));
if (!$ret) {
	exit;
}

$keys = array_keys($table_types);
$table_type_name = $keys[$table_type_num - 1];
$hv_table_type_name = "";
$timer_tags_arr = array();
echo "Okay, you've chosen: ".BOLD.$table_types[$table_type_name].NOCOLOR." (".$table_type_name.")\n\n";

if ($table_type_name == "histogram") {
	echo BOLD."You have to make sure all histogram options match the parent report options.\n".NOCOLOR;
	echo "Now choose ".BOLD."parent table type".NOCOLOR." for histogram from the list above: ";

parent_table_type:

	$ret = read_value($hv_table_type_num, "%d", range(1, count($table_types)));
	if (!$ret) {
		exit;
	}

	$hv_table_type_name = $keys[$hv_table_type_num - 1];
	if ($hv_table_type_name == "histogram" || $hv_table_type_name == "info") {
		echo RED."$hv_table_type_name cannot be a parent table for histogram, choose again: ".NOCOLOR;
		goto parent_table_type;
	}
}

if (strstr($table_type_name, "tag") || ($table_type_name == "histogram" && strstr($hv_table_type_name, "tag"))) {

	$tmp_table_type_name = $hv_table_type_name ? $hv_table_type_name : $table_type_name;

	echo "Now choose ".BOLD."timer tag(s) (comma separated list of tag names)".NOCOLOR.": ";

timer_tags:

	$timer_tags = "";

	$ret = read_value($timer_tags, "%l", NULL, false);
	if (!$ret) {
		exit;
	}

	$timer_tags_arr = explode(",", $timer_tags);
	foreach ($timer_tags_arr as $key=>$tag) {
		$timer_tags_arr[$key] = trim($tag);
		if (strstr($tag, ":")) {
			echo RED."Timer tag name cannot contain colon, please choose a different tag name.\n".NOCOLOR;
			goto timer_tags;
		}
		if (!$timer_tags_arr[$key]) {
			unset($timer_tags_arr[$key]);
		}
	}
	$timer_tags_arr = array_unique($timer_tags_arr);

	switch ($tmp_table_type_name) {
		case "tag_info":
		case "tag_report":
		case "tag_report2":
			if (count($timer_tags_arr) > 1) {
				echo RED."Only one tag is allowed for table type '".$tmp_table_type_name."', please choose tag again: ".NOCOLOR;
				goto timer_tags;
			}
			break;
		case "tag2_info":
		case "tag2_report":
		case "tag2_report2":
			if (count($timer_tags_arr) > 2) {
				echo RED."Only two tags are allowed for table type '".$tmp_table_type_name."', please choose tags again: ".NOCOLOR;
				goto timer_tags;
			} else if (count($timer_tags_arr) != 2) {
				echo RED."Two tags are required for table type '".$tmp_table_type_name."', please choose tags again: ".NOCOLOR;
				goto timer_tags;
			}
			break;
	}
	
	if (count($timer_tags_arr) == 0) {
		echo RED."Timer tags cannot by empty for table type '".$tmp_table_type_name."', please choose tag(s) again: ".NOCOLOR;
		goto timer_tags;
	}

	$timer_tags_str = join(", ", $timer_tags_arr);
	echo "Timer tag(s) you've chosen: ".$timer_tags_str."\n";
}

echo "Choose conditions (press Enter to skip).\n";

min_time:

$min_time = "";
echo BOLD."Minimal request time (seconds, float): ".NOCOLOR;
$ret = read_value($min_time, "%f", NULL, true);

$max_time = "";
echo BOLD."Maximal request time (seconds, float): ".NOCOLOR;
$ret = read_value($max_time, "%f", NULL, true);

if ($max_time != 0 && $max_time <= $min_time) {
	echo RED."max_time is less then min_time. This makes no sense, please try again\n.".NOCOLOR;
	goto min_time;
}

request_tags:

$request_tags = "";
echo BOLD."Request tags".NOCOLOR." (comma separated list of 'tag=value' combinations): ";
$ret = read_value($request_tags, "%l", NULL, true);

$request_tags_str = "";
$request_tags_arr = array();
if ($request_tags) {
	$request_tags_arr = array();
	$tmp_request_tags_arr = explode(",", $request_tags);
	foreach ($tmp_request_tags_arr as $key=>$tagvalue) {
		$tmp_request_tags_arr[$key] = trim($tagvalue);
		if (!$tmp_request_tags_arr[$key]) {
			continue;
		}

		$tmp_arr = explode("=", $tagvalue);
		if (!$tmp_arr || count($tmp_arr) != 2) {
			echo RED."Invalid request tag/value pair found (".$tagvalue."), please correct it and type again.\n".NOCOLOR;
			goto request_tags;
		}

		$tag = trim($tmp_arr[0]);
		$value = trim($tmp_arr[1]);

		if (!$tag || !$value) {
			echo RED."Tag name and tag value cannot be empty, try again.\n".NOCOLOR;
			goto request_tags;
		}
		if (strstr($tag, ":") || strstr($value, ":")) {
			echo RED."Tag name and tag value cannot contain colon, please choose a different one.\n".NOCOLOR;
			goto request_tags;
		}
		$request_tags_arr[$tmp_arr[0]] = $value;
	}
	$request_tags_str = "";
	foreach ($request_tags_arr as $tag=>$value) {
		$request_tags_str .= "tag.".$tag."=".$value.",";
	}
	$request_tags_str = rtrim($request_tags_str, ",");
}

echo "Okay, you've chosen:
  ".BOLD."min time".NOCOLOR.": ".($min_time ? sprintf("%.2f", $min_time) : "unset")."
  ".BOLD."max time".NOCOLOR.": ".($max_time ? sprintf("%.2f", $max_time) : "unset")."
  ".BOLD."request tags".NOCOLOR.": ".($request_tags_str ? $request_tags_str : "unset")."\n";

$percentiles_arr = array();
if ($table_type_name != "histogram") {
	percentile:

		$percentiles = "";
	echo BOLD."Percentiles".NOCOLOR." (comma separated list of integers < 100, press Enter to skip): ";
	$ret = read_value($percentiles, "%l", NULL, true);

	$percentiles_str = "";
	if ($percentiles) {
		$percentiles_arr = explode(",", $percentiles);
		foreach ($percentiles_arr as $key=>$per) {
			$percentiles_arr[$key] = (int)$per;
			if ($percentiles_arr[$key] <= 0 || $percentiles_arr[$key] >= 100) {
				echo RED."Percentiles must be greater than 0 and less than 100, please type a different value.\n".NOCOLOR;
				goto percentile;
			}
		}
		$percentiles_arr = array_unique($percentiles_arr);
		sort($percentiles_arr);
		$percentiles_str = join(", ", $percentiles_arr);
	}

	echo "Okay, you've chosen: ".($percentiles_str ? $percentiles_str : "none")."\n";
}

$table_name = "";
echo "Enter ".BOLD."table name".NOCOLOR.": ";
$ret = read_value($table_name, "%l", NULL, false);
echo "\n\n";

echo "Here are all the choises you've made, check them out and press Enter if everything is correct.\n";
echo BOLD."  Table type: ".NOCOLOR.$table_types[$table_type_name]." (".$table_type_name.")\n";

if ($table_type_name == "histogram") {
	echo BOLD."  Histogram parent report type: ".NOCOLOR.$table_types[$hv_table_type_name]." (".$hv_table_type_name.")\n";
} 

if (strstr($table_type_name, "tag") || ($table_type_name == "histogram" && strstr($hv_table_type_name, "tag"))) {
	echo BOLD."  Timer tag(s): ".NOCOLOR.$timer_tags_str."\n";
}

echo BOLD."  Minimal request time: ".NOCOLOR.($min_time ? sprintf("%.2f", $min_time) : "unset")."\n";
echo BOLD."  Maximal request time: ".NOCOLOR.($max_time ? sprintf("%.2f", $max_time) : "unset")."\n";
echo BOLD."  Request tags: ".NOCOLOR.($request_tags_str ? $request_tags_str : "unset")."\n";

if ($table_type_name != "histogram") {
	echo BOLD."  Percentiles: ".NOCOLOR.($percentiles_str ? $percentiles_str : "none")."\n";
}

echo BOLD."  Table name: ".NOCOLOR.$table_name."\n";

read_value($dummy, "%l", NULL, true);

$percentile_columns = "";
$percentiles_str = join(",", $percentiles_arr);
foreach ($percentiles_arr as $percentile) {
	$percentile_columns .= "`p$percentile` float DEFAULT NULL,\n\t";
}
$percentile_columns = rtrim($percentile_columns, "\n\t");
$percentile_columns = ltrim($percentile_columns);

$conditions = array();
if ($min_time) {
	$conditions[] = "min_time=".sprintf("%.2f", $min_time);
}

if ($max_time) {
	$conditions[] = "max_time=".sprintf("%.2f", $max_time);
}

if ($request_tags_str) {
	$conditions[] = $request_tags_str;
}

$conditions_str = join(",", $conditions);
$timer_tags_str = join(",", $timer_tags_arr);

$tag_value_columns = "";
if (strstr($table_type_name, "tagN") || ($table_type_name == "histogram" && strstr($hv_table_type_name, "tagN"))) {
	$i = 1;
	foreach ($timer_tags_arr as $tag) {
		$tag_value_columns .= "	`tag".$i."_value` varchar(64) DEFAULT NULL,\n";
	}
	$tag_value_columns = rtrim($tag_value_columns, ",\n");
	$tag_value_columns = ltrim($tag_value_columns);
}

$values = array(
	"%name%" => $table_name,
	"%conditions%" => $conditions_str,
	"%percentile_columns%" => $percentile_columns,
	"%percentiles%" => $percentiles_str,
	"%tags%" => $timer_tags_str,
	"%parent_table_type%" => $hv_table_type_name,
	"%tag_value_columns%" => $tag_value_columns,
);

echo process_template($table_type_name, $values), "\n";

?>
