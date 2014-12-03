<?php

$REPORTS = array(
	array( /* info */
		'id' => "_info",
		'no_index' => true,
	),
	array( /* report by script name */
		'id' => 1,
		'index_d' => 'const uint8_t *index',
		'create_index' => 'index = (const uint8_t *)record->data.script_name',
		'assign_data' => ''
	),
	array( /* report by server name (domain name) */
		'id' => 2,
		'index_d' => 'const uint8_t *index',
		'create_index' => 'index = (const uint8_t *)record->data.server_name',
		'assign_data' => ''
	),
	array( /* report by hostname */
		'id' => 3,
		'index_d' => 'const uint8_t *index',
		'create_index' => 'index = (const uint8_t *)record->data.hostname',
		'assign_data' => ''
	),
	array( /* report by server name and script name */
		'id' => 4,
		'index_d' => 'uint8_t index[PINBA_SERVER_NAME_SIZE + PINBA_SCRIPT_NAME_SIZE + 1] = {0}',
		'create_index' =>
		"
		memcpy_static(index, record->data.server_name, record->data.server_name_len, index_len);
		(index_len < sizeof(index)-1) ? index[index_len++] = '/' : 0;
		memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);
		",
		'assign_data' =>
		"
		memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);
		memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
		"
	),
	array( /* report by hostname and script name */
		'id' => 5,
		'index_d' => 'uint8_t index[PINBA_HOSTNAME_SIZE + PINBA_SCRIPT_NAME_SIZE + 1] = {0}',
		'create_index' =>
		"
		memcpy_static(index, record->data.hostname, record->data.hostname_len, index_len);
		(index_len < sizeof(index)-1) ? index[index_len++] = '/' : 0;
		memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);
		",
		'assign_data' =>
		"
		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
		"
	),
	array( /* report by hostname and server name */
		'id' => 6,
		'index_d' => 'uint8_t index[PINBA_HOSTNAME_SIZE + PINBA_SERVER_NAME_SIZE + 1] = {0}',
		'create_index' =>
		"
		memcpy_static(index, record->data.hostname, record->data.hostname_len, index_len);
		(index_len < sizeof(index)-1) ? index[index_len++] = '/' : 0;
		memcat_static(index, index_len, record->data.server_name, record->data.server_name_len, index_len);
		",
		'assign_data' =>
		"
		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);
		"
	),
	array( /* report by hostname, server name and script name */
		'id' => 7,
		'index_d' => 'uint8_t index[PINBA_HOSTNAME_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE] = {0}',
		'create_index' =>
		"
		memcpy_static(index, record->data.hostname, record->data.hostname_len, index_len);
		(index_len < sizeof(index)-1) ? index[index_len++] = ':' : 0;
		memcat_static(index, index_len, record->data.server_name, record->data.server_name_len, index_len);
		(index_len < sizeof(index)-1) ? index[index_len++] = '/' : 0;
		memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);
		",
		'assign_data' =>
		"
		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);
		memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
		"
	),
	array( /* report by HTTP status */
		'id' => 8,
		'index_d' => 'uint8_t index[PINBA_STATUS_SIZE] = {0}',
		'create_index' =>
		'
		sprintf((char *)index, "%u", record->data.status);
		',
		'assign_data' =>
		"
		data->status = record->data.status;
		"
	),
	array( /* report by HTTP status and script name */
		'id' => 9,
		'index_d' => 'uint8_t index[PINBA_STATUS_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE] = {0}',
		'create_index' =>
		'
        index_len = sprintf((char *)index, "%u:", record->data.status);
        memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);
		',
		'assign_data' =>
		"
		data->status = record->data.status;
		memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
		"
	),
	array( /* report by HTTP status and server name */
		'id' => 10,
		'index_d' => 'uint8_t index[PINBA_STATUS_SIZE + 1 + PINBA_SERVER_NAME_SIZE] = {0}',
		'create_index' =>
		'
		index_len = sprintf((char *)index, "%u", record->data.status);
		memcat_static(index, index_len, record->data.server_name, record->data.server_name_len, index_len);
		',
		'assign_data' =>
		"
		data->status = record->data.status;
		memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);
		"
	),
	array( /* report by HTTP status and hostname */
		'id' => 11,
		'index_d' => 'uint8_t index[PINBA_HOSTNAME_SIZE + 1 + PINBA_STATUS_SIZE] = {0}',
		'create_index' =>
		'
		index_len = sprintf((char *)index, "%u", record->data.status);
		memcat_static(index, index_len, record->data.hostname, record->data.hostname_len, index_len);
		',
		'assign_data' =>
		"
		data->status = record->data.status;
		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		"
	),
	array( /* report by HTTP status, hostname and script name */
		'id' => 12,
		'index_d' => 'uint8_t index[PINBA_STATUS_SIZE + 1 + PINBA_HOSTNAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE] = {0}',
		'create_index' => <<<C
		index_len = sprintf((char *)index, "%u", record->data.status);
		memcat_static(index, index_len, record->data.hostname, record->data.hostname_len, index_len);
		(index_len < sizeof(index)-1) ? index[index_len++] = '/' : 0;
		memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);
C
		,
		'assign_data' =>
		"
		data->status = record->data.status;
		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
		"
	),
	array( /* report by schema */
		'id' => 13,
		'index_d' => 'const uint8_t *index',
		'create_index' => 'index = (const uint8_t *)record->data.schema',
		'assign_data' =>
		''
	),
	array( /* report by schema and script name */
		'id' => 14,
		'index_d' => 'uint8_t index[PINBA_SCHEMA_SIZE + PINBA_SCRIPT_NAME_SIZE + 1] = {0}',
		'create_index' =>
		"
		memcpy_static(index, record->data.schema, record->data.schema_len, index_len);
		(index_len < sizeof(index)-1) ? index[index_len++] = ':' : 0;
		memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);
		",
		'assign_data' =>
		"
		memcpy_static(data->schema, record->data.schema, record->data.schema_len, dummy);
		memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
		"
	),
	array( /* report by schema and server name */
		'id' => 15,
		'index_d' => 'uint8_t index[PINBA_SCHEMA_SIZE + PINBA_SERVER_NAME_SIZE + 1] = {0}',
		'create_index' =>
		"
		memcpy_static(index, record->data.schema, record->data.schema_len, index_len);
		(index_len < sizeof(index)-1) ? index[index_len++] = ':' : 0;
		memcat_static(index, index_len, record->data.server_name, record->data.server_name_len, index_len);
		",
		'assign_data' =>
		"
		memcpy_static(data->schema, record->data.schema, record->data.schema_len, dummy);
		memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);
		"
	),
	array( /* report by schema and hostname */
		'id' => 16,
		'index_d' => 'uint8_t index[PINBA_SCHEMA_SIZE + PINBA_HOSTNAME_SIZE + 1] = {0}',
		'create_index' =>
		"
		memcpy_static(index, record->data.schema, record->data.schema_len, index_len);
		(index_len < sizeof(index)-1) ? index[index_len++] = ':' : 0;
		memcat_static(index, index_len, record->data.hostname, record->data.hostname_len, index_len);
		",
		'assign_data' =>
		"
		memcpy_static(data->schema, record->data.schema, record->data.schema_len, dummy);
		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		"
	),
	array( /* report by schema, hostname and script name */
		'id' => 17,
		'index_d' => 'uint8_t index[PINBA_SCHEMA_SIZE + 1 + PINBA_HOSTNAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE] = {0}',
		'create_index' =>
		"
		memcpy_static(index, record->data.schema, record->data.schema_len, index_len);
		(index_len < sizeof(index)-1) ? index[index_len++] = '/' : 0;
		memcat_static(index, index_len, record->data.hostname, record->data.hostname_len, index_len);
		(index_len < sizeof(index)-1) ? index[index_len++] = '/' : 0;
		memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);
		",
		'assign_data' =>
		"
		memcpy_static(data->schema, record->data.schema, record->data.schema_len, dummy);
		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
		"
	),
	array( /* report by schema, hostname and status */
		'id' => 18,
		'index_d' => 'uint8_t index[PINBA_SCHEMA_SIZE + 1 + PINBA_HOSTNAME_SIZE + 1 + PINBA_STATUS_SIZE] = {0}',
		'create_index' => <<<C
		index_len = sprintf((char *)index, "%u:", record->data.status);
		memcat_static(index, index_len, record->data.schema, record->data.schema_len, index_len);
		(index_len < sizeof(index)-1) ? index[index_len++] = '/' : 0;
		memcat_static(index, index_len, record->data.hostname, record->data.hostname_len, index_len);
C
		,
		'assign_data' =>
		"
		data->status = record->data.status;
		memcpy_static(data->schema, record->data.schema, record->data.schema_len, dummy);
		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		"
	),
);

$CWD = dirname(__FILE__);

$FILES = array(
	"pinba_regenerate_report_tpl.h"=>"pinba_regenerate_report.h",
	"pinba_update_report_tpl.h"=>"pinba_update_report.h",
	"pinba_update_report_proto_tpl.h"=>"pinba_update_report_proto.h",
);

$TAGS = array(
	'PINBA_REGENERATE_REPORT_FUNC_D' => 'pinba_regenerate_report#id#(PINBA_SHARE *share)',
	'PINBA_REPORT_DATA_STRUCT_D' => 'struct pinba_report#id#_data *data',
	'PINBA_REPORT_DATA_STRUCT' => 'struct pinba_report#id#_data',
	'PINBA_REPORT_INDEX_D' => '#index_d#',
	'PINBA_INDEX_VARS_D' => 'size_t index_len, dummy',
	'PINBA_REPORT_ID' => 'PINBA_TABLE_REPORT#ID#',
	'PINBA_UPDATE_REPORT_ADD_FUNC_D' => 'pinba_update_report#id#_add(size_t request_id, pinba_report *report, const pinba_stats_record *record)',
	'PINBA_UPDATE_REPORT_ADD_FUNC' => 'pinba_update_report#id#_add',
	'PINBA_UPDATE_REPORT_DELETE_FUNC_D' => 'pinba_update_report#id#_delete(size_t request_id, pinba_report *report, const pinba_stats_record *record)',
	'PINBA_UPDATE_REPORT_DELETE_FUNC' => 'pinba_update_report#id#_delete',
	'PINBA_REPORT_NO_INDEX' => '#no_index#',
	'PINBA_CREATE_INDEX_VALUE' => '#create_index#',
	'PINBA_REPORT_ASSIGN_DATA' => '#assign_data#',
);

$HEADER = <<<HEADER
/* This file is autogenerated, edit the original template instead! */

/* Copyright (c) 2007-2013 Antony Dovgal <tony@daylessday.org>

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

HEADER;

function process_1st_pass($contents, $tags) /* {{{ */
{
	foreach($tags as $tag=>$value) {
		$contents = str_replace($tag."()", $value, $contents);
	}
	return $contents;
}
/* }}} */

function process_2nd_pass($contents, $report) /* {{{ */
{
	foreach($report as $key=>$value) {
		$contents = str_replace('#'.$key.'#', $value, $contents);
	}

	/* special case for UPPERCASE id */
	if (isset($report['id'])) {
		$contents = str_replace('#ID#', strtoupper($report['id']), $contents);
	}

	/* special case for no_index */
	if (!isset($report['no_index'])) {
		$contents = str_replace('#no_index#', '0', $contents);
	}

	/* nuke the leftovers */
	$contents = preg_replace('/(#([0-9a-zA_Z_]+)#)*/', '', $contents);
	return $contents;
}
/* }}} */

foreach ($FILES as $tpl=>$output) {
	$processed_text = $HEADER;

	$template_text = file_get_contents($CWD.'/'.$tpl);
	$template_text = process_1st_pass($template_text, $TAGS);

	foreach ($REPORTS as $report) {
		$processed_text .= process_2nd_pass($template_text, $report);
	}

	file_put_contents($output, $processed_text);
}

?>
