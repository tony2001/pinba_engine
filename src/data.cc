/* Copyright (c) 2007-2009 Antony Dovgal <tony@daylessday.org>

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

#include "pinba.h"
#include "pinba_update_report.h"
#include "tag_report_map.h"

void pinba_update_tag_info_add(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report = (pinba_tag_report *)rep;
	struct pinba_tag_info_data *data;
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, tag_found;
	pinba_word *word;


	for (i = 0; i < record->timers_cnt; i++) {
		tag_found = 0;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag_id[0] == timer->tag_ids[j]) {
				tag_found = 1;
				break;
			}
		}

		if (!tag_found) {
			continue;
		}

		word = (pinba_word *)timer->tag_values[j];
		ppvalue = JudySLGet(report->results, (uint8_t *)word->str, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {

			ppvalue = JudySLIns(&report->results, (uint8_t *)word->str, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				continue;
			}

			data = (struct pinba_tag_info_data *)calloc(1, sizeof(struct pinba_tag_info_data));
			if (!data) {
				continue;
			}

			data->req_count = 1;
			data->hit_count = timer->hit_count;
			data->timer_value = timer->value;
			data->prev_add_request_id = request_id;
			data->prev_del_request_id = -1;

			*ppvalue = data;
			report->std.results_cnt++;
		} else {
			data = (struct pinba_tag_info_data *)*ppvalue;
			data->hit_count += timer->hit_count;
			timeradd(&data->timer_value, &timer->value, &data->timer_value);
		}
		timeradd(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
		timeradd(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
		PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

		/* count tag values only once per request */
		if (request_id != data->prev_add_request_id) {
			data->req_count++;
			data->prev_add_request_id = request_id;
		}
	}
}
/* }}} */

void pinba_update_tag_info_delete(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report = (pinba_tag_report *)rep;
	struct pinba_tag_info_data *data;
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, tag_found;
	pinba_word *word;

	PINBA_REPORT_DELETE_CHECK(report, record);

	for (i = 0; i < record->timers_cnt; i++) {
		tag_found = 0;
		timer = record_get_timer(&D->timer_pool, record, i);

		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag_id[0] == timer->tag_ids[j]) {
				tag_found = 1;
				break;
			}
		}

		if (!tag_found) {
			continue;
		}

		word = (pinba_word *)timer->tag_values[j];

		ppvalue = JudySLGet(report->results, (uint8_t *)word->str, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			continue;
		} else {
			data = (struct pinba_tag_info_data *)*ppvalue;

			/* count tag values only once per request */
			if (request_id != data->prev_del_request_id) {
				data->req_count--;
				data->prev_del_request_id = request_id;
			}

			if (UNLIKELY(data->req_count == 0)) {
				free(data);
				JudySLDel(&report->results, (uint8_t *)word->str, NULL);
				report->std.results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
				timersub(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
				timersub(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
				PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data->histogram_data, timer->value, timer->hit_count);
			}
		}
	}
}
/* }}} */

void pinba_update_tag2_info_add(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report = (pinba_tag_report *)rep;
	struct pinba_tag2_info_data *data;
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, tag1_pos, tag2_pos, dummy;
	pinba_word *word1, *word2;
	int index_len;
	uint8_t index_val[PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE];


	for (i = 0; i < record->timers_cnt; i++) {
		tag1_pos = -1;
		tag2_pos = -1;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag_id[0] == timer->tag_ids[j]) {
				tag1_pos = j;
				continue;
			}
			if (report->tag_id[1] == timer->tag_ids[j]) {
				tag2_pos = j;
				continue;
			}
		}

		if (tag1_pos < 0 || tag2_pos < 0) {
			continue;
		}

		word1 = (pinba_word *)timer->tag_values[tag1_pos];
		word2 = (pinba_word *)timer->tag_values[tag2_pos];

		memcpy_static(index_val, word1->str, word1->len, index_len);
		index_val[index_len] = '|'; index_len++;
		memcat_static(index_val, index_len, word2->str, word2->len, index_len);

		ppvalue = JudySLGet(report->results, index_val, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {

			ppvalue = JudySLIns(&report->results, index_val, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				continue;
			}

			data = (struct pinba_tag2_info_data *)calloc(1, sizeof(struct pinba_tag2_info_data));
			if (!data) {
				continue;
			}

			data->req_count = 1;
			data->hit_count = timer->hit_count;
			data->timer_value = timer->value;
			data->prev_add_request_id = request_id;
			data->prev_del_request_id = -1;

			memcpy_static(data->tag1_value, word1->str, word1->len, dummy);
			memcpy_static(data->tag2_value, word2->str, word2->len, dummy);
			(void)dummy;

			*ppvalue = data;
			report->std.results_cnt++;
		} else {
			data = (struct pinba_tag2_info_data *)*ppvalue;
			data->hit_count += timer->hit_count;
			timeradd(&data->timer_value, &timer->value, &data->timer_value);
		}
		timeradd(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
		timeradd(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
		PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

		/* count tag values only once per request */
		if (request_id != data->prev_add_request_id) {
			data->req_count++;
			data->prev_add_request_id = request_id;
		}
	}
}
/* }}} */

void pinba_update_tag2_info_delete(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report = (pinba_tag_report *)rep;
	struct pinba_tag2_info_data *data;
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, tag1_pos, tag2_pos;
	int index_len;
	pinba_word *word1, *word2;
	uint8_t index_val[PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE];

	PINBA_REPORT_DELETE_CHECK(report, record);

	for (i = 0; i < record->timers_cnt; i++) {
		tag1_pos = -1;
		tag2_pos = -1;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag_id[0] == timer->tag_ids[j]) {
				tag1_pos = j;
				continue;
			}
			if (report->tag_id[1] == timer->tag_ids[j]) {
				tag2_pos = j;
				continue;
			}
		}

		if (tag1_pos < 0 || tag2_pos < 0) {
			continue;
		}

		word1 = (pinba_word *)timer->tag_values[tag1_pos];
		word2 = (pinba_word *)timer->tag_values[tag2_pos];

		memcpy_static(index_val, word1->str, word1->len, index_len);
		index_val[index_len] = '|'; index_len++;
		memcat_static(index_val, index_len, word2->str, word2->len, index_len);

		ppvalue = JudySLGet(report->results, index_val, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			continue;
		} else {
			data = (struct pinba_tag2_info_data *)*ppvalue;

			/* count tag values only once per request */
			if (request_id != data->prev_del_request_id) {
				data->req_count--;
				data->prev_del_request_id = request_id;
			}

			if (UNLIKELY(data->req_count == 0)) {
				free(data);
				JudySLDel(&report->results, (uint8_t *)index_val, NULL);
				report->std.results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
				timersub(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
				timersub(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
				PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data->histogram_data, timer->value, timer->hit_count);
			}
		}
	}
}
/* }}} */

void pinba_update_tag_report_add(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report = (pinba_tag_report *)rep;
	struct pinba_tag_report_data *data;
	PPvoid_t ppvalue_script;
	pinba_timer_record *timer;
	int i, j, tag_found, dummy;
	pinba_word *word;

	ppvalue_script = NULL;
	for (i = 0; i < record->timers_cnt; i++) {
		tag_found = 0;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag_id[0] == timer->tag_ids[j]) {
				tag_found = 1;
				break;
			}
		}

		if (!tag_found) {
			continue;
		}

		word = (pinba_word *)timer->tag_values[j];

		if (!ppvalue_script) {
			ppvalue_script = JudySLIns(&report->results, (uint8_t *)record->data.script_name, NULL);
			if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
				continue;
			}
		}

		data = (struct pinba_tag_report_data *)tag_report_map_get(*ppvalue_script, word->str);


		if (UNLIKELY(!data)) {
			data = (struct pinba_tag_report_data *)calloc(1, sizeof(struct pinba_tag_report_data));
			if (!data) {
				continue;
			}

			data->req_count = 1;
			data->hit_count = timer->hit_count;
			data->timer_value = timer->value;
			data->prev_add_request_id = request_id;
			data->prev_del_request_id = -1;

			memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
			memcpy_static(data->tag_value, word->str, word->len, dummy);
			(void)dummy;

			*ppvalue_script = tag_report_map_add(*ppvalue_script, word->str, data);
			report->std.results_cnt++;
		} else {
			data->hit_count += timer->hit_count;
			timeradd(&data->timer_value, &timer->value, &data->timer_value);
		}
		timeradd(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
		timeradd(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
		PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

		/* count tag values only once per request */
		if (request_id != data->prev_add_request_id) {
			data->req_count++;
			data->prev_add_request_id = request_id;
		}
	}
}
/* }}} */

void pinba_update_tag_report_delete(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report = (pinba_tag_report *)rep;
	struct pinba_tag_report_data *data;
	PPvoid_t ppvalue_script;
	pinba_timer_record *timer;
	int i, j, tag_found;
	pinba_word *word;

	PINBA_REPORT_DELETE_CHECK(report, record);

	ppvalue_script = JudySLGet(report->results, (uint8_t *)record->data.script_name, NULL);

	if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
		return;
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag_found = 0;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag_id[0] == timer->tag_ids[j]) {
				tag_found = 1;
				break;
			}
		}

		if (!tag_found) {
			continue;
		}

		word = (pinba_word *)timer->tag_values[j];

		if (!ppvalue_script) {
			continue;
		}

		data = (struct pinba_tag_report_data *)tag_report_map_get(*ppvalue_script, word->str);

		if (!data) {
			continue;
		} else {
			/* count tag values only once per request */
			if (request_id != data->prev_del_request_id) {
				data->req_count--;
				data->prev_del_request_id = request_id;
			}

			if (UNLIKELY(data->req_count == 0)) {

				if (tag_report_map_del(*ppvalue_script, word->str) < 0) {
					tag_report_destroy(*ppvalue_script);
					JudySLDel(&report->results, (uint8_t *)record->data.script_name, NULL);
					ppvalue_script = NULL;
				}

				free(data);
				report->std.results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
				timersub(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
				timersub(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
				PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data->histogram_data, timer->value, timer->hit_count);
			}
		}
	}
}
/* }}} */

void pinba_update_tag2_report_add(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report = (pinba_tag_report *)rep;
	struct pinba_tag2_report_data *data;
	PPvoid_t ppvalue_script;
	pinba_timer_record *timer;
	int i, j, tag1_pos, tag2_pos, dummy;
	int index_len;
	char index_val[PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1];
	pinba_word *word1, *word2;

	ppvalue_script = NULL;
	for (i = 0; i < record->timers_cnt; i++) {
		tag1_pos = -1;
		tag2_pos = -1;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag_id[0] == timer->tag_ids[j]) {
				tag1_pos = j;
				continue;
			}
			if (report->tag_id[1] == timer->tag_ids[j]) {
				tag2_pos = j;
				continue;
			}
		}

		if (tag1_pos < 0 || tag2_pos < 0) {
			continue;
		}

		word1 = (pinba_word *)timer->tag_values[tag1_pos];
		word2 = (pinba_word *)timer->tag_values[tag2_pos];

		memcpy_static(index_val, word1->str, word1->len, index_len);
		index_val[index_len] = '|'; index_len++;
		memcat_static(index_val, index_len, word2->str, word2->len, index_len);

		if (!ppvalue_script) {
			ppvalue_script = JudySLIns(&report->results, (uint8_t *)record->data.script_name, NULL);
			if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
				continue;
			}
		}

		data = (struct pinba_tag2_report_data *)tag_report_map_get(*ppvalue_script, index_val);

		if (UNLIKELY(!data)) {

			data = (struct pinba_tag2_report_data *)calloc(1, sizeof(struct pinba_tag2_report_data));
			if (UNLIKELY(!data)) {
				continue;
			}

			data->req_count = 1;
			data->hit_count = timer->hit_count;
			data->timer_value = timer->value;
			data->prev_add_request_id = request_id;
			data->prev_del_request_id = -1;

			memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
			memcpy_static(data->tag1_value, word1->str, word1->len, dummy);
			memcpy_static(data->tag2_value, word2->str, word2->len, dummy);
			(void)dummy;

			*ppvalue_script = tag_report_map_add(*ppvalue_script, index_val, data);
			report->std.results_cnt++;
		} else {
			data->hit_count += timer->hit_count;
			timeradd(&data->timer_value, &timer->value, &data->timer_value);
		}
		timeradd(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
		timeradd(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
		PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

		/* count tag values only once per request */
		if (request_id != data->prev_add_request_id) {
			data->req_count++;
			data->prev_add_request_id = request_id;
		}
	}
}
/* }}} */

void pinba_update_tag2_report_delete(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report = (pinba_tag_report *)rep;
	struct pinba_tag2_report_data *data;
	PPvoid_t ppvalue_script;
	pinba_timer_record *timer;
	int i, j, tag1_pos, tag2_pos;
	int index_len;
	char index_val[PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1];
	pinba_word *word1, *word2;

	PINBA_REPORT_DELETE_CHECK(report, record);

	ppvalue_script = JudySLGet(report->results, (uint8_t *)record->data.script_name, NULL);
	if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
		return;
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag1_pos = -1;
		tag2_pos = -1;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag_id[0] == timer->tag_ids[j]) {
				tag1_pos = j;
				continue;
			}
			if (report->tag_id[1] == timer->tag_ids[j]) {
				tag2_pos = j;
				continue;
			}
		}

		if (tag1_pos < 0 || tag2_pos < 0) {
			continue;
		}

		word1 = (pinba_word *)timer->tag_values[tag1_pos];
		word2 = (pinba_word *)timer->tag_values[tag2_pos];

		memcpy_static(index_val, word1->str, word1->len, index_len);
		index_val[index_len] = '|'; index_len++;
		memcat_static(index_val, index_len, word2->str, word2->len, index_len);

		if (!ppvalue_script) {
			continue;
		}

		data = (struct pinba_tag2_report_data*)tag_report_map_get(*ppvalue_script, index_val);
		if (UNLIKELY(!data)) {
			continue;
		} else {
			/* count tag values only once per request */
			if (request_id != data->prev_del_request_id) {
				data->req_count--;
				data->prev_del_request_id = request_id;
			}

			if (UNLIKELY(data->req_count == 0)) {
				free(data);
				if (tag_report_map_del(*ppvalue_script, index_val) < 0) {
					tag_report_destroy(*ppvalue_script);
					JudySLDel(&report->results, (uint8_t *)record->data.script_name, NULL);
					ppvalue_script = NULL;
				}
				report->std.results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
				timersub(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
				timersub(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
				PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data->histogram_data, timer->value, timer->hit_count);
			}
		}
	}
}
/* }}} */

void pinba_update_tag_report2_add(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report = (pinba_tag_report *)rep;
	struct pinba_tag_report2_data *data;
	PPvoid_t ppvalue_script;
	pinba_timer_record *timer;
	int i, j, tag_found, index_len, dummy;
	char index[PINBA_SCRIPT_NAME_SIZE + PINBA_HOSTNAME_SIZE + PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE];
	pinba_word *word;

	ppvalue_script = NULL;
	for (i = 0; i < record->timers_cnt; i++) {
		tag_found = 0;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag_id[0] == timer->tag_ids[j]) {
				tag_found = 1;
				break;
			}
		}

		if (!tag_found) {
			continue;
		}

		word = (pinba_word *)timer->tag_values[j];

		memcpy_static(index, record->data.hostname, (int)record->data.hostname_len, index_len);
		index[index_len] = '|'; index_len++;
		memcat_static(index, index_len, record->data.server_name, (int)record->data.server_name_len, index_len);
		index[index_len] = '|'; index_len++;
		memcat_static(index, index_len, word->str, word->len, index_len);

		if (!ppvalue_script) {
			ppvalue_script = JudySLIns(&report->results, (uint8_t *)record->data.script_name, NULL);
			if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
				continue;
			}
		}

		data = (struct pinba_tag_report2_data*)tag_report_map_get(*ppvalue_script, index);

		if (UNLIKELY(!data)) {

			data = (struct pinba_tag_report2_data *)calloc(1, sizeof(struct pinba_tag_report2_data));
			if (!data) {
				continue;
			}

			data->req_count = 1;
			data->hit_count = timer->hit_count;
			data->timer_value = timer->value;
			data->prev_add_request_id = request_id;
			data->prev_del_request_id = -1;

			memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
			memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);
			memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
			memcpy_static(data->tag_value, word->str, word->len, dummy);
			(void)dummy;

			*ppvalue_script = tag_report_map_add(*ppvalue_script, index, data);
			report->std.results_cnt++;
		} else {

			data->hit_count += timer->hit_count;
			timeradd(&data->timer_value, &timer->value, &data->timer_value);
		}
		timeradd(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
		timeradd(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
		PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

		/* count tag values only once per request */
		if (request_id != data->prev_add_request_id) {
			data->req_count++;
			data->prev_add_request_id = request_id;
		}
	}
}
/* }}} */

void pinba_update_tag_report2_delete(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report = (pinba_tag_report *)rep;
	struct pinba_tag_report2_data *data;
	PPvoid_t ppvalue_script;
	pinba_timer_record *timer;
	int i, j, tag_found, index_len;
	char index[PINBA_HOSTNAME_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE];
	pinba_word *word;

	PINBA_REPORT_DELETE_CHECK(report, record);

	ppvalue_script = JudySLGet(report->results, (uint8_t *)record->data.script_name, NULL);

	if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
		return;
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag_found = 0;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag_id[0] == timer->tag_ids[j]) {
				tag_found = 1;
				break;
			}
		}

		if (!tag_found) {
			continue;
		}

		word = (pinba_word *)timer->tag_values[j];

		if (!ppvalue_script) {
			continue;
		}

		memcpy_static(index, record->data.hostname, (int)record->data.hostname_len, index_len);
		index[index_len] = '|'; index_len++;
		memcat_static(index, index_len, record->data.server_name, (int)record->data.server_name_len, index_len);
		index[index_len] = '|'; index_len++;
		memcat_static(index, index_len, word->str, word->len, index_len);


		data = (struct pinba_tag_report2_data *)tag_report_map_get(*ppvalue_script, index);

		if (!data) {
			continue;
		} else {

			/* count tag values only once per request */
			if (request_id != data->prev_del_request_id) {
				data->req_count--;
				data->prev_del_request_id = request_id;
			}

			if (UNLIKELY(data->req_count == 0)) {
				free(data);
				if (tag_report_map_del(*ppvalue_script, index) < 0) {
					tag_report_destroy(*ppvalue_script);
					JudySLDel(&report->results, (uint8_t *)record->data.script_name, NULL);
					ppvalue_script = NULL;
				}
				report->std.results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
				timersub(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
				timersub(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
				PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data->histogram_data, timer->value, timer->hit_count);
			}
		}
	}
}
/* }}} */

void pinba_update_tag2_report2_add(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report = (pinba_tag_report *)rep;
	struct pinba_tag2_report2_data *data;
	PPvoid_t  ppvalue_script;
	pinba_timer_record *timer;
	int i, j, tag1_pos, tag2_pos, dummy;
	int index_len;
	char index_val[PINBA_HOSTNAME_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE];
	pinba_word *word1, *word2;

	ppvalue_script = NULL;
	for (i = 0; i < record->timers_cnt; i++) {
		tag1_pos = -1;
		tag2_pos = -1;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag_id[0] == timer->tag_ids[j]) {
				tag1_pos = j;
				continue;
			}
			if (report->tag_id[1] == timer->tag_ids[j]) {
				tag2_pos = j;
				continue;
			}
		}

		if (tag1_pos < 0 || tag2_pos < 0) {
			continue;
		}

		word1 = (pinba_word *)timer->tag_values[tag1_pos];
		word2 = (pinba_word *)timer->tag_values[tag2_pos];

		memcpy_static(index_val, record->data.hostname, (int)record->data.hostname_len, index_len);
		index_val[index_len] = '|'; index_len++;
		memcat_static(index_val, index_len, record->data.server_name, (int)record->data.server_name_len, index_len);
		index_val[index_len] = '|'; index_len++;
		memcat_static(index_val, index_len, word1->str, word1->len, index_len);
		index_val[index_len] = '|'; index_len++;
		memcat_static(index_val, index_len, word2->str, word2->len, index_len);

        if (!ppvalue_script) {
            ppvalue_script = JudySLIns(&report->results, (uint8_t *)record->data.script_name, NULL);
            if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
                continue;
            }
        }

		data = (struct pinba_tag2_report2_data*)tag_report_map_get(*ppvalue_script, index_val);

        if (UNLIKELY(!data)) {
			data = (struct pinba_tag2_report2_data *)calloc(1, sizeof(struct pinba_tag2_report2_data));
			if (UNLIKELY(!data)) {
				continue;
			}

			data->req_count = 1;
			data->hit_count = timer->hit_count;
			data->timer_value = timer->value;
			data->prev_add_request_id = request_id;
			data->prev_del_request_id = -1;

			memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
			memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);
			memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
			memcpy_static(data->tag1_value, word1->str, word1->len, dummy);
			memcpy_static(data->tag2_value, word2->str, word2->len, dummy);
			(void)dummy;

			*ppvalue_script = tag_report_map_add(*ppvalue_script, index_val, data);
			report->std.results_cnt++;
		} else {
			data->hit_count += timer->hit_count;
			timeradd(&data->timer_value, &timer->value, &data->timer_value);
		}
		timeradd(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
		timeradd(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
		PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

		/* count tag values only once per request */
		if (request_id != data->prev_add_request_id) {
			data->req_count++;
			data->prev_add_request_id = request_id;
		}
	}
}
/* }}} */

void pinba_update_tag2_report2_delete(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report = (pinba_tag_report *)rep;
	struct pinba_tag2_report2_data *data;
	PPvoid_t ppvalue_script;
	pinba_timer_record *timer;
	int i, j, tag1_pos, tag2_pos;
	int index_len;
	char index_val[PINBA_HOSTNAME_SIZE + PINBA_SERVER_NAME_SIZE + PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE];
	pinba_word *word1, *word2;

	PINBA_REPORT_DELETE_CHECK(report, record);

	ppvalue_script = JudySLGet(report->results, (uint8_t *)record->data.script_name, NULL);
	if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
		return;
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag1_pos = -1;
		tag2_pos = -1;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag_id[0] == timer->tag_ids[j]) {
				tag1_pos = j;
				continue;
			}
			if (report->tag_id[1] == timer->tag_ids[j]) {
				tag2_pos = j;
				continue;
			}
		}

		if (tag1_pos < 0 || tag2_pos < 0) {
			continue;
		}

		word1 = (pinba_word *)timer->tag_values[tag1_pos];
		word2 = (pinba_word *)timer->tag_values[tag2_pos];

		memcpy_static(index_val, record->data.hostname, (int)record->data.hostname_len, index_len);
		index_val[index_len] = '|'; index_len++;
		memcat_static(index_val, index_len, record->data.server_name, (int)record->data.server_name_len, index_len);
		index_val[index_len] = '|'; index_len++;
		memcat_static(index_val, index_len, word1->str, word1->len, index_len);
		index_val[index_len] = '|'; index_len++;
		memcat_static(index_val, index_len, word2->str, word2->len, index_len);

		if (!ppvalue_script) {
			continue;
		}

		data = (struct pinba_tag2_report2_data*)tag_report_map_get(*ppvalue_script, index_val);

		if (UNLIKELY(!data)) {
			continue;
		} else {
			/* count tag values only once per request */
			if (request_id != data->prev_del_request_id) {
				data->req_count--;
				data->prev_del_request_id = request_id;
			}

			if (UNLIKELY(data->req_count == 0)) {
				free(data);
				if (tag_report_map_del(*ppvalue_script, index_val) < 0) {
					tag_report_destroy(*ppvalue_script);
					JudySLDel(&report->results, (uint8_t *)record->data.script_name, NULL);
					ppvalue_script = NULL;
				}
				report->std.results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
				timersub(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
				timersub(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
				PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data->histogram_data, timer->value, timer->hit_count);
			}
		}
	}
}
/* }}} */

void pinba_update_tagN_info_add(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report = (pinba_tag_report *)rep;
	struct pinba_tagN_info_data *data;
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, k, found_tags_cnt, h;
	int index_len;
	pinba_word *word;

	for (i = 0; i < record->timers_cnt; i++) {
		found_tags_cnt = 0;

		timer = record_get_timer(&D->timer_pool, record, i);

		if (timer->tag_num < report->tags_cnt) {
			continue;
		}

		for (h = 0; h < report->tags_cnt; h++) {
			int found = 0, tag_id = report->tag_id[h];

			for (j = 0; j < timer->tag_num; j++) {
				if (tag_id == timer->tag_ids[j]) {
					report->words[h] = (pinba_word *)timer->tag_values[j];
					found_tags_cnt++;
					if (found_tags_cnt == report->tags_cnt) {
						goto jump_ahead;
					}
					found = 1;
				}
			}

			if (!found) {
				break;
			}
		}

		if (found_tags_cnt != report->tags_cnt) {
			continue;
		}

jump_ahead:

		index_len = 0;
		for (k = 0; k < report->tags_cnt; k++) {
			word = report->words[k];
			memcpy(report->index + index_len, word->str, word->len);
			index_len += word->len;
			report->index[index_len] = '|';
			index_len ++;
		}
		report->index[index_len] = '\0';

		ppvalue = JudySLGet(report->results, report->index, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			ppvalue = JudySLIns(&report->results, report->index, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				continue;
			}

			data = (struct pinba_tagN_info_data *)calloc(1, sizeof(struct pinba_tagN_info_data));
			if (UNLIKELY(!data)) {
				continue;
			}

			data->tag_value = (char *)calloc(report->tags_cnt, PINBA_TAG_VALUE_SIZE);
			if (UNLIKELY(!data->tag_value)) {
				free(data);
				continue;
			}

			data->req_count = 1;
			data->hit_count = timer->hit_count;
			data->timer_value = timer->value;
			data->prev_add_request_id = request_id;
			data->prev_del_request_id = -1;

			for (k = 0; k < report->tags_cnt; k++) {
				word = report->words[k];
				memcpy(data->tag_value + PINBA_TAG_VALUE_SIZE * k, word->str, word->len);
			}

			*ppvalue = data;
			report->std.results_cnt++;
		} else {
			data = (struct pinba_tagN_info_data *)*ppvalue;
			data->hit_count += timer->hit_count;
			timeradd(&data->timer_value, &timer->value, &data->timer_value);
		}
		timeradd(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
		timeradd(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
		PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

		/* count tag values only once per request */
		if (request_id != data->prev_add_request_id) {
			data->req_count++;
			data->prev_add_request_id = request_id;
		}
	}
}
/* }}} */

void pinba_update_tagN_info_delete(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report = (pinba_tag_report *)rep;
	struct pinba_tagN_info_data *data;
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, k, found_tags_cnt, h;
	int index_len;
	pinba_word *word;

	PINBA_REPORT_DELETE_CHECK(report, record);

	for (i = 0; i < record->timers_cnt; i++) {
		found_tags_cnt = 0;

		timer = record_get_timer(&D->timer_pool, record, i);

		if (timer->tag_num < report->tags_cnt) {
			continue;
		}

		for (h = 0; h < report->tags_cnt; h++) {
			int found = 0, tag_id = report->tag_id[h];

			for (j = 0; j < timer->tag_num; j++) {
				if (tag_id == timer->tag_ids[j]) {
					report->words[h] = (pinba_word *)timer->tag_values[j];
					found_tags_cnt++;
					if (found_tags_cnt == report->tags_cnt) {
						goto jump_ahead;
					}
					found = 1;
				}
			}

			if (!found) {
				break;
			}
		}

		if (found_tags_cnt != report->tags_cnt) {
			continue;
		}

jump_ahead:

		index_len = 0;
		for (k = 0; k < report->tags_cnt; k++) {
			word = report->words[k];
			memcpy(report->index + index_len, word->str, word->len);
			index_len += word->len;
			report->index[index_len] = '|';
			index_len ++;
		}
		report->index[index_len] = '\0';

		ppvalue = JudySLGet(report->results, report->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			continue;
		} else {
			data = (struct pinba_tagN_info_data *)*ppvalue;

			/* count tag values only once per request */
			if (request_id != data->prev_del_request_id) {
				data->req_count--;
				data->prev_del_request_id = request_id;
			}

			if (UNLIKELY(data->req_count == 0)) {
				free(data->tag_value);
				free(data);
				JudySLDel(&report->results, (uint8_t *)report->index, NULL);
				report->std.results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
				timersub(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
				timersub(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
				PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data->histogram_data, timer->value, timer->hit_count);
			}
		}
	}
}
/* }}} */

void pinba_update_tagN_report_add(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report = (pinba_tag_report *)rep;
	struct pinba_tagN_report_data *data;
	PPvoid_t ppvalue, ppvalue_script;
	pinba_timer_record *timer;
	int i, j, k, found_tags_cnt, dummy, h;
	int index_len;
	pinba_word *word;

	ppvalue_script = NULL;
	for (i = 0; i < record->timers_cnt; i++) {
		found_tags_cnt = 0;

		timer = record_get_timer(&D->timer_pool, record, i);

		for (h = 0; h < report->tags_cnt; h++) {
			int found = 0, tag_id = report->tag_id[h];

			for (j = 0; j < timer->tag_num; j++) {
				if (tag_id == timer->tag_ids[j]) {
					report->words[h] = (pinba_word *)timer->tag_values[j];
					found_tags_cnt++;
					if (found_tags_cnt == report->tags_cnt) {
						goto jump_ahead;
					}
					found = 1;
				}
			}

			if (!found) {
				break;
			}
		}

		if (found_tags_cnt != report->tags_cnt) {
			continue;
		}

jump_ahead:

		if (!ppvalue_script) {
			ppvalue_script = JudySLIns(&report->results, (uint8_t *)record->data.script_name, NULL);
			if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
				continue;
			}
		}

		index_len = 0;
		for (k = 0; k < report->tags_cnt; k++) {
			word = report->words[k];
			memcpy(report->index + index_len, word->str, word->len);
			index_len += word->len;
			report->index[index_len] = '|';
			index_len ++;
		}
		report->index[index_len] = '\0';

		ppvalue = JudySLGet(*ppvalue_script, report->index, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			ppvalue = JudySLIns(ppvalue_script, report->index, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				continue;
			}

			data = (struct pinba_tagN_report_data *)calloc(1, sizeof(struct pinba_tagN_report_data));
			if (UNLIKELY(!data)) {
				continue;
			}

			data->tag_value = (char *)calloc(report->tags_cnt, PINBA_TAG_VALUE_SIZE);
			if (UNLIKELY(!data->tag_value)) {
				free(data);
				continue;
			}

			data->req_count = 1;
			data->hit_count = timer->hit_count;
			data->timer_value = timer->value;
			data->prev_add_request_id = request_id;
			data->prev_del_request_id = -1;

			memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
			(void)dummy;
			for (k = 0; k < report->tags_cnt; k++) {
				word = report->words[k];
				memcpy(data->tag_value + PINBA_TAG_VALUE_SIZE * k, word->str, word->len);
			}

			*ppvalue = data;
			report->std.results_cnt++;
		} else {
			data = (struct pinba_tagN_report_data *)*ppvalue;
			data->hit_count += timer->hit_count;
			timeradd(&data->timer_value, &timer->value, &data->timer_value);
		}
		timeradd(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
		timeradd(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
		PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

		/* count tag values only once per request */
		if (request_id != data->prev_add_request_id) {
			data->req_count++;
			data->prev_add_request_id = request_id;
		}
	}
}
/* }}} */

void pinba_update_tagN_report_delete(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report = (pinba_tag_report *)rep;
	struct pinba_tagN_report_data *data;
	PPvoid_t ppvalue, ppvalue_script;
	pinba_timer_record *timer;
	int i, j, k, found_tags_cnt, h;
	int index_len;
	pinba_word *word;

	PINBA_REPORT_DELETE_CHECK(report, record);

	ppvalue_script = JudySLGet(report->results, (uint8_t *)record->data.script_name, NULL);
	if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
		return;
	}

	for (i = 0; i < record->timers_cnt; i++) {

		if (!ppvalue_script) {
			continue;
		}

		found_tags_cnt = 0;

		timer = record_get_timer(&D->timer_pool, record, i);

		for (h = 0; h < report->tags_cnt; h++) {
			int found = 0, tag_id = report->tag_id[h];

			for (j = 0; j < timer->tag_num; j++) {
				if (tag_id == timer->tag_ids[j]) {
					report->words[h] = (pinba_word *)timer->tag_values[j];
					found_tags_cnt++;
					if (found_tags_cnt == report->tags_cnt) {
						goto jump_ahead;
					}
					found = 1;
				}
			}

			if (!found) {
				break;
			}
		}

		if (found_tags_cnt != report->tags_cnt) {
			continue;
		}

jump_ahead:

		index_len = 0;
		for (k = 0; k < report->tags_cnt; k++) {
			word = report->words[k];
			memcpy(report->index + index_len, word->str, word->len);
			index_len += word->len;
			report->index[index_len] = '|';
			index_len ++;
		}
		report->index[index_len] = '\0';

		ppvalue = JudySLGet(*ppvalue_script, report->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			continue;
		} else {
			data = (struct pinba_tagN_report_data *)*ppvalue;

			/* count tag values only once per request */
			if (request_id != data->prev_del_request_id) {
				data->req_count--;
				data->prev_del_request_id = request_id;
			}

			if (UNLIKELY(data->req_count == 0)) {
				free(data->tag_value);
				free(data);
				JudySLDel(ppvalue_script, (uint8_t *)report->index, NULL);
				if (*ppvalue_script == NULL) {
					JudySLDel(&report->results, (uint8_t *)record->data.script_name, NULL);
					ppvalue_script = NULL;
				}
				report->std.results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
				timersub(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
				timersub(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
				PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data->histogram_data, timer->value, timer->hit_count);
			}
		}
	}
}
/* }}} */

void pinba_update_tagN_report2_add(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report = (pinba_tag_report *)rep;
	struct pinba_tagN_report2_data *data;
	PPvoid_t ppvalue, ppvalue_script;
	pinba_timer_record *timer;
	int i, j, k, found_tags_cnt, dummy, h;
	int index_len;
	pinba_word *word;

	ppvalue_script = NULL;
	for (i = 0; i < record->timers_cnt; i++) {
		found_tags_cnt = 0;

		timer = record_get_timer(&D->timer_pool, record, i);

		for (h = 0; h < report->tags_cnt; h++) {
			int found = 0, tag_id = report->tag_id[h];

			for (j = 0; j < timer->tag_num; j++) {
				if (tag_id == timer->tag_ids[j]) {
					report->words[h] = (pinba_word *)timer->tag_values[j];
					found_tags_cnt++;
					if (found_tags_cnt == report->tags_cnt) {
						goto jump_ahead;
					}
					found = 1;
				}
			}

			if (!found) {
				break;
			}
		}

		if (found_tags_cnt != report->tags_cnt) {
			continue;
		}

jump_ahead:

		if (!ppvalue_script) {
			ppvalue_script = JudySLIns(&report->results, (uint8_t *)record->data.script_name, NULL);
			if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
				continue;
			}
		}

		memcpy(report->index, record->data.hostname, record->data.hostname_len);
		index_len = record->data.hostname_len;
		report->index[index_len] = '|'; index_len++;
		memcpy(report->index + index_len, record->data.server_name, record->data.server_name_len);
		index_len += record->data.server_name_len;
		report->index[index_len] = '|'; index_len++;

		for (k = 0; k < report->tags_cnt; k++) {
			word = report->words[k];
			memcpy(report->index + index_len, word->str, word->len);
			index_len += word->len;
			report->index[index_len] = '|';
			index_len ++;
		}
		report->index[index_len] = '\0';

		ppvalue = JudySLGet(*ppvalue_script, report->index, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			ppvalue = JudySLIns(ppvalue_script, report->index, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				continue;
			}

			data = (struct pinba_tagN_report2_data *)calloc(1, sizeof(struct pinba_tagN_report2_data));
			if (UNLIKELY(!data)) {
				continue;
			}

			data->tag_value = (char *)calloc(report->tags_cnt, PINBA_TAG_VALUE_SIZE);
			if (UNLIKELY(!data->tag_value)) {
				free(data);
				continue;
			}

			data->req_count = 1;
			data->hit_count = timer->hit_count;
			data->timer_value = timer->value;
			data->prev_add_request_id = request_id;
			data->prev_del_request_id = -1;

			memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
			memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);
			memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
			(void)dummy;
			for (k = 0; k < report->tags_cnt; k++) {
				word = report->words[k];
				memcpy(data->tag_value + PINBA_TAG_VALUE_SIZE * k, word->str, word->len);
			}

			*ppvalue = data;
			report->std.results_cnt++;
		} else {
			data = (struct pinba_tagN_report2_data *)*ppvalue;
			data->hit_count += timer->hit_count;
			timeradd(&data->timer_value, &timer->value, &data->timer_value);
		}
		timeradd(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
		timeradd(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
		PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

		/* count tag values only once per request */
		if (request_id != data->prev_add_request_id) {
			data->req_count++;
			data->prev_add_request_id = request_id;
		}
	}
}
/* }}} */

void pinba_update_tagN_report2_delete(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report = (pinba_tag_report *)rep;
	struct pinba_tagN_report2_data *data;
	PPvoid_t ppvalue, ppvalue_script;
	pinba_timer_record *timer;
	int i, j, k, found_tags_cnt, h;
	int index_len;
	pinba_word *word;

	PINBA_REPORT_DELETE_CHECK(report, record);

	ppvalue_script = JudySLGet(report->results, (uint8_t *)record->data.script_name, NULL);
	if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
		return;
	}

	for (i = 0; i < record->timers_cnt; i++) {

		if (!ppvalue_script) {
			continue;
		}

		found_tags_cnt = 0;

		timer = record_get_timer(&D->timer_pool, record, i);

		for (h = 0; h < report->tags_cnt; h++) {
			int found = 0, tag_id = report->tag_id[h];

			for (j = 0; j < timer->tag_num; j++) {
				if (tag_id == timer->tag_ids[j]) {
					report->words[h] = (pinba_word *)timer->tag_values[j];
					found_tags_cnt++;
					if (found_tags_cnt == report->tags_cnt) {
						goto jump_ahead;
					}
					found = 1;
				}
			}

			if (!found) {
				break;
			}
		}

		if (found_tags_cnt != report->tags_cnt) {
			continue;
		}

jump_ahead:

		memcpy(report->index, record->data.hostname, record->data.hostname_len);
		index_len = record->data.hostname_len;
		report->index[index_len] = '|'; index_len++;
		memcpy(report->index + index_len, record->data.server_name, record->data.server_name_len);
		index_len += record->data.server_name_len;
		report->index[index_len] = '|'; index_len++;

		for (k = 0; k < report->tags_cnt; k++) {
			word = report->words[k];
			memcpy(report->index + index_len, word->str, word->len);
			index_len += word->len;
			report->index[index_len] = '|';
			index_len ++;
		}
		report->index[index_len] = '\0';

		ppvalue = JudySLGet(*ppvalue_script, report->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			continue;
		} else {
			data = (struct pinba_tagN_report2_data *)*ppvalue;

			/* count tag values only once per request */
			if (request_id != data->prev_del_request_id) {
				data->req_count--;
				data->prev_del_request_id = request_id;
			}

			if (UNLIKELY(data->req_count == 0)) {
				free(data->tag_value);
				free(data);
				JudySLDel(ppvalue_script, (uint8_t *)report->index, NULL);
				if (*ppvalue_script == NULL) {
					JudySLDel(&report->results, (uint8_t *)record->data.script_name, NULL);
					ppvalue_script = NULL;
				}
				report->std.results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
				timersub(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
				timersub(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
				PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data->histogram_data, timer->value, timer->hit_count);
			}
		}
	}
}
/* }}} */


void pinba_update_rtag_info_add(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_rtag_report *report = (pinba_rtag_report *)rep;
	struct pinba_rtag_info_data *data;
	PPvoid_t ppvalue;
	unsigned int i, tag_found = 0;
	pinba_word *word;

	for (i = 0; i < record->data.tags_cnt; i++) {
		if (report->tags[0] == record->data.tag_names[i]) {
			tag_found = 1;
			break;
		}
	}

	if (!tag_found) {
		return;
	}

	word = (pinba_word *)record->data.tag_values[i];
	ppvalue = JudySLGet(report->results, (uint8_t *)word->str, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {

		ppvalue = JudySLIns(&report->results, (uint8_t *)word->str, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return;
		}

		data = (struct pinba_rtag_info_data *)calloc(1, sizeof(struct pinba_rtag_info_data));
		if (!data) {
			return;
		}

		*ppvalue = data;
		report->std.results_cnt++;
	} else {
		data = (struct pinba_rtag_info_data *)*ppvalue;
	}

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;
	report->memory_footprint += record->data.memory_footprint;

	data->req_count++;
	timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
	timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
	timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
	data->kbytes_total += record->data.doc_size;
	data->memory_footprint += record->data.memory_footprint;
	PINBA_UPDATE_HISTOGRAM_ADD(report, data->histogram_data, record->data.req_time);
}
/* }}} */

void pinba_update_rtag_info_delete(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_rtag_report *report = (pinba_rtag_report *)rep;
	struct pinba_rtag_info_data *data;
	PPvoid_t ppvalue;
	unsigned int i, tag_found = 0;
	pinba_word *word;

	PINBA_REPORT_DELETE_CHECK(report, record);

	for (i = 0; i < record->data.tags_cnt; i++) {
		if (report->tags[0] == record->data.tag_names[i]) {
			tag_found = 1;
			break;
		}
	}

	if (!tag_found) {
		return;
	}

	word = (pinba_word *)record->data.tag_values[i];
	ppvalue = JudySLGet(report->results, (uint8_t *)word->str, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		return;
	} else {
		data = (struct pinba_rtag_info_data *)*ppvalue;

		timersub(&report->time_total, &record->data.req_time, &report->time_total);
		timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
		timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
		report->kbytes_total -= record->data.doc_size;
		report->memory_footprint -= record->data.memory_footprint;

		data->req_count--;

		if (UNLIKELY(data->req_count == 0)) {
			free(data);
			JudySLDel(&report->results, (uint8_t *)word->str, NULL);
			report->std.results_cnt--;
			return;
		} else {
			timersub(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timersub(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timersub(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total -= record->data.doc_size;
			data->memory_footprint -= record->data.memory_footprint;
			PINBA_UPDATE_HISTOGRAM_DEL(report, data->histogram_data, record->data.req_time);
		}
	}
}
/* }}} */

void pinba_update_rtag2_info_add(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_rtag_report *report = (pinba_rtag_report *)rep;
	struct pinba_rtag2_info_data *data;
	PPvoid_t ppvalue;
	unsigned int i;
	int tag1_pos = -1, tag2_pos = -1, index_len;
	pinba_word *word1, *word2;
	uint8_t index_val[PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1];

	for (i = 0; i < record->data.tags_cnt; i++) {
		if (report->tags[0] == record->data.tag_names[i]) {
			tag1_pos = i;
		} else if (report->tags[1] == record->data.tag_names[i]) {
			tag2_pos = i;
		}

		if (tag1_pos >= 0 && tag2_pos >= 0) {
			break;
		}
	}

	if (tag1_pos < 0 || tag2_pos < 0) {
		return;
	}

	word1 = (pinba_word *)record->data.tag_values[tag1_pos];
	word2 = (pinba_word *)record->data.tag_values[tag2_pos];

	memcpy_static(index_val, word1->str, word1->len, index_len);
	index_val[index_len] = '|'; index_len++;
	memcat_static(index_val, index_len, word2->str, word2->len, index_len);

	ppvalue = JudySLGet(report->results, (uint8_t *)index_val, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		int dummy;

		ppvalue = JudySLIns(&report->results, (uint8_t *)index_val, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return;
		}

		data = (struct pinba_rtag2_info_data *)calloc(1, sizeof(struct pinba_rtag2_info_data));
		if (!data) {
			return;
		}

		*ppvalue = data;

		memcpy_static(data->tag1_value, word1->str, word1->len, dummy);
		memcpy_static(data->tag2_value, word2->str, word2->len, dummy);
		(void)dummy;
		report->std.results_cnt++;
	} else {
		data = (struct pinba_rtag2_info_data *)*ppvalue;
	}

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;
	report->memory_footprint += record->data.memory_footprint;

	data->req_count++;
	timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
	timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
	timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
	data->kbytes_total += record->data.doc_size;
	data->memory_footprint += record->data.memory_footprint;
	PINBA_UPDATE_HISTOGRAM_ADD(report, data->histogram_data, record->data.req_time);
}
/* }}} */

void pinba_update_rtag2_info_delete(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_rtag_report *report = (pinba_rtag_report *)rep;
	struct pinba_rtag2_info_data *data;
	PPvoid_t ppvalue;
	unsigned int i;
	int tag1_pos = -1, tag2_pos = -1, index_len;
	pinba_word *word1, *word2;
	uint8_t index_val[PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1];

	PINBA_REPORT_DELETE_CHECK(report, record);

	for (i = 0; i < record->data.tags_cnt; i++) {
		if (report->tags[0] == record->data.tag_names[i]) {
			tag1_pos = i;
		} else if (report->tags[1] == record->data.tag_names[i]) {
			tag2_pos = i;
		}

		if (tag1_pos >= 0 && tag2_pos >= 0) {
			break;
		}
	}

	if (tag1_pos < 0 || tag2_pos < 0) {
		return;
	}

	word1 = (pinba_word *)record->data.tag_values[tag1_pos];
	word2 = (pinba_word *)record->data.tag_values[tag2_pos];

	memcpy_static(index_val, word1->str, word1->len, index_len);
	index_val[index_len] = '|'; index_len++;
	memcat_static(index_val, index_len, word2->str, word2->len, index_len);

	ppvalue = JudySLGet(report->results, (uint8_t *)index_val, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		return;
	} else {
		data = (struct pinba_rtag2_info_data *)*ppvalue;

		timersub(&report->time_total, &record->data.req_time, &report->time_total);
		timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
		timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
		report->kbytes_total -= record->data.doc_size;
		report->memory_footprint -= record->data.memory_footprint;

		data->req_count--;

		if (UNLIKELY(data->req_count == 0)) {
			free(data);
			JudySLDel(&report->results, (uint8_t *)index_val, NULL);
			report->std.results_cnt--;
			return;
		} else {
			timersub(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timersub(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timersub(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total -= record->data.doc_size;
			data->memory_footprint -= record->data.memory_footprint;
			PINBA_UPDATE_HISTOGRAM_DEL(report, data->histogram_data, record->data.req_time);
		}
	}
}
/* }}} */

void pinba_update_rtagN_info_add(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_rtag_report *report = (pinba_rtag_report *)rep;
	struct pinba_rtagN_info_data *data;
	PPvoid_t ppvalue;
	unsigned int i, j, found_tags_cnt = 0;
	int index_len;
	pinba_word *word;

	if (record->data.tags_cnt < report->tags_cnt) {
		return;
	}

	for (j = 0; j < report->tags_cnt; j++) {
		pinba_word *rtag = report->tags[j];
		int found = 0;

		for (i = 0; i < record->data.tags_cnt; i++) {
			if (rtag == record->data.tag_names[i]) {
				found = 1;
				report->values[j] = record->data.tag_values[i];
				break;
			}
		}

		if (!found) {
			break;
		}
		found_tags_cnt++;
	}

	if (found_tags_cnt != report->tags_cnt) {
		return;
	}

	index_len = 0;
	for (i = 0; i < report->tags_cnt; i++) {
		word = report->values[i];
		memcpy(report->index + index_len, word->str, word->len);
		index_len += word->len;
		report->index[index_len] = '|';
		index_len ++;
	}
	report->index[index_len] = '\0';

	ppvalue = JudySLGet(report->results, (uint8_t *)report->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		ppvalue = JudySLIns(&report->results, (uint8_t *)report->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return;
		}

		data = (struct pinba_rtagN_info_data *)calloc(1, sizeof(struct pinba_rtagN_info_data));
		if (!data) {
			return;
		}

		data->tag_value = (char *)calloc(report->tags_cnt, PINBA_TAG_VALUE_SIZE);
		if (!data->tag_value) {
			free(data);
			return;
		}

		*ppvalue = data;

		for (i = 0; i < report->tags_cnt; i++) {
			word = report->values[i];
			memcpy(data->tag_value + PINBA_TAG_VALUE_SIZE * i, word->str, word->len);
		}
		report->std.results_cnt++;
	} else {
		data = (struct pinba_rtagN_info_data *)*ppvalue;
	}

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;
	report->memory_footprint += record->data.memory_footprint;

	data->req_count++;
	timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
	timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
	timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
	data->kbytes_total += record->data.doc_size;
	data->memory_footprint += record->data.memory_footprint;
	PINBA_UPDATE_HISTOGRAM_ADD(report, data->histogram_data, record->data.req_time);
}
/* }}} */

void pinba_update_rtagN_info_delete(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_rtag_report *report = (pinba_rtag_report *)rep;
	struct pinba_rtagN_info_data *data;
	PPvoid_t ppvalue;
	unsigned int i, j, found_tags_cnt = 0;
	int index_len;
	pinba_word *word;

	PINBA_REPORT_DELETE_CHECK(report, record);

	if (record->data.tags_cnt < report->tags_cnt) {
		return;
	}

	for (j = 0; j < report->tags_cnt; j++) {
		pinba_word *rtag = report->tags[j];
		int found = 0;

		for (i = 0; i < record->data.tags_cnt; i++) {
			if (rtag == record->data.tag_names[i]) {
				found = 1;
				report->values[j] = record->data.tag_values[i];
				break;
			}
		}

		if (!found) {
			break;
		}
		found_tags_cnt++;
	}

	if (found_tags_cnt != report->tags_cnt) {
		return;
	}

	index_len = 0;
	for (i = 0; i < report->tags_cnt; i++) {
		word = report->values[i];
		memcpy(report->index + index_len, word->str, word->len);
		index_len += word->len;
		report->index[index_len] = '|';
		index_len ++;
	}
	report->index[index_len] = '\0';

	ppvalue = JudySLGet(report->results, (uint8_t *)report->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		return;
	} else {
		data = (struct pinba_rtagN_info_data *)*ppvalue;

		timersub(&report->time_total, &record->data.req_time, &report->time_total);
		timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
		timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
		report->kbytes_total -= record->data.doc_size;
		report->memory_footprint -= record->data.memory_footprint;

		data->req_count--;

		if (UNLIKELY(data->req_count == 0)) {
			free(data->tag_value);
			free(data);
			JudySLDel(&report->results, (uint8_t *)report->index, NULL);
			report->std.results_cnt--;
			return;
		} else {
			timersub(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timersub(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timersub(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total -= record->data.doc_size;
			data->memory_footprint -= record->data.memory_footprint;
			PINBA_UPDATE_HISTOGRAM_DEL(report, data->histogram_data, record->data.req_time);
		}
	}
}
/* }}} */

void pinba_update_rtag_report_add(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_rtag_report *report = (pinba_rtag_report *)rep;
	struct pinba_rtag_report_data *data;
	PPvoid_t ppvalue, ppvalue_host = NULL;
	unsigned int i, tag_found = 0;
	pinba_word *word;

	for (i = 0; i < record->data.tags_cnt; i++) {
		if (report->tags[0] == record->data.tag_names[i]) {
			tag_found = 1;
			break;
		}
	}

	if (!tag_found) {
		return;
	}

	word = (pinba_word *)record->data.tag_values[i];

	ppvalue_host = JudySLIns(&report->results, (uint8_t *)record->data.hostname, NULL);
	if (UNLIKELY(!ppvalue_host || ppvalue_host == PPJERR)) {
		return;
	}

	ppvalue = JudySLGet(*ppvalue_host, (uint8_t *)word->str, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		int dummy;

		ppvalue = JudySLIns(ppvalue_host, (uint8_t *)word->str, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return;
		}

		data = (struct pinba_rtag_report_data *)calloc(1, sizeof(struct pinba_rtag_report_data));
		if (!data) {
			return;
		}

		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		memcpy_static(data->tag_value, word->str, word->len, dummy);
		(void)dummy;

		*ppvalue = data;
		report->std.results_cnt++;
	} else {
		data = (struct pinba_rtag_report_data *)*ppvalue;
	}

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;
	report->memory_footprint += record->data.memory_footprint;

	data->req_count++;
	timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
	timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
	timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
	data->kbytes_total += record->data.doc_size;
	data->memory_footprint += record->data.memory_footprint;
	PINBA_UPDATE_HISTOGRAM_ADD(report, data->histogram_data, record->data.req_time);
}
/* }}} */

void pinba_update_rtag_report_delete(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_rtag_report *report = (pinba_rtag_report *)rep;
	struct pinba_rtag_report_data *data;
	PPvoid_t ppvalue, ppvalue_host = NULL;
	unsigned int i, tag_found = 0;
	pinba_word *word;

	PINBA_REPORT_DELETE_CHECK(report, record);

	ppvalue_host = JudySLGet(report->results, (uint8_t *)record->data.hostname, NULL);
	if (UNLIKELY(!ppvalue_host || ppvalue_host == PPJERR)) {
		return;
	}

	for (i = 0; i < record->data.tags_cnt; i++) {
		if (report->tags[0] == record->data.tag_names[i]) {
			tag_found = 1;
			break;
		}
	}

	if (!tag_found) {
		return;
	}

	word = (pinba_word *)record->data.tag_values[i];

	ppvalue = JudySLGet(*ppvalue_host, (uint8_t *)word->str, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		return;
	} else {
		data = (struct pinba_rtag_report_data *)*ppvalue;

		timersub(&report->time_total, &record->data.req_time, &report->time_total);
		timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
		timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
		report->kbytes_total -= record->data.doc_size;
		report->memory_footprint -= record->data.memory_footprint;

		data->req_count--;

		if (UNLIKELY(data->req_count == 0)) {
			free(data);
			JudySLDel(ppvalue_host, (uint8_t *)word->str, NULL);
			if (*ppvalue_host == NULL) {
				JudySLDel(&report->results, (uint8_t *)record->data.hostname, NULL);
			}
			report->std.results_cnt--;
			return;
		} else {
			timersub(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timersub(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timersub(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total -= record->data.doc_size;
			data->memory_footprint -= record->data.memory_footprint;
			PINBA_UPDATE_HISTOGRAM_DEL(report, data->histogram_data, record->data.req_time);
		}
	}
}
/* }}} */

void pinba_update_rtag2_report_add(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_rtag_report *report = (pinba_rtag_report *)rep;
	struct pinba_rtag2_report_data *data;
	PPvoid_t ppvalue_host = NULL;
	int tag1_pos = -1, tag2_pos = -1, index_len;
	unsigned int i;
	pinba_word *word1, *word2;
	char index_val[PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1];

	for (i = 0; i < record->data.tags_cnt; i++) {
		if (report->tags[0] == record->data.tag_names[i]) {
			tag1_pos = i;
		} else if (report->tags[1] == record->data.tag_names[i]) {
			tag2_pos = i;
		}

		if (tag1_pos >= 0 && tag2_pos >= 0) {
			break;
		}
	}

	if (tag1_pos < 0 || tag2_pos < 0) {
		return;
	}

	word1 = (pinba_word *)record->data.tag_values[tag1_pos];
	word2 = (pinba_word *)record->data.tag_values[tag2_pos];

	memcpy_static(index_val, word1->str, word1->len, index_len);
	index_val[index_len] = '|'; index_len++;
	memcat_static(index_val, index_len, word2->str, word2->len, index_len);

	ppvalue_host = JudySLIns(&report->results, (uint8_t *)record->data.hostname, NULL);
	if (UNLIKELY(!ppvalue_host || ppvalue_host == PPJERR)) {
		return;
	}

	data = (struct pinba_rtag2_report_data *)tag_report_map_get(*ppvalue_host, index_val);

	if (UNLIKELY(!data)) {
		int dummy;

		data = (struct pinba_rtag2_report_data *)calloc(1, sizeof(struct pinba_rtag2_report_data));
		if (!data) {
			return;
		}

		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		memcpy_static(data->tag1_value, word1->str, word1->len, dummy);
		memcpy_static(data->tag2_value, word2->str, word2->len, dummy);
		(void)dummy;

		*ppvalue_host = tag_report_map_add(*ppvalue_host, index_val, data);
		report->std.results_cnt++;
	}

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;
	report->memory_footprint += record->data.memory_footprint;

	data->req_count++;
	timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
	timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
	timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
	data->kbytes_total += record->data.doc_size;
	data->memory_footprint += record->data.memory_footprint;
	PINBA_UPDATE_HISTOGRAM_ADD(report, data->histogram_data, record->data.req_time);
}
/* }}} */

void pinba_update_rtag2_report_delete(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_rtag_report *report = (pinba_rtag_report *)rep;
	struct pinba_rtag2_report_data *data;
	PPvoid_t ppvalue_host = NULL;
	int tag1_pos = -1, tag2_pos = -1, index_len;
	unsigned int i;
	pinba_word *word1, *word2;
	char index_val[PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1];

	PINBA_REPORT_DELETE_CHECK(report, record);

	for (i = 0; i < record->data.tags_cnt; i++) {
		if (report->tags[0] == record->data.tag_names[i]) {
			tag1_pos = i;
		} else if (report->tags[1] == record->data.tag_names[i]) {
			tag2_pos = i;
		}

		if (tag1_pos >= 0 && tag2_pos >= 0) {
			break;
		}
	}

	if (tag1_pos < 0 || tag2_pos < 0) {
		return;
	}

	word1 = (pinba_word *)record->data.tag_values[tag1_pos];
	word2 = (pinba_word *)record->data.tag_values[tag2_pos];

	memcpy_static(index_val, word1->str, word1->len, index_len);
	index_val[index_len] = '|'; index_len++;
	memcat_static(index_val, index_len, word2->str, word2->len, index_len);

	ppvalue_host = JudySLGet(report->results, (uint8_t *)record->data.hostname, NULL);
	if (UNLIKELY(!ppvalue_host || ppvalue_host == PPJERR)) {
		return;
	}

	data = (struct pinba_rtag2_report_data *)tag_report_map_get(*ppvalue_host, index_val);
	if (UNLIKELY(!data)) {
		return;
	} else {

		timersub(&report->time_total, &record->data.req_time, &report->time_total);
		timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
		timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
		report->kbytes_total -= record->data.doc_size;
		report->memory_footprint -= record->data.memory_footprint;

		data->req_count--;

		if (UNLIKELY(data->req_count == 0)) {
			free(data);

			if (tag_report_map_del(*ppvalue_host, index_val) < 0) {
				tag_report_destroy(*ppvalue_host);
				JudySLDel(&report->results, (uint8_t *)record->data.hostname, NULL);
			}
			report->std.results_cnt--;
			return;
		} else {
			timersub(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timersub(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timersub(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total -= record->data.doc_size;
			data->memory_footprint -= record->data.memory_footprint;
			PINBA_UPDATE_HISTOGRAM_DEL(report, data->histogram_data, record->data.req_time);
		}
	}
}
/* }}} */

void pinba_update_rtagN_report_add(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_rtag_report *report = (pinba_rtag_report *)rep;
	struct pinba_rtagN_report_data *data;
	PPvoid_t ppvalue, ppvalue_host = NULL;
	unsigned int i, j, found_tags_cnt = 0;
	int index_len;
	pinba_word *word;

	if (record->data.tags_cnt < report->tags_cnt) {
		return;
	}

	for (j = 0; j < report->tags_cnt; j++) {
		pinba_word *rtag = report->tags[j];
		int found = 0;

		for (i = 0; i < record->data.tags_cnt; i++) {
			if (rtag == record->data.tag_names[i]) {
				found = 1;
				report->values[j] = record->data.tag_values[i];
				break;
			}
		}

		if (!found) {
			break;
		}
		found_tags_cnt++;
	}

	if (found_tags_cnt != report->tags_cnt) {
		return;
	}

	ppvalue_host = JudySLIns(&report->results, (uint8_t *)record->data.hostname, NULL);
	if (UNLIKELY(!ppvalue_host || ppvalue_host == PPJERR)) {
		return;
	}

	index_len = 0;
	for (i = 0; i < report->tags_cnt; i++) {
		word = report->values[i];
		memcpy(report->index + index_len, word->str, word->len);
		index_len += word->len;
		report->index[index_len] = '|';
		index_len ++;
	}
	report->index[index_len] = '\0';

	ppvalue = JudySLGet(*ppvalue_host, report->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		int dummy;

		ppvalue = JudySLIns(ppvalue_host, report->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return;
		}

		data = (struct pinba_rtagN_report_data *)calloc(1, sizeof(struct pinba_rtagN_report_data));
		if (!data) {
			return;
		}

		data->tag_value = (char *)calloc(report->tags_cnt, PINBA_TAG_VALUE_SIZE);
		if (!data->tag_value) {
			free(data);
			return;
		}

		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		(void)dummy;

		for (i = 0; i < report->tags_cnt; i++) {
			word = report->values[i];
			memcpy(data->tag_value + PINBA_TAG_VALUE_SIZE * i, word->str, word->len);
		}

		*ppvalue = data;
		report->std.results_cnt++;
	} else {
		data = (struct pinba_rtagN_report_data *)*ppvalue;
	}

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;
	report->memory_footprint += record->data.memory_footprint;

	data->req_count++;
	timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
	timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
	timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
	data->kbytes_total += record->data.doc_size;
	data->memory_footprint += record->data.memory_footprint;
	PINBA_UPDATE_HISTOGRAM_ADD(report, data->histogram_data, record->data.req_time);
}
/* }}} */

void pinba_update_rtagN_report_delete(size_t request_id, void *rep, const pinba_stats_record *record) /* {{{ */
{
	pinba_rtag_report *report = (pinba_rtag_report *)rep;
	struct pinba_rtagN_report_data *data;
	PPvoid_t ppvalue, ppvalue_host = NULL;
	unsigned int i, j, found_tags_cnt = 0;
	int index_len;
	pinba_word *word;

	PINBA_REPORT_DELETE_CHECK(report, record);

	if (record->data.tags_cnt < report->tags_cnt) {
		return;
	}

	for (j = 0; j < report->tags_cnt; j++) {
		pinba_word *rtag = report->tags[j];
		int found = 0;

		for (i = 0; i < record->data.tags_cnt; i++) {
			if (rtag == record->data.tag_names[i]) {
				found = 1;
				report->values[j] = record->data.tag_values[i];
				break;
			}
		}

		if (!found) {
			break;
		}
		found_tags_cnt++;
	}

	if (found_tags_cnt != report->tags_cnt) {
		return;
	}

	ppvalue_host = JudySLGet(report->results, (uint8_t *)record->data.hostname, NULL);
	if (UNLIKELY(!ppvalue_host || ppvalue_host == PPJERR)) {
		return;
	}

	index_len = 0;
	for (i = 0; i < report->tags_cnt; i++) {
		word = report->values[i];
		memcpy(report->index + index_len, word->str, word->len);
		index_len += word->len;
		report->index[index_len] = '|';
		index_len ++;
	}
	report->index[index_len] = '\0';

	ppvalue = JudySLGet(*ppvalue_host, report->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		return;
	} else {
		data = (struct pinba_rtagN_report_data *)*ppvalue;

		timersub(&report->time_total, &record->data.req_time, &report->time_total);
		timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
		timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
		report->kbytes_total -= record->data.doc_size;
		report->memory_footprint -= record->data.memory_footprint;

		data->req_count--;

		if (UNLIKELY(data->req_count == 0)) {
			free(data->tag_value);
			free(data);
			JudySLDel(ppvalue_host, report->index, NULL);
			if (*ppvalue_host == NULL) {
				JudySLDel(&report->results, (uint8_t *)record->data.hostname, NULL);
			}
			report->std.results_cnt--;
			return;
		} else {
			timersub(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timersub(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timersub(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total -= record->data.doc_size;
			data->memory_footprint -= record->data.memory_footprint;
			PINBA_UPDATE_HISTOGRAM_DEL(report, data->histogram_data, record->data.req_time);
		}
	}
}
/* }}} */


void pinba_update_add(pinba_array_t *array, size_t request_id, const pinba_stats_record *record) /* {{{ */
{
	pinba_std_report *report;
	unsigned int i;

	for (i = 0; i < array->size; i++) {
		report = (pinba_std_report *)array->data[i];

		CHECK_REPORT_CONDITIONS_CONTINUE(report, record);

		pthread_rwlock_wrlock(&report->lock);
		report->add_func(request_id, report, record);
		report->time_interval = pinba_get_time_interval(report);
		pthread_rwlock_unlock(&report->lock);
	}
}
/* }}} */

void pinba_update_delete(pinba_array_t *array, size_t request_id, const pinba_stats_record *record) /* {{{ */
{
	pinba_std_report *report;
	unsigned int i;

	for (i = 0; i < array->size; i++) {
		report = (pinba_std_report *)array->data[i];

		CHECK_REPORT_CONDITIONS_CONTINUE(report, record);

		pthread_rwlock_wrlock(&report->lock);
		report->delete_func(request_id, report, record);
		report->time_interval = pinba_get_time_interval(report);
		pthread_rwlock_unlock(&report->lock);
	}
}
/* }}} */


void pinba_report_results_dtor(pinba_report *report) /* {{{ */
{
	PPvoid_t ppvalue;
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};

	for (ppvalue = JudySLFirst(report->results, index, NULL); ppvalue != NULL && ppvalue != PPJERR; ppvalue = JudySLNext(report->results, index, NULL)) {
		free(*ppvalue);
	}
	JudySLFreeArray(&report->results, NULL);
	report->std.results_cnt = 0;
}
/* }}} */

void pinba_std_report_dtor(void *rprt) /* {{{ */
{
	pinba_std_report *std_report = (pinba_std_report *)rprt;
	unsigned int i;

	if (std_report->cond.tag_names) {
		for (i = 0; i < std_report->cond.tags_cnt; i++) {
			char *tag_name = std_report->cond.tag_names[i];
			free(tag_name);
		}
		free(std_report->cond.tag_names);
	}

	if (std_report->cond.tag_values) {
		for (i = 0; i < std_report->cond.tags_cnt; i++) {
			char *tag_value = std_report->cond.tag_values[i];
			free(tag_value);
		}
		free(std_report->cond.tag_values);
	}

	if (std_report->index) {
		free(std_report->index);
	}
	pthread_rwlock_destroy(&std_report->lock);
}
/* }}} */

void pinba_report_dtor(pinba_report *report, int lock_reports) /* {{{ */
{
	if (lock_reports) {
		pthread_rwlock_wrlock(&D->base_reports_lock);
	}

	JudySLDel(&D->base_reports, report->std.index, NULL);
	pinba_array_delete(&D->base_reports_arr, report);

	if (lock_reports) {
		pthread_rwlock_unlock(&D->base_reports_lock);
	}

	if (report->std.results_cnt) {
		pinba_report_results_dtor(report);

		report->std.time_interval = 0;
		report->std.results_cnt = 0;
		report->results = NULL;
		report->time_total.tv_sec = 0;
		report->time_total.tv_usec = 0;
		report->ru_utime_total.tv_sec = 0;
		report->ru_utime_total.tv_usec = 0;
		report->ru_stime_total.tv_sec = 0;
		report->ru_stime_total.tv_usec = 0;
		report->kbytes_total = 0;
	}
	pinba_std_report_dtor(report);
	free(report);
}
/* }}} */

void pinba_reports_destroy() /* {{{ */
{
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	PPvoid_t ppvalue;

	for (ppvalue = JudySLFirst(D->base_reports, index, NULL); ppvalue != NULL && ppvalue != PPJERR; ppvalue = JudySLNext(D->base_reports, index, NULL)) {
		pinba_report *report = (pinba_report *)*ppvalue;

		pinba_report_dtor(report, 0);
	}
	free(D->base_reports_arr.data);
	JudySLFreeArray(&D->base_reports, NULL);
}
/* }}} */

void pinba_tag_report_dtor(pinba_tag_report *report, int lock_tag_reports) /* {{{ */
{
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	PPvoid_t ppvalue;

	if (lock_tag_reports) {
		pthread_rwlock_wrlock(&D->tag_reports_lock);
	}

	JudySLDel(&D->tag_reports, report->std.index, NULL);
	pinba_array_delete(&D->tag_reports_arr, report);

	if (lock_tag_reports) {
		pthread_rwlock_unlock(&D->tag_reports_lock);
	}

	for (ppvalue = JudySLFirst(report->results, index, NULL); ppvalue != NULL; ppvalue = JudySLNext(report->results, index, NULL)) {
//		free(*ppvalue);
	}

	JudySLFreeArray(&report->results, NULL);

	pinba_std_report_dtor(report);
	free(report->tag_id);

	if (report->words) {
		free(report->words);
	}
	free(report);
}
/* }}} */

void pinba_tag_reports_destroy(void) /* {{{ */
{
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	PPvoid_t ppvalue;

	for (ppvalue = JudySLFirst(D->tag_reports, index, NULL); ppvalue != NULL; ppvalue = JudySLNext(D->tag_reports, index, NULL)) {
		pinba_tag_report *report = (pinba_tag_report *)*ppvalue;

		pinba_tag_report_dtor(report, 0);
	}
	free(D->tag_reports_arr.data);
	JudySLFreeArray(&D->tag_reports, NULL);
}
/* }}} */

void pinba_rtag_report_dtor(pinba_rtag_report *report, int lock) /* {{{ */
{
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	PPvoid_t ppvalue;

	if (lock) {
		pthread_rwlock_wrlock(&D->rtag_reports_lock);
	}

	JudySLDel(&D->rtag_reports, report->std.index, NULL);
	pinba_array_delete(&D->rtag_reports_arr, report);

	if (lock) {
		pthread_rwlock_unlock(&D->rtag_reports_lock);
	}

	for (ppvalue = JudySLFirst(report->results, index, NULL); ppvalue != NULL; ppvalue = JudySLNext(report->results, index, NULL)) {
//		free(*ppvalue);
	}

	JudySLFreeArray(&report->results, NULL);

	pinba_std_report_dtor(report);

	if (report->values) {
		free(report->values);
	}

	if (report->index) {
		free(report->index);
	}

	free(report->tags);
	free(report);
}
/* }}} */

void pinba_rtag_reports_destroy(void) /* {{{ */
{
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	PPvoid_t ppvalue;

	for (ppvalue = JudySLFirst(D->rtag_reports, index, NULL); ppvalue != NULL; ppvalue = JudySLNext(D->rtag_reports, index, NULL)) {
		pinba_rtag_report *report = (pinba_rtag_report *)*ppvalue;

		pinba_rtag_report_dtor(report, 0);
	}
	free(D->rtag_reports_arr.data);
	JudySLFreeArray(&D->rtag_reports, NULL);
}
/* }}} */

int pinba_array_add(pinba_array_t *array, void *report) /* {{{ */
{
	array->data = (void **)realloc(array->data, sizeof(void *) * (array->size + 1));
	if (!array->data) {
		return -1;
	}

	array->data[array->size] = report;
	array->size++;
	return 0;
}
/* }}} */

int pinba_array_delete(pinba_array_t *array, void *report) /* {{{ */
{
	size_t i;

	for (i = 0; i < array->size; i++) {
		if (array->data[i] == report) {
			if (i != (array->size - 1)) {
				memmove(array->data + i, array->data + i + 1, sizeof(void *) * (array->size - (i + 1)));
			}
			array->size--;
			return 0;
		}
	}
	return -1;
}
/* }}} */

/*
 * vim600: sw=4 ts=4 fdm=marker
 */
