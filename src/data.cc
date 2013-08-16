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
#include <string>
using namespace std;

static time_t last_warning = 0;


#include "pinba_update_report.h"

void pinba_update_tag_info_add(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
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
			report->results_cnt++;
		} else {
			data = (struct pinba_tag_info_data *)*ppvalue;
			data->hit_count += timer->hit_count;
			timeradd(&data->timer_value, &timer->value, &data->timer_value);
		}
		PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

		/* count tag values only once per request */
		if (request_id != data->prev_add_request_id) {
			data->req_count++;
			data->prev_add_request_id = request_id;
		}
	}
}
/* }}} */

void pinba_update_tag_info_delete(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
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
				report->results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
				PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data->histogram_data, timer->value, timer->hit_count);
			}
		}
	}
}
/* }}} */

void pinba_update_tag2_info_add(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
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

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_tag2_info_data *)*ppvalue;
			data->hit_count += timer->hit_count;
			timeradd(&data->timer_value, &timer->value, &data->timer_value);
		}
		PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

		/* count tag values only once per request */
		if (request_id != data->prev_add_request_id) {
			data->req_count++;
			data->prev_add_request_id = request_id;
		}
	}
}
/* }}} */

void pinba_update_tag2_info_delete(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tag2_info_data *data;
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, tag1_pos, tag2_pos;
	int index_len;
	pinba_word *word1, *word2;
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
				report->results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
				PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data->histogram_data, timer->value, timer->hit_count);
			}
		}
	}
}
/* }}} */

void pinba_update_tag_report_add(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tag_report_data *data;
	PPvoid_t ppvalue, ppvalue_script;
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

		ppvalue = JudySLGet(*ppvalue_script, (uint8_t *)word->str, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {

			ppvalue = JudySLIns(ppvalue_script, (uint8_t *)word->str, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				continue;
			}

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

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_tag_report_data *)*ppvalue;
			data->hit_count += timer->hit_count;
			timeradd(&data->timer_value, &timer->value, &data->timer_value);
		}
		PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

		/* count tag values only once per request */
		if (request_id != data->prev_add_request_id) {
			data->req_count++;
			data->prev_add_request_id = request_id;
		}
	}
}
/* }}} */

void pinba_update_tag_report_delete(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tag_report_data *data;
	PPvoid_t ppvalue, ppvalue_script;
	pinba_timer_record *timer;
	int i, j, tag_found;
	pinba_word *word;

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

		ppvalue = JudySLGet(*ppvalue_script, (uint8_t *)word->str, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			continue;
		} else {
			data = (struct pinba_tag_report_data *)*ppvalue;

			/* count tag values only once per request */
			if (request_id != data->prev_del_request_id) {
				data->req_count--;
				data->prev_del_request_id = request_id;
			}

			if (UNLIKELY(data->req_count == 0)) {
				free(data);
				JudySLDel(ppvalue_script, (uint8_t *)word->str, NULL);
				if (*ppvalue_script == NULL) {
					JudySLDel(&report->results, (uint8_t *)record->data.script_name, NULL);
					ppvalue_script = NULL;
				}
				report->results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
				PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data->histogram_data, timer->value, timer->hit_count);
			}
		}
	}
}
/* }}} */

void pinba_update_tag2_report_add(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tag2_report_data *data;
	PPvoid_t ppvalue, ppvalue_script;
	pinba_timer_record *timer;
	int i, j, tag1_pos, tag2_pos, dummy;
	int index_len;
	uint8_t index_val[PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1];
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

		ppvalue = JudySLGet(*ppvalue_script, index_val, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			ppvalue = JudySLIns(ppvalue_script, index_val, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				continue;
			}

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

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_tag2_report_data *)*ppvalue;
			data->hit_count += timer->hit_count;
			timeradd(&data->timer_value, &timer->value, &data->timer_value);
		}
		PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

		/* count tag values only once per request */
		if (request_id != data->prev_add_request_id) {
			data->req_count++;
			data->prev_add_request_id = request_id;
		}
	}
}
/* }}} */

void pinba_update_tag2_report_delete(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tag2_report_data *data;
	PPvoid_t ppvalue, ppvalue_script;
	pinba_timer_record *timer;
	int i, j, tag1_pos, tag2_pos;
	int index_len;
	uint8_t index_val[PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1];
	pinba_word *word1, *word2;

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

		ppvalue = JudySLGet(*ppvalue_script, index_val, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			continue;
		} else {
			data = (struct pinba_tag2_report_data *)*ppvalue;

			/* count tag values only once per request */
			if (request_id != data->prev_del_request_id) {
				data->req_count--;
				data->prev_del_request_id = request_id;
			}

			if (UNLIKELY(data->req_count == 0)) {
				free(data);
				JudySLDel(ppvalue_script, (uint8_t *)index_val, NULL);
				if (*ppvalue_script == NULL) {
					JudySLDel(&report->results, (uint8_t *)record->data.script_name, NULL);
					ppvalue_script = NULL;
				}
				report->results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
				PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data->histogram_data, timer->value, timer->hit_count);
			}
		}
	}
}
/* }}} */

void pinba_update_tag_report2_add(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tag_report2_data *data;
	PPvoid_t ppvalue, ppvalue_script;
	pinba_timer_record *timer;
	int i, j, tag_found, index_len, dummy;
	uint8_t index[PINBA_SCRIPT_NAME_SIZE + PINBA_HOSTNAME_SIZE + PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE];
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

		ppvalue = JudySLGet(*ppvalue_script, (uint8_t *)index, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {

			ppvalue = JudySLIns(ppvalue_script, (uint8_t *)index, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				continue;
			}

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

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_tag_report2_data *)*ppvalue;
			data->hit_count += timer->hit_count;
			timeradd(&data->timer_value, &timer->value, &data->timer_value);
		}
		PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

		/* count tag values only once per request */
		if (request_id != data->prev_add_request_id) {
			data->req_count++;
			data->prev_add_request_id = request_id;
		}
	}
}
/* }}} */

void pinba_update_tag_report2_delete(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tag_report2_data *data;
	PPvoid_t ppvalue, ppvalue_script;
	pinba_timer_record *timer;
	int i, j, tag_found, index_len;
	uint8_t index[PINBA_HOSTNAME_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE];
	pinba_word *word;

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

		ppvalue = JudySLGet(*ppvalue_script, index, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			continue;
		} else {
			data = (struct pinba_tag_report2_data *)*ppvalue;

			/* count tag values only once per request */
			if (request_id != data->prev_del_request_id) {
				data->req_count--;
				data->prev_del_request_id = request_id;
			}

			if (UNLIKELY(data->req_count == 0)) {
				free(data);
				JudySLDel(ppvalue_script, index, NULL);
				if (*ppvalue_script == NULL) {
					JudySLDel(&report->results, (uint8_t *)record->data.script_name, NULL);
					ppvalue_script = NULL;
				}
				report->results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
				PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data->histogram_data, timer->value, timer->hit_count);
			}
		}
	}
}
/* }}} */

void pinba_update_tag2_report2_add(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tag2_report2_data *data;
	PPvoid_t ppvalue, ppvalue_script;
	pinba_timer_record *timer;
	int i, j, tag1_pos, tag2_pos, dummy;
	int index_len;
	uint8_t index_val[PINBA_HOSTNAME_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE];
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

        ppvalue = JudySLGet(*ppvalue_script, index_val, NULL);

        if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
            ppvalue = JudySLIns(ppvalue_script, index_val, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				continue;
			}

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

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_tag2_report2_data *)*ppvalue;
			data->hit_count += timer->hit_count;
			timeradd(&data->timer_value, &timer->value, &data->timer_value);
		}
		PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

		/* count tag values only once per request */
		if (request_id != data->prev_add_request_id) {
			data->req_count++;
			data->prev_add_request_id = request_id;
		}
	}
}
/* }}} */

void pinba_update_tag2_report2_delete(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tag2_report2_data *data;
	PPvoid_t ppvalue, ppvalue_script;
	pinba_timer_record *timer;
	int i, j, tag1_pos, tag2_pos;
	int index_len;
	uint8_t index_val[PINBA_HOSTNAME_SIZE + PINBA_SERVER_NAME_SIZE + PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE];
	pinba_word *word1, *word2;

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

		ppvalue = JudySLGet(*ppvalue_script, index_val, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			continue;
		} else {
			data = (struct pinba_tag2_report2_data *)*ppvalue;

			/* count tag values only once per request */
			if (request_id != data->prev_del_request_id) {
				data->req_count--;
				data->prev_del_request_id = request_id;
			}

			if (UNLIKELY(data->req_count == 0)) {
				free(data);
				JudySLDel(ppvalue_script, (uint8_t *)index_val, NULL);
				if (*ppvalue_script == NULL) {
					JudySLDel(&report->results, (uint8_t *)record->data.script_name, NULL);
					ppvalue_script = NULL;
				}
				report->results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
				PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data->histogram_data, timer->value, timer->hit_count);
			}
		}
	}
}
/* }}} */

void pinba_update_tagN_info_add(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tagN_info_data *data;
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, k, found_tag_cnt, dummy;
	int index_len, index_alloc_len;
	uint8_t *index_val = NULL;
	pinba_word *word, **words = NULL;

	for (i = 0; i < record->timers_cnt; i++) {
		found_tag_cnt = 0;

		if (!words) {
			words = (pinba_word **)calloc(report->tag_cnt, sizeof(void *));
			if (!words) {
				return;
			}
		}

		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			int h;

			for (h = 0; h < report->tag_cnt; h++) {
				if (report->tag_id[h] == timer->tag_ids[j]) {
					words[h] = (pinba_word *)timer->tag_values[j];
					found_tag_cnt++;
				}
			}
		}

		if (found_tag_cnt != report->tag_cnt) {
			continue;
		}

		if (!index_val) {
			index_alloc_len = PINBA_TAG_VALUE_SIZE * report->tag_cnt + report->tag_cnt;
			index_val = (uint8_t *)malloc(index_alloc_len);
			if (!index_val) {
				free(words);
				return;
			}
		}

		index_len = 0;
		for (k = 0; k < report->tag_cnt; k++) {
			word = words[k];
			index_len += snprintf((char *)index_val + index_len, index_alloc_len - index_len, "%s|", word->str);
		}

		ppvalue = JudySLGet(report->results, index_val, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			ppvalue = JudySLIns(&report->results, index_val, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				continue;
			}

			data = (struct pinba_tagN_info_data *)calloc(1, sizeof(struct pinba_tagN_info_data));
			if (UNLIKELY(!data)) {
				continue;
			}

			data->tag_value = (char *)calloc(report->tag_cnt, PINBA_TAG_VALUE_SIZE);
			if (UNLIKELY(!data->tag_value)) {
				free(data);
				continue;
			}

			data->req_count = 1;
			data->hit_count = timer->hit_count;
			data->timer_value = timer->value;
			data->prev_add_request_id = request_id;
			data->prev_del_request_id = -1;

			for (k = 0; k < report->tag_cnt; k++) {
				word = words[k];
				memcpy(data->tag_value + PINBA_TAG_VALUE_SIZE * k, word->str, word->len);
			}

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_tagN_info_data *)*ppvalue;
			data->hit_count += timer->hit_count;
			timeradd(&data->timer_value, &timer->value, &data->timer_value);
		}
		PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

		/* count tag values only once per request */
		if (request_id != data->prev_add_request_id) {
			data->req_count++;
			data->prev_add_request_id = request_id;
		}
	}
	if (index_val) {
		free(index_val);
	}
}
/* }}} */

void pinba_update_tagN_info_delete(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tagN_info_data *data;
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, k, found_tag_cnt;
	int index_len, index_alloc_len;
	uint8_t *index_val = NULL;
	pinba_word *word, **words = NULL;

	for (i = 0; i < record->timers_cnt; i++) {

		if (!words) {
			words = (pinba_word **)calloc(report->tag_cnt, sizeof(void *));
			if (!words) {
				return;
			}
		}

		found_tag_cnt = 0;

		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			int h;

			for (h = 0; h < report->tag_cnt; h++) {
				if (report->tag_id[h] == timer->tag_ids[j]) {
					words[h] = (pinba_word *)timer->tag_values[j];
					found_tag_cnt++;
				}
			}
		}

		if (found_tag_cnt != report->tag_cnt) {
			continue;
		}

		if (!index_val) {
			index_alloc_len = PINBA_TAG_VALUE_SIZE * report->tag_cnt + report->tag_cnt;
			index_val = (uint8_t *)malloc(index_alloc_len);
			if (!index_val) {
				free(words);
				return;
			}
		}

		index_len = 0;
		for (k = 0; k < report->tag_cnt; k++) {
			word = words[k];
			index_len += snprintf((char *)index_val + index_len, index_alloc_len - index_len, "%s|", word->str);
		}

		ppvalue = JudySLGet(report->results, index_val, NULL);
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
				JudySLDel(&report->results, (uint8_t *)index_val, NULL);
				report->results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
				PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data->histogram_data, timer->value, timer->hit_count);
			}
		}
	}
	if (index_val) {
		free(index_val);
	}
}
/* }}} */

void pinba_update_tagN_report_add(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tagN_report_data *data;
	PPvoid_t ppvalue, ppvalue_script;
	pinba_timer_record *timer;
	int i, j, k, found_tag_cnt, dummy;
	int index_len, index_alloc_len;
	uint8_t *index_val = NULL;
	pinba_word *word, **words = NULL;

	ppvalue_script = NULL;
	for (i = 0; i < record->timers_cnt; i++) {
		found_tag_cnt = 0;

		if (!words) {
			words = (pinba_word **)calloc(report->tag_cnt, sizeof(void *));
			if (!words) {
				return;
			}
		}

		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			int h;

			for (h = 0; h < report->tag_cnt; h++) {
				if (report->tag_id[h] == timer->tag_ids[j]) {
					words[h] = (pinba_word *)timer->tag_values[j];
					found_tag_cnt++;
				}
			}
		}

		if (found_tag_cnt != report->tag_cnt) {
			continue;
		}

		if (!ppvalue_script) {
			ppvalue_script = JudySLIns(&report->results, (uint8_t *)record->data.script_name, NULL);
			if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
				continue;
			}
		}

		if (!index_val) {
			index_alloc_len = PINBA_TAG_VALUE_SIZE * report->tag_cnt + report->tag_cnt;
			index_val = (uint8_t *)malloc(index_alloc_len);
			if (!index_val) {
				free(words);
				return;
			}
		}

		index_len = 0;
		for (k = 0; k < report->tag_cnt; k++) {
			word = words[k];
			index_len += snprintf((char *)index_val + index_len, index_alloc_len - index_len, "%s|", word->str);
		}

		ppvalue = JudySLGet(*ppvalue_script, index_val, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			ppvalue = JudySLIns(ppvalue_script, index_val, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				continue;
			}

			data = (struct pinba_tagN_report_data *)calloc(1, sizeof(struct pinba_tagN_report_data));
			if (UNLIKELY(!data)) {
				continue;
			}

			data->tag_value = (char *)calloc(report->tag_cnt, PINBA_TAG_VALUE_SIZE);
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
			for (k = 0; k < report->tag_cnt; k++) {
				word = words[k];
				memcpy(data->tag_value + PINBA_TAG_VALUE_SIZE * k, word->str, word->len);
			}

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_tagN_report_data *)*ppvalue;
			data->hit_count += timer->hit_count;
			timeradd(&data->timer_value, &timer->value, &data->timer_value);
		}
		PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

		/* count tag values only once per request */
		if (request_id != data->prev_add_request_id) {
			data->req_count++;
			data->prev_add_request_id = request_id;
		}
	}
	if (index_val) {
		free(index_val);
	}
}
/* }}} */

void pinba_update_tagN_report_delete(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tagN_report_data *data;
	PPvoid_t ppvalue, ppvalue_script;
	pinba_timer_record *timer;
	int i, j, k, found_tag_cnt;
	int index_len, index_alloc_len;
	uint8_t *index_val = NULL;
	pinba_word *word, **words = NULL;

	ppvalue_script = JudySLGet(report->results, (uint8_t *)record->data.script_name, NULL);
	if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
		return;
	}

	for (i = 0; i < record->timers_cnt; i++) {

		if (!ppvalue_script) {
			continue;
		}

		if (!words) {
			words = (pinba_word **)calloc(report->tag_cnt, sizeof(void *));
			if (!words) {
				return;
			}
		}

		found_tag_cnt = 0;

		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			int h;

			for (h = 0; h < report->tag_cnt; h++) {
				if (report->tag_id[h] == timer->tag_ids[j]) {
					words[h] = (pinba_word *)timer->tag_values[j];
					found_tag_cnt++;
				}
			}
		}

		if (found_tag_cnt != report->tag_cnt) {
			continue;
		}

		if (!index_val) {
			index_alloc_len = PINBA_TAG_VALUE_SIZE * report->tag_cnt + report->tag_cnt;
			index_val = (uint8_t *)malloc(index_alloc_len);
			if (!index_val) {
				free(words);
				return;
			}
		}

		index_len = 0;
		for (k = 0; k < report->tag_cnt; k++) {
			word = words[k];
			index_len += snprintf((char *)index_val + index_len, index_alloc_len - index_len, "%s|", word->str);
		}

		ppvalue = JudySLGet(*ppvalue_script, index_val, NULL);
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
				JudySLDel(ppvalue_script, (uint8_t *)index_val, NULL);
				if (*ppvalue_script == NULL) {
					JudySLDel(&report->results, (uint8_t *)record->data.script_name, NULL);
					ppvalue_script = NULL;
				}
				report->results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
				PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data->histogram_data, timer->value, timer->hit_count);
			}
		}
	}
	if (index_val) {
		free(index_val);
	}
}
/* }}} */

void pinba_update_tagN_report2_add(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tagN_report2_data *data;
	PPvoid_t ppvalue, ppvalue_script;
	pinba_timer_record *timer;
	int i, j, k, found_tag_cnt, dummy;
	int index_len, index_alloc_len;
	uint8_t *index_val = NULL;
	pinba_word *word, **words = NULL;

	ppvalue_script = NULL;
	for (i = 0; i < record->timers_cnt; i++) {
		found_tag_cnt = 0;

		if (!words) {
			words = (pinba_word **)calloc(report->tag_cnt, sizeof(void *));
			if (!words) {
				return;
			}
		}

		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			int h;

			for (h = 0; h < report->tag_cnt; h++) {
				if (report->tag_id[h] == timer->tag_ids[j]) {
					words[h] = (pinba_word *)timer->tag_values[j];
					found_tag_cnt++;
				}
			}
		}

		if (found_tag_cnt != report->tag_cnt) {
			continue;
		}

		if (!ppvalue_script) {
			ppvalue_script = JudySLIns(&report->results, (uint8_t *)record->data.script_name, NULL);
			if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
				continue;
			}
		}

		if (!index_val) {
			index_alloc_len = PINBA_HOSTNAME_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE * report->tag_cnt + report->tag_cnt;
			index_val = (uint8_t *)malloc(index_alloc_len);
			if (!index_val) {
				free(words);
				return;
			}
		}

		index_len = snprintf((char *)index_val, index_alloc_len, "%s|%s|", record->data.hostname, record->data.server_name);
		for (k = 0; k < report->tag_cnt; k++) {
			word = words[k];
			index_len += snprintf((char *)index_val + index_len, index_alloc_len - index_len, "%s|", word->str);
		}

		ppvalue = JudySLGet(*ppvalue_script, index_val, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			ppvalue = JudySLIns(ppvalue_script, index_val, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				continue;
			}

			data = (struct pinba_tagN_report2_data *)calloc(1, sizeof(struct pinba_tagN_report2_data));
			if (UNLIKELY(!data)) {
				continue;
			}

			data->tag_value = (char *)calloc(report->tag_cnt, PINBA_TAG_VALUE_SIZE);
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
			for (k = 0; k < report->tag_cnt; k++) {
				word = words[k];
				memcpy(data->tag_value + PINBA_TAG_VALUE_SIZE * k, word->str, word->len);
			}

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_tagN_report2_data *)*ppvalue;
			data->hit_count += timer->hit_count;
			timeradd(&data->timer_value, &timer->value, &data->timer_value);
		}
		PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

		/* count tag values only once per request */
		if (request_id != data->prev_add_request_id) {
			data->req_count++;
			data->prev_add_request_id = request_id;
		}
	}
	if (index_val) {
		free(index_val);
	}
}
/* }}} */

void pinba_update_tagN_report2_delete(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tagN_report2_data *data;
	PPvoid_t ppvalue, ppvalue_script;
	pinba_timer_record *timer;
	int i, j, k, found_tag_cnt;
	int index_len, index_alloc_len;
	uint8_t *index_val = NULL;
	pinba_word *word, **words = NULL;

	ppvalue_script = JudySLGet(report->results, (uint8_t *)record->data.script_name, NULL);
	if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
		return;
	}

	for (i = 0; i < record->timers_cnt; i++) {

		if (!ppvalue_script) {
			continue;
		}

		if (!words) {
			words = (pinba_word **)calloc(report->tag_cnt, sizeof(void *));
			if (!words) {
				return;
			}
		}

		found_tag_cnt = 0;

		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			int h;

			for (h = 0; h < report->tag_cnt; h++) {
				if (report->tag_id[h] == timer->tag_ids[j]) {
					words[h] = (pinba_word *)timer->tag_values[j];
					found_tag_cnt++;
				}
			}
		}

		if (found_tag_cnt != report->tag_cnt) {
			continue;
		}

		if (!index_val) {
			index_alloc_len = PINBA_HOSTNAME_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE * report->tag_cnt + report->tag_cnt;
			index_val = (uint8_t *)malloc(index_alloc_len);
			if (!index_val) {
				free(words);
				return;
			}
		}

		index_len = snprintf((char *)index_val, index_alloc_len, "%s|%s|", record->data.hostname, record->data.server_name);
		for (k = 0; k < report->tag_cnt; k++) {
			word = words[k];
			index_len += snprintf((char *)index_val + index_len, index_alloc_len - index_len, "%s|", word->str);
		}

		ppvalue = JudySLGet(*ppvalue_script, index_val, NULL);
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
				JudySLDel(ppvalue_script, (uint8_t *)index_val, NULL);
				if (*ppvalue_script == NULL) {
					JudySLDel(&report->results, (uint8_t *)record->data.script_name, NULL);
					ppvalue_script = NULL;
				}
				report->results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
				PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data->histogram_data, timer->value, timer->hit_count);
			}
		}
	}
	if (index_val) {
		free(index_val);
	}
}
/* }}} */


void pinba_update_tag_reports_add(int request_id, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report;
	int i;

	for (i = 0; i < D->tag_reports_arr_size; i++) {
		report = (pinba_tag_report *)D->tag_reports_arr[i];

		CHECK_REPORT_CONDITIONS_CONTINUE(report, record);

		pthread_rwlock_wrlock(&report->lock);
		report->add_func(request_id, report, record);
		report->time_interval = pinba_get_time_interval();
		pthread_rwlock_unlock(&report->lock);
	}
}
/* }}} */

void pinba_update_tag_reports_delete(int request_id, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report;
	int i;

	for (i = 0; i < D->tag_reports_arr_size; i++) {
		report = (pinba_tag_report *)D->tag_reports_arr[i];

		CHECK_REPORT_CONDITIONS_CONTINUE(report, record);

		pthread_rwlock_wrlock(&report->lock);
		report->delete_func(request_id, report, record);
		report->time_interval = pinba_get_time_interval();
		pthread_rwlock_unlock(&report->lock);
	}
}
/* }}} */

void pinba_update_reports_add(const pinba_stats_record *record) /* {{{ */
{
	pinba_report *report;
	int i;

	for (i = 0; i < D->base_reports_arr_size; i++) {
		report = (pinba_report *)D->base_reports_arr[i];

		CHECK_REPORT_CONDITIONS_CONTINUE(report, record);

		pthread_rwlock_wrlock(&report->lock);
		report->add_func(report, record);
		report->time_interval = pinba_get_time_interval();
		pthread_rwlock_unlock(&report->lock);
	}
}
/* }}} */

void pinba_update_reports_delete(const pinba_stats_record *record) /* {{{ */
{
	pinba_report *report;
	int i;

	for (i = 0; i < D->base_reports_arr_size; i++) {
		report = (pinba_report *)D->base_reports_arr[i];

		CHECK_REPORT_CONDITIONS_CONTINUE(report, record);

		pthread_rwlock_wrlock(&report->lock);
		report->delete_func(report, record);
		report->time_interval = pinba_get_time_interval();
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
	report->results_cnt = 0;
}
/* }}} */

void pinba_std_report_dtor(void *rprt) /* {{{ */
{
	pinba_std_report *std_report = (pinba_std_report *)rprt;
	int i;

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
}
/* }}} */

void pinba_reports_destroy() /* {{{ */
{
	pinba_report *report;
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	PPvoid_t ppvalue;

	for (ppvalue = JudySLFirst(D->base_reports, index, NULL); ppvalue != NULL && ppvalue != PPJERR; ppvalue = JudySLNext(D->base_reports, index, NULL)) {
		report = (pinba_report *)*ppvalue;

		pthread_rwlock_wrlock(&report->lock);
		if (report->results_cnt) {
			pinba_report_results_dtor(report);

			report->time_interval = 0;
			report->results_cnt = 0;
			report->results = NULL;
			report->time_total.tv_sec = 0;
			report->time_total.tv_usec = 0;
			report->ru_utime_total.tv_sec = 0;
			report->ru_utime_total.tv_usec = 0;
			report->ru_stime_total.tv_sec = 0;
			report->ru_stime_total.tv_usec = 0;
			report->kbytes_total = 0;
		}
		pthread_rwlock_unlock(&report->lock);
		pthread_rwlock_destroy(&report->lock);
		pinba_std_report_dtor(report);
		free(report);
	}

	free(D->base_reports_arr);
}
/* }}} */

void pinba_tag_reports_destroy(void) /* {{{ */
{
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	uint8_t sub_index[PINBA_MAX_LINE_LEN] = {0};
	pinba_tag_report *report;
	PPvoid_t ppvalue, sub_ppvalue;
	time_t now;

	now = time(NULL);

	pthread_rwlock_wrlock(&D->tag_reports_lock);
	for (ppvalue = JudySLFirst(D->tag_reports, index, NULL); ppvalue != NULL && ppvalue != PPJERR; ppvalue = JudySLNext(D->tag_reports, index, NULL)) {
		report = (pinba_tag_report *)*ppvalue;

		sub_index[0] = 0;

		JudySLDel(&D->tag_reports, index, NULL);

		pthread_rwlock_wrlock(&report->lock);
		for (sub_ppvalue = JudySLFirst(report->results, sub_index, NULL); sub_ppvalue != NULL && sub_ppvalue != PPJERR; sub_ppvalue = JudySLNext(report->results, sub_index, NULL)) {
			free(*sub_ppvalue);
		}
		JudySLFreeArray(&report->results, NULL);
		pthread_rwlock_unlock(&report->lock);
		pthread_rwlock_destroy(&report->lock);
		pinba_tag_reports_array_delete(report);
		pinba_std_report_dtor(report);
		free(report->tag_id);
		free(report);
	}
	free(D->tag_reports_arr);
	pthread_rwlock_unlock(&D->tag_reports_lock);
}
/* }}} */

int pinba_base_reports_array_add(void *report) /* {{{ */
{
	D->base_reports_arr = (void **)realloc(D->base_reports_arr, sizeof(void *) * (D->base_reports_arr_size + 1));
	if (!D->base_reports_arr) {
		return -1;
	}

	D->base_reports_arr[D->base_reports_arr_size] = report;
	D->base_reports_arr_size++;
	return 0;
}
/* }}} */

int pinba_base_reports_array_delete(void *report) /* {{{ */
{
	int i;

	for (i = 0; i < D->base_reports_arr_size; i++) {
		if (D->base_reports_arr[i] == report) {
			if (i != (D->base_reports_arr_size - 1)) {
				memmove(D->base_reports_arr + i, D->base_reports_arr + i + 1, sizeof(void *) * (D->base_reports_arr_size - (i + 1)));
			}
			D->base_reports_arr_size--;
			return 0;
		}
	}
	return -1;
}
/* }}} */

int pinba_tag_reports_array_add(void *tag_report) /* {{{ */
{
	D->tag_reports_arr = (void **)realloc(D->tag_reports_arr, sizeof(void *) * (D->tag_reports_arr_size + 1));
	if (!D->tag_reports_arr) {
		return -1;
	}

	D->tag_reports_arr[D->tag_reports_arr_size] = tag_report;
	D->tag_reports_arr_size++;
	return 0;
}
/* }}} */

int pinba_tag_reports_array_delete(void *tag_report) /* {{{ */
{
	int i;

	for (i = 0; i < D->tag_reports_arr_size; i++) {
		if (D->tag_reports_arr[i] == tag_report) {
			if (i != (D->tag_reports_arr_size - 1)) {
				memmove(D->tag_reports_arr + i, D->tag_reports_arr + i + 1, sizeof(void *) * (D->tag_reports_arr_size - (i + 1)));
			}
			D->tag_reports_arr_size--;
			return 0;
		}
	}
	return -1;
}
/* }}} */

#if 0
int pinba_process_stats_packet(const unsigned char *buf, int buf_len) /* {{{ */
{
	time_t now;
	bool res;
	pinba_tmp_stats_record *tmp_record;
	pinba_pool *temp_pool = &D->temp_pool;

	now = time(NULL);

	pthread_rwlock_wrlock(&D->temp_lock);
	if (pinba_pool_is_full(temp_pool)) { /* got maximum */
		pthread_rwlock_unlock(&D->temp_lock);
		if (now != last_warning) { /* we don't want to throw warnings faster than once per second */
			pinba_debug("failed to store stats packet - temporary pool is full");
			last_warning = now;
		}
		return P_FAILURE;
	}

	tmp_record = TMP_POOL(temp_pool) + temp_pool->in;
	res = tmp_record->request.ParseFromArray(buf, buf_len);

	if (UNLIKELY(!res)) {
		pthread_rwlock_unlock(&D->temp_lock);
		return P_FAILURE;
	} else {
		tmp_record->time = now;

		if (UNLIKELY(temp_pool->in == (temp_pool->size - 1))) {
			temp_pool->in = 0;
		} else {
			temp_pool->in++;
		}
		pthread_rwlock_unlock(&D->temp_lock);
		return P_SUCCESS;
	}
}
/* }}} */
#endif

/*
 * vim600: sw=4 ts=4 fdm=marker
 */
