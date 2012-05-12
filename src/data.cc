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

void pinba_update_report_info_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;
	report->results_cnt++;
}
/* }}} */

void pinba_update_report_info_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	if (UNLIKELY(report->results_cnt == 0)) {
		return;
	}

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	timersub(&report->time_total, &record->data.req_time, &report->time_total);
	timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total -= record->data.doc_size;
	report->results_cnt--;

	if (UNLIKELY(report->results_cnt == 0)) {
		report->time_total.tv_sec = 0;
		report->time_total.tv_usec = 0;
		report->ru_utime_total.tv_sec = 0;
		report->ru_utime_total.tv_usec = 0;
		report->ru_stime_total.tv_sec = 0;
		report->ru_stime_total.tv_usec = 0;
		report->kbytes_total = 0;
	}
}
/* }}} */

void pinba_update_report1_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_report1_data *data;
	PPvoid_t ppvalue;

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;

	ppvalue = JudySLGet(report->results, (uint8_t *)record->data.script_name, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, insert */
		ppvalue = JudySLIns(&report->results, (uint8_t *)record->data.script_name, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return;
		}
		data = (struct pinba_report1_data *)malloc(sizeof(struct pinba_report1_data));

		data->req_count = 1;
		data->req_time_total = record->data.req_time;
		data->ru_utime_total = record->data.ru_utime;
		data->ru_stime_total = record->data.ru_stime;
		data->kbytes_total = record->data.doc_size;

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report1_data *)*ppvalue;
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

void pinba_update_report1_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_report1_data *data;
	PPvoid_t ppvalue;

	if (report->results_cnt == 0) {
		return;
	}

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	timersub(&report->time_total, &record->data.req_time, &report->time_total);
	timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total -= record->data.doc_size;

	ppvalue = JudySLGet(report->results, (uint8_t *)record->data.script_name, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, mmm?? */
		return;
	} else {
		data = (struct pinba_report1_data *)*ppvalue;
		if (UNLIKELY(data->req_count == 1)) {
			free(data);
			JudySLDel(&report->results, (uint8_t *)record->data.script_name, NULL);
			report->results_cnt--;
		} else {
			data->req_count--;
			timersub(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timersub(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timersub(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total -= record->data.doc_size;
		}
	}
}
/* }}} */

void pinba_update_report2_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_report2_data *data;
	PPvoid_t ppvalue;

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;

	ppvalue = JudySLGet(report->results, (uint8_t *)record->data.server_name, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, insert */
		ppvalue = JudySLIns(&report->results, (uint8_t *)record->data.server_name, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return;
		}
		data = (struct pinba_report2_data *)malloc(sizeof(struct pinba_report2_data));

		data->req_count = 1;
		data->req_time_total = record->data.req_time;
		data->ru_utime_total = record->data.ru_utime;
		data->ru_stime_total = record->data.ru_stime;
		data->kbytes_total = record->data.doc_size;

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report2_data *)*ppvalue;
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

void pinba_update_report2_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_report2_data *data;
	PPvoid_t ppvalue;

	if (report->results_cnt == 0) {
		return;
	}

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	timersub(&report->time_total, &record->data.req_time, &report->time_total);
	timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total -= record->data.doc_size;

	ppvalue = JudySLGet(report->results, (uint8_t *)record->data.server_name, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, mmm?? */
		return;
	} else {
		data = (struct pinba_report2_data *)*ppvalue;
		if (UNLIKELY(data->req_count == 1)) {
			free(data);
			JudySLDel(&report->results, (uint8_t *)record->data.server_name, NULL);
			report->results_cnt--;
		} else {
			data->req_count--;
			timersub(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timersub(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timersub(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total -= record->data.doc_size;
		}
	}
}
/* }}} */

void pinba_update_report3_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_report3_data *data;
	PPvoid_t ppvalue;

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;

	ppvalue = JudySLGet(report->results, (uint8_t *)record->data.hostname, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, insert */
		ppvalue = JudySLIns(&report->results, (uint8_t *)record->data.hostname, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return;
		}
		data = (struct pinba_report3_data *)malloc(sizeof(struct pinba_report3_data));

		data->req_count = 1;
		data->req_time_total = record->data.req_time;
		data->ru_utime_total = record->data.ru_utime;
		data->ru_stime_total = record->data.ru_stime;
		data->kbytes_total = record->data.doc_size;

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report3_data *)*ppvalue;
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

void pinba_update_report3_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_report3_data *data;
	PPvoid_t ppvalue;

	if (report->results_cnt == 0) {
		return;
	}

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	timersub(&report->time_total, &record->data.req_time, &report->time_total);
	timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total -= record->data.doc_size;

	ppvalue = JudySLGet(report->results, (uint8_t *)record->data.hostname, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, mmm?? */
		return;
	} else {
		data = (struct pinba_report3_data *)*ppvalue;
		if (UNLIKELY(data->req_count == 1)) {
			free(data);
			JudySLDel(&report->results, (uint8_t *)record->data.hostname, NULL);
			report->results_cnt--;
		} else {
			data->req_count--;
			timersub(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timersub(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timersub(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total -= record->data.doc_size;
		}
	}
}
/* }}} */

void pinba_update_report4_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_SERVER_NAME_SIZE + PINBA_SCRIPT_NAME_SIZE + 1] = {0};
	struct pinba_report4_data *data;
	PPvoid_t ppvalue;
	int index_len, dummy;

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;

	memcpy_static(index, record->data.server_name, record->data.server_name_len, index_len);
	index[index_len] = '/'; index_len++;
	memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);

	ppvalue = JudySLGet(report->results, index, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, insert */
		ppvalue = JudySLIns(&report->results, index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return;
		}
		data = (struct pinba_report4_data *)malloc(sizeof(struct pinba_report4_data));

		data->req_count = 1;
		data->req_time_total = record->data.req_time;
		data->ru_utime_total = record->data.ru_utime;
		data->ru_stime_total = record->data.ru_stime;
		data->kbytes_total = record->data.doc_size;
		memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);
		memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report4_data *)*ppvalue;
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

void pinba_update_report4_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_SERVER_NAME_SIZE + PINBA_SCRIPT_NAME_SIZE + 1] = {0};
	struct pinba_report4_data *data;
	PPvoid_t ppvalue;
	int index_len;

	if (report->results_cnt == 0) {
		return;
	}

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	timersub(&report->time_total, &record->data.req_time, &report->time_total);
	timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total -= record->data.doc_size;

	memcpy_static(index, record->data.server_name, record->data.server_name_len, index_len);
	index[index_len] = '/'; index_len++;
	memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);

	ppvalue = JudySLGet(report->results, index, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, mmm?? */
		return;
	} else {
		data = (struct pinba_report4_data *)*ppvalue;
		if (UNLIKELY(data->req_count == 1)) {
			free(data);
			JudySLDel(&report->results, index, NULL);
			report->results_cnt--;
		} else {
			data->req_count--;
			timersub(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timersub(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timersub(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total -= record->data.doc_size;
		}
	}
}
/* }}} */

void pinba_update_report5_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_HOSTNAME_SIZE + PINBA_SCRIPT_NAME_SIZE + 1] = {0};
	struct pinba_report5_data *data;
	PPvoid_t ppvalue;
	int index_len, dummy;

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;

	memcpy_static(index, record->data.hostname, record->data.hostname_len, index_len);
	index[index_len] = ':'; index_len++;
	memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);

	ppvalue = JudySLGet(report->results, index, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, insert */
		ppvalue = JudySLIns(&report->results, index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return;
		}
		data = (struct pinba_report5_data *)malloc(sizeof(struct pinba_report5_data));

		data->req_count = 1;
		data->req_time_total = record->data.req_time;
		data->ru_utime_total = record->data.ru_utime;
		data->ru_stime_total = record->data.ru_stime;
		data->kbytes_total = record->data.doc_size;
		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report5_data *)*ppvalue;
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

void pinba_update_report5_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_HOSTNAME_SIZE + PINBA_SCRIPT_NAME_SIZE + 1] = {0};
	struct pinba_report5_data *data;
	PPvoid_t ppvalue;
	int index_len;

	if (report->results_cnt == 0) {
		return;
	}

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	timersub(&report->time_total, &record->data.req_time, &report->time_total);
	timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total -= record->data.doc_size;

	memcpy_static(index, record->data.hostname, record->data.hostname_len, index_len);
	index[index_len] = ':'; index_len++;
	memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);

	ppvalue = JudySLGet(report->results, index, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, mmm?? */
		return;
	} else {
		data = (struct pinba_report5_data *)*ppvalue;
		if (UNLIKELY(data->req_count == 1)) {
			free(data);
			JudySLDel(&report->results, index, NULL);
			report->results_cnt--;
		} else {
			data->req_count--;
			timersub(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timersub(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timersub(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total -= record->data.doc_size;
		}
	}
}
/* }}} */

void pinba_update_report6_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_HOSTNAME_SIZE + PINBA_SERVER_NAME_SIZE + 1] = {0};
	struct pinba_report6_data *data;
	PPvoid_t ppvalue;
	int index_len, dummy;

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;

	memcpy_static(index, record->data.hostname, record->data.hostname_len, index_len);
	index[index_len] = '/'; index_len++;
	memcat_static(index, index_len, record->data.server_name, record->data.server_name_len, index_len);

	ppvalue = JudySLGet(report->results, index, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, insert */
		ppvalue = JudySLIns(&report->results, index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return;
		}
		data = (struct pinba_report6_data *)malloc(sizeof(struct pinba_report6_data));

		data->req_count = 1;
		data->req_time_total = record->data.req_time;
		data->ru_utime_total = record->data.ru_utime;
		data->ru_stime_total = record->data.ru_stime;
		data->kbytes_total = record->data.doc_size;

		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report6_data *)*ppvalue;
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

void pinba_update_report6_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_HOSTNAME_SIZE + PINBA_SERVER_NAME_SIZE + 1] = {0};
	struct pinba_report6_data *data;
	PPvoid_t ppvalue;
	int index_len;

	if (report->results_cnt == 0) {
		return;
	}

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	timersub(&report->time_total, &record->data.req_time, &report->time_total);
	timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total -= record->data.doc_size;

	memcpy_static(index, record->data.hostname, record->data.hostname_len, index_len);
	index[index_len] = '/'; index_len++;
	memcat_static(index, index_len, record->data.server_name, record->data.server_name_len, index_len);

	ppvalue = JudySLGet(report->results, index, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, mmm?? */
		return;
	} else {
		data = (struct pinba_report6_data *)*ppvalue;
		if (UNLIKELY(data->req_count == 1)) {
			free(data);
			JudySLDel(&report->results, index, NULL);
			report->results_cnt--;
		} else {
			data->req_count--;
			timersub(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timersub(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timersub(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total -= record->data.doc_size;
		}
	}
}
/* }}} */

void pinba_update_report7_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_HOSTNAME_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE] = {0};
	struct pinba_report7_data *data;
	PPvoid_t ppvalue;
	int index_len, dummy;

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;

	memcpy_static(index, record->data.hostname, record->data.hostname_len, index_len);
	index[index_len] = ':'; index_len++;
	memcat_static(index, index_len, record->data.server_name, record->data.server_name_len, index_len);
	index[index_len] = '/'; index_len++;
	memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);

	ppvalue = JudySLGet(report->results, index, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, insert */
		ppvalue = JudySLIns(&report->results, index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return;
		}
		data = (struct pinba_report7_data *)malloc(sizeof(struct pinba_report7_data));

		data->req_count = 1;
		data->req_time_total = record->data.req_time;
		data->ru_utime_total = record->data.ru_utime;
		data->ru_stime_total = record->data.ru_stime;
		data->kbytes_total = record->data.doc_size;

		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);
		memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report7_data *)*ppvalue;
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

void pinba_update_report7_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_HOSTNAME_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE] = {0};
	struct pinba_report7_data *data;
	PPvoid_t ppvalue;
	int index_len;

	if (report->results_cnt == 0) {
		return;
	}

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	timersub(&report->time_total, &record->data.req_time, &report->time_total);
	timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total -= record->data.doc_size;

	memcpy_static(index, record->data.hostname, record->data.hostname_len, index_len);
	index[index_len] = ':'; index_len++;
	memcat_static(index, index_len, record->data.server_name, record->data.server_name_len, index_len);
	index[index_len] = '/'; index_len++;
	memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);

	ppvalue = JudySLGet(report->results, index, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, mmm?? */
		return;
	} else {
		data = (struct pinba_report7_data *)*ppvalue;
		if (UNLIKELY(data->req_count == 1)) {
			free(data);
			JudySLDel(&report->results, index, NULL);
			report->results_cnt--;
		} else {
			data->req_count--;
			timersub(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timersub(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timersub(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total -= record->data.doc_size;
		}
	}
}
/* }}} */

void pinba_update_report8_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_STATUS_SIZE] = {0};
	struct pinba_report8_data *data;
	PPvoid_t ppvalue;

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;

	sprintf((char *)index, "%u", record->data.status);
	ppvalue = JudySLGet(report->results, index, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, insert */
		ppvalue = JudySLIns(&report->results, index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return;
		}
		data = (struct pinba_report8_data *)malloc(sizeof(struct pinba_report8_data));

		data->req_count = 1;
		data->req_time_total = record->data.req_time;
		data->ru_utime_total = record->data.ru_utime;
		data->ru_stime_total = record->data.ru_stime;
		data->kbytes_total = record->data.doc_size;
		data->status = record->data.status;

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report8_data *)*ppvalue;
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

void pinba_update_report8_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_STATUS_SIZE] = {0};
	struct pinba_report8_data *data;
	PPvoid_t ppvalue;

	if (report->results_cnt == 0) {
		return;
	}

	timersub(&report->time_total, &record->data.req_time, &report->time_total);
	timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total -= record->data.doc_size;

	sprintf((char *)index, "%u", record->data.status);

	ppvalue = JudySLGet(report->results, index, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, mmm?? */
		return;
	} else {
		data = (struct pinba_report8_data *)*ppvalue;
		if (UNLIKELY(data->req_count == 1)) {
			free(data);
			JudySLDel(&report->results, index, NULL);
			report->results_cnt--;
		} else {
			data->req_count--;
			timersub(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timersub(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timersub(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total -= record->data.doc_size;
		}
	}
}
/* }}} */

void pinba_update_report9_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_STATUS_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE] = {0};
	struct pinba_report9_data *data;
	PPvoid_t ppvalue;
	int index_len, dummy;

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;

	index_len = sprintf((char *)index, "%u:", record->data.status);
	memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);

	ppvalue = JudySLGet(report->results, index, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, insert */
		ppvalue = JudySLIns(&report->results, index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return;
		}
		data = (struct pinba_report9_data *)malloc(sizeof(struct pinba_report9_data));

		data->req_count = 1;
		data->req_time_total = record->data.req_time;
		data->ru_utime_total = record->data.ru_utime;
		data->ru_stime_total = record->data.ru_stime;
		data->kbytes_total = record->data.doc_size;

		memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
		data->status = record->data.status;

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report9_data *)*ppvalue;
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

void pinba_update_report9_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_STATUS_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE] = {0};
	struct pinba_report9_data *data;
	PPvoid_t ppvalue;
	int index_len;

	if (report->results_cnt == 0) {
		return;
	}

	timersub(&report->time_total, &record->data.req_time, &report->time_total);
	timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total -= record->data.doc_size;

	index_len = sprintf((char *)index, "%u:", record->data.status);
	memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);

	ppvalue = JudySLGet(report->results, index, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, mmm?? */
		return;
	} else {
		data = (struct pinba_report9_data *)*ppvalue;
		if (UNLIKELY(data->req_count == 1)) {
			free(data);
			JudySLDel(&report->results, index, NULL);
			report->results_cnt--;
		} else {
			data->req_count--;
			timersub(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timersub(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timersub(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total -= record->data.doc_size;
		}
	}
}
/* }}} */

void pinba_update_report10_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_STATUS_SIZE + 1 + PINBA_SERVER_NAME_SIZE] = {0};
	struct pinba_report10_data *data;
	PPvoid_t ppvalue;
	int index_len, dummy;

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;

	index_len = sprintf((char *)index, "%u:", record->data.status);
	memcat_static(index, index_len, record->data.server_name, record->data.server_name_len, index_len);

	ppvalue = JudySLGet(report->results, index, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, insert */
		ppvalue = JudySLIns(&report->results, index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return;
		}
		data = (struct pinba_report10_data *)malloc(sizeof(struct pinba_report10_data));

		data->req_count = 1;
		data->req_time_total = record->data.req_time;
		data->ru_utime_total = record->data.ru_utime;
		data->ru_stime_total = record->data.ru_stime;
		data->kbytes_total = record->data.doc_size;

		memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);
		data->status = record->data.status;

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report10_data *)*ppvalue;
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

void pinba_update_report10_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_STATUS_SIZE + 1 + PINBA_SERVER_NAME_SIZE] = {0};
	struct pinba_report10_data *data;
	PPvoid_t ppvalue;
	int index_len;

	if (report->results_cnt == 0) {
		return;
	}

	timersub(&report->time_total, &record->data.req_time, &report->time_total);
	timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total -= record->data.doc_size;

	index_len = sprintf((char *)index, "%u:", record->data.status);
	memcat_static(index, index_len, record->data.server_name, record->data.server_name_len, index_len);

	ppvalue = JudySLGet(report->results, index, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, mmm?? */
		return;
	} else {
		data = (struct pinba_report10_data *)*ppvalue;
		if (UNLIKELY(data->req_count == 1)) {
			free(data);
			JudySLDel(&report->results, index, NULL);
			report->results_cnt--;
		} else {
			data->req_count--;
			timersub(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timersub(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timersub(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total -= record->data.doc_size;
		}
	}
}
/* }}} */

void pinba_update_report11_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_STATUS_SIZE + 1 + PINBA_HOSTNAME_SIZE] = {0};
	struct pinba_report11_data *data;
	PPvoid_t ppvalue;
	int index_len, dummy;

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;

	index_len = sprintf((char *)index, "%u:", record->data.status);
	memcat_static(index, index_len, record->data.hostname, record->data.hostname_len, index_len);

	ppvalue = JudySLGet(report->results, index, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, insert */
		ppvalue = JudySLIns(&report->results, index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return;
		}
		data = (struct pinba_report11_data *)malloc(sizeof(struct pinba_report11_data));

		data->req_count = 1;
		data->req_time_total = record->data.req_time;
		data->ru_utime_total = record->data.ru_utime;
		data->ru_stime_total = record->data.ru_stime;
		data->kbytes_total = record->data.doc_size;

		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		data->status = record->data.status;

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report11_data *)*ppvalue;
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

void pinba_update_report11_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_STATUS_SIZE + 1 + PINBA_HOSTNAME_SIZE] = {0};
	struct pinba_report11_data *data;
	PPvoid_t ppvalue;
	int index_len;

	if (report->results_cnt == 0) {
		return;
	}

	timersub(&report->time_total, &record->data.req_time, &report->time_total);
	timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total -= record->data.doc_size;

	index_len = sprintf((char *)index, "%u:", record->data.status);
	memcat_static(index, index_len, record->data.hostname, record->data.hostname_len, index_len);

	ppvalue = JudySLGet(report->results, index, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, mmm?? */
		return;
	} else {
		data = (struct pinba_report11_data *)*ppvalue;
		if (UNLIKELY(data->req_count == 1)) {
			free(data);
			JudySLDel(&report->results, index, NULL);
			report->results_cnt--;
		} else {
			data->req_count--;
			timersub(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timersub(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timersub(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total -= record->data.doc_size;
		}
	}
}
/* }}} */

void pinba_update_report12_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_STATUS_SIZE + 1 + PINBA_HOSTNAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE] = {0};
	struct pinba_report12_data *data;
	PPvoid_t ppvalue;
	int index_len, dummy;

	timeradd(&report->time_total, &record->data.req_time, &report->time_total);
	timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total += record->data.doc_size;

	index_len = sprintf((char *)index, "%u:", record->data.status);
	memcat_static(index, index_len, record->data.hostname, record->data.hostname_len, index_len);
	index[index_len] = '/'; index_len++;
	memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);

	ppvalue = JudySLGet(report->results, index, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, insert */
		ppvalue = JudySLIns(&report->results, index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return;
		}
		data = (struct pinba_report12_data *)malloc(sizeof(struct pinba_report12_data));

		data->req_count = 1;
		data->req_time_total = record->data.req_time;
		data->ru_utime_total = record->data.ru_utime;
		data->ru_stime_total = record->data.ru_stime;
		data->kbytes_total = record->data.doc_size;

		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
		data->status = record->data.status;

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report12_data *)*ppvalue;
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

void pinba_update_report12_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_STATUS_SIZE + 1 + PINBA_HOSTNAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE] = {0};
	struct pinba_report12_data *data;
	PPvoid_t ppvalue;
	int index_len;

	if (report->results_cnt == 0) {
		return;
	}

	timersub(&report->time_total, &record->data.req_time, &report->time_total);
	timersub(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
	timersub(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
	report->kbytes_total -= record->data.doc_size;

	index_len = sprintf((char *)index, "%u:", record->data.status);
	memcat_static(index, index_len, record->data.hostname, record->data.hostname_len, index_len);
	index[index_len] = '/'; index_len++;
	memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);

	ppvalue = JudySLGet(report->results, index, NULL);

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		/* no such value, mmm?? */
		return;
	} else {
		data = (struct pinba_report12_data *)*ppvalue;
		if (UNLIKELY(data->req_count == 1)) {
			free(data);
			JudySLDel(&report->results, index, NULL);
			report->results_cnt--;
		} else {
			data->req_count--;
			timersub(&data->req_time_total, &record->data.req_time, &data->req_time_total);
			timersub(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
			timersub(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
			data->kbytes_total -= record->data.doc_size;
		}
	}
}
/* }}} */


void pinba_update_tag_info_add(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tag_info_data *data;
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, tag_found;
	pinba_word *word;

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag_found = 0;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag1_id == timer->tag_ids[j]) {
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

			data = (struct pinba_tag_info_data *)malloc(sizeof(struct pinba_tag_info_data));
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

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag_found = 0;
		timer = record_get_timer(&D->timer_pool, record, i);

		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag1_id == timer->tag_ids[j]) {
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

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag1_pos = -1;
		tag2_pos = -1;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag1_id == timer->tag_ids[j]) {
				tag1_pos = j;
				continue;
			}
			if (report->tag2_id == timer->tag_ids[j]) {
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

			data = (struct pinba_tag2_info_data *)malloc(sizeof(struct pinba_tag2_info_data));
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

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag1_pos = -1;
		tag2_pos = -1;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag1_id == timer->tag_ids[j]) {
				tag1_pos = j;
				continue;
			}
			if (report->tag2_id == timer->tag_ids[j]) {
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

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	ppvalue_script = JudySLGet(report->results, (uint8_t *)record->data.script_name, NULL);

	if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {

		ppvalue_script = JudySLIns(&report->results, (uint8_t *)record->data.script_name, NULL);
		if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
			return;
		}
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag_found = 0;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag1_id == timer->tag_ids[j]) {
				tag_found = 1;
				break;
			}
		}

		if (!tag_found) {
			continue;
		}

		word = (pinba_word *)timer->tag_values[j];

		ppvalue = JudySLGet(*ppvalue_script, (uint8_t *)word->str, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {

			ppvalue = JudySLIns(ppvalue_script, (uint8_t *)word->str, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				continue;
			}

			data = (struct pinba_tag_report_data *)malloc(sizeof(struct pinba_tag_report_data));
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

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	ppvalue_script = JudySLGet(report->results, (uint8_t *)record->data.script_name, NULL);

	if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
		return;
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag_found = 0;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag1_id == timer->tag_ids[j]) {
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

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	ppvalue_script = JudySLGet(report->results, (uint8_t *)record->data.script_name, NULL);
	if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {

		ppvalue_script = JudySLIns(&report->results, (uint8_t *)record->data.script_name, NULL);
		if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
			return;
		}
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag1_pos = -1;
		tag2_pos = -1;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag1_id == timer->tag_ids[j]) {
				tag1_pos = j;
				continue;
			}
			if (report->tag2_id == timer->tag_ids[j]) {
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

		ppvalue = JudySLGet(*ppvalue_script, index_val, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			ppvalue = JudySLIns(ppvalue_script, index_val, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				continue;
			}

			data = (struct pinba_tag2_report_data *)malloc(sizeof(struct pinba_tag2_report_data));
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

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	ppvalue_script = JudySLGet(report->results, (uint8_t *)record->data.script_name, NULL);
	if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
		return;
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag1_pos = -1;
		tag2_pos = -1;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag1_id == timer->tag_ids[j]) {
				tag1_pos = j;
				continue;
			}
			if (report->tag2_id == timer->tag_ids[j]) {
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
			}
		}
	}
}
/* }}} */

void pinba_update_tag_report2_add(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tag_report2_data *data;
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, tag_found, index_len, dummy;
	uint8_t index[PINBA_SCRIPT_NAME_SIZE + PINBA_HOSTNAME_SIZE + PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE];
	pinba_word *word;

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag_found = 0;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag1_id == timer->tag_ids[j]) {
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
		memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);
		index[index_len] = '|'; index_len++;
		memcat_static(index, index_len, word->str, word->len, index_len);

		ppvalue = JudySLGet(report->results, index, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {

			ppvalue = JudySLIns(&report->results, index, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				continue;
			}

			data = (struct pinba_tag_report2_data *)malloc(sizeof(struct pinba_tag_report2_data));
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
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, tag_found, index_len;
	uint8_t index[PINBA_HOSTNAME_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE];
	pinba_word *word;

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag_found = 0;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag1_id == timer->tag_ids[j]) {
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
		memcat_static(index, index_len, record->data.script_name, record->data.script_name_len, index_len);
		index[index_len] = '|'; index_len++;
		memcat_static(index, index_len, word->str, word->len, index_len);

		ppvalue = JudySLGet(report->results, index, NULL);

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
				JudySLDel(&report->results, index, NULL);
				report->results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
			}
		}
	}
}
/* }}} */

void pinba_update_tag2_report2_add(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tag2_report2_data *data;
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, tag1_pos, tag2_pos, dummy;
	int index_len;
	uint8_t index_val[PINBA_HOSTNAME_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE];
	pinba_word *word1, *word2;

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag1_pos = -1;
		tag2_pos = -1;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag1_id == timer->tag_ids[j]) {
				tag1_pos = j;
				continue;
			}
			if (report->tag2_id == timer->tag_ids[j]) {
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
		memcat_static(index_val, index_len, record->data.script_name, record->data.script_name_len, index_len);
		index_val[index_len] = '|'; index_len++;
		memcat_static(index_val, index_len, word1->str, word1->len, index_len);
		index_val[index_len] = '|'; index_len++;
		memcat_static(index_val, index_len, word2->str, word2->len, index_len);

		ppvalue = JudySLGet(report->results, index_val, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			ppvalue = JudySLIns(&report->results, index_val, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				continue;
			}

			data = (struct pinba_tag2_report2_data *)malloc(sizeof(struct pinba_tag2_report2_data));
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
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, tag1_pos, tag2_pos;
	int index_len;
	uint8_t index_val[PINBA_HOSTNAME_SIZE + PINBA_SERVER_NAME_SIZE + PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE];
	pinba_word *word1, *word2;

	if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
		if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
			return;
		}
		if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
			return;
		}
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag1_pos = -1;
		tag2_pos = -1;
		timer = record_get_timer(&D->timer_pool, record, i);
		for (j = 0; j < timer->tag_num; j++) {
			if (report->tag1_id == timer->tag_ids[j]) {
				tag1_pos = j;
				continue;
			}
			if (report->tag2_id == timer->tag_ids[j]) {
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
		memcat_static(index_val, index_len, record->data.script_name, record->data.script_name_len, index_len);
		index_val[index_len] = '|'; index_len++;
		memcat_static(index_val, index_len, word1->str, word1->len, index_len);
		index_val[index_len] = '|'; index_len++;
		memcat_static(index_val, index_len, word2->str, word2->len, index_len);

		ppvalue = JudySLGet(report->results, index_val, NULL);

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
				JudySLDel(&report->results, (uint8_t *)index_val, NULL);
				report->results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
			}
		}
	}
}
/* }}} */

void pinba_update_tag_reports_add(int request_id, const pinba_stats_record *record) /* {{{ */
{
	pinba_tag_report *report;
	int i;

	for (i = 0; i < D->tag_reports_arr_size; i++) {
		report = (pinba_tag_report *)D->tag_reports_arr[i];

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
		free(report);
	}
}
/* }}} */

void pinba_tag_reports_destroy(int force) /* {{{ */
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

		if (force || (D->settings.tag_report_timeout != -1 && (report->last_requested + D->settings.tag_report_timeout) < now)) {
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
			free(report);
		}
	}
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
