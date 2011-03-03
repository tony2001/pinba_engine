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

static inline void pinba_update_report_info_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	report->time_total += timeval_to_float(record->data.req_time);
	report->ru_utime_total += timeval_to_float(record->data.ru_utime);
	report->ru_stime_total += timeval_to_float(record->data.ru_stime);
	report->kbytes_total += record->data.doc_size;
	report->results_cnt++;
}
/* }}} */

static inline void pinba_update_report_info_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	if (UNLIKELY(report->results_cnt == 0)) {
		return;
	}

	report->time_total -= timeval_to_float(record->data.req_time);
	report->ru_utime_total -= timeval_to_float(record->data.ru_utime);
	report->ru_stime_total -= timeval_to_float(record->data.ru_stime);
	report->kbytes_total -= record->data.doc_size;
	report->results_cnt--;

	if (UNLIKELY(report->results_cnt == 0)) {
		report->time_total = 0;
		report->ru_utime_total = 0;
		report->ru_stime_total = 0;
		report->kbytes_total = 0;
	}
}
/* }}} */

static inline void pinba_update_report1_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_report1_data *data;
	PPvoid_t ppvalue;

	report->time_total += timeval_to_float(record->data.req_time);
	report->ru_utime_total += timeval_to_float(record->data.ru_utime);
	report->ru_stime_total += timeval_to_float(record->data.ru_stime);
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
		data->req_time_total = timeval_to_float(record->data.req_time);
		data->ru_utime_total = timeval_to_float(record->data.ru_utime);
		data->ru_stime_total = timeval_to_float(record->data.ru_stime);
		data->kbytes_total = record->data.doc_size;

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report1_data *)*ppvalue;
		data->req_count++;
		data->req_time_total += timeval_to_float(record->data.req_time);
		data->ru_utime_total += timeval_to_float(record->data.ru_utime);
		data->ru_stime_total += timeval_to_float(record->data.ru_stime);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

static inline void pinba_update_report1_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_report1_data *data;
	PPvoid_t ppvalue;

	if (report->results_cnt == 0) {
		return;
	}

	report->time_total -= timeval_to_float(record->data.req_time);
	report->ru_utime_total -= timeval_to_float(record->data.ru_utime);
	report->ru_stime_total -= timeval_to_float(record->data.ru_stime);
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
			data->req_time_total -= timeval_to_float(record->data.req_time);
			data->ru_utime_total -= timeval_to_float(record->data.ru_utime);
			data->ru_stime_total -= timeval_to_float(record->data.ru_stime);
			data->kbytes_total -= record->data.doc_size;
		}	
	}
}
/* }}} */

static inline void pinba_update_report2_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_report2_data *data;
	PPvoid_t ppvalue;

	report->time_total += timeval_to_float(record->data.req_time);
	report->ru_utime_total += timeval_to_float(record->data.ru_utime);
	report->ru_stime_total += timeval_to_float(record->data.ru_stime);
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
		data->req_time_total = timeval_to_float(record->data.req_time);
		data->ru_utime_total = timeval_to_float(record->data.ru_utime);
		data->ru_stime_total = timeval_to_float(record->data.ru_stime);
		data->kbytes_total = record->data.doc_size;

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report2_data *)*ppvalue;
		data->req_count++;
		data->req_time_total += timeval_to_float(record->data.req_time);
		data->ru_utime_total += timeval_to_float(record->data.ru_utime);
		data->ru_stime_total += timeval_to_float(record->data.ru_stime);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

static inline void pinba_update_report2_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_report2_data *data;
	PPvoid_t ppvalue;

	if (report->results_cnt == 0) {
		return;
	}

	report->time_total -= timeval_to_float(record->data.req_time);
	report->ru_utime_total -= timeval_to_float(record->data.ru_utime);
	report->ru_stime_total -= timeval_to_float(record->data.ru_stime);
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
			data->req_time_total -= timeval_to_float(record->data.req_time);
			data->ru_utime_total -= timeval_to_float(record->data.ru_utime);
			data->ru_stime_total -= timeval_to_float(record->data.ru_stime);
			data->kbytes_total -= record->data.doc_size;
		}	
	}
}
/* }}} */

static inline void pinba_update_report3_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_report3_data *data;
	PPvoid_t ppvalue;

	report->time_total += timeval_to_float(record->data.req_time);
	report->ru_utime_total += timeval_to_float(record->data.ru_utime);
	report->ru_stime_total += timeval_to_float(record->data.ru_stime);
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
		data->req_time_total = timeval_to_float(record->data.req_time);
		data->ru_utime_total = timeval_to_float(record->data.ru_utime);
		data->ru_stime_total = timeval_to_float(record->data.ru_stime);
		data->kbytes_total = record->data.doc_size;

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report3_data *)*ppvalue;
		data->req_count++;
		data->req_time_total += timeval_to_float(record->data.req_time);
		data->ru_utime_total += timeval_to_float(record->data.ru_utime);
		data->ru_stime_total += timeval_to_float(record->data.ru_stime);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

static inline void pinba_update_report3_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_report3_data *data;
	PPvoid_t ppvalue;

	if (report->results_cnt == 0) {
		return;
	}

	report->time_total -= timeval_to_float(record->data.req_time);
	report->ru_utime_total -= timeval_to_float(record->data.ru_utime);
	report->ru_stime_total -= timeval_to_float(record->data.ru_stime);
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
			data->req_time_total -= timeval_to_float(record->data.req_time);
			data->ru_utime_total -= timeval_to_float(record->data.ru_utime);
			data->ru_stime_total -= timeval_to_float(record->data.ru_stime);
			data->kbytes_total -= record->data.doc_size;
		}	
	}
}
/* }}} */

static inline void pinba_update_report4_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_SERVER_NAME_SIZE + PINBA_SCRIPT_NAME_SIZE + 1] = {0};
	struct pinba_report4_data *data;
	PPvoid_t ppvalue;
	int index_len, dummy;

	report->time_total += timeval_to_float(record->data.req_time);
	report->ru_utime_total += timeval_to_float(record->data.ru_utime);
	report->ru_stime_total += timeval_to_float(record->data.ru_stime);
	report->kbytes_total += record->data.doc_size;

	memcpy_static(index, record->data.server_name, record->data.server_name_len, index_len);
	memcat_static(index, index_len, "/", 1, index_len);
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
		data->req_time_total = timeval_to_float(record->data.req_time);
		data->ru_utime_total = timeval_to_float(record->data.ru_utime);
		data->ru_stime_total = timeval_to_float(record->data.ru_stime);
		data->kbytes_total = record->data.doc_size;
		memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);
		memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report4_data *)*ppvalue;
		data->req_count++;
		data->req_time_total += timeval_to_float(record->data.req_time);
		data->ru_utime_total += timeval_to_float(record->data.ru_utime);
		data->ru_stime_total += timeval_to_float(record->data.ru_stime);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

static inline void pinba_update_report4_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_SERVER_NAME_SIZE + PINBA_SCRIPT_NAME_SIZE + 1] = {0};
	struct pinba_report4_data *data;
	PPvoid_t ppvalue;
	int index_len;

	if (report->results_cnt == 0) {
		return;
	}

	report->time_total -= timeval_to_float(record->data.req_time);
	report->ru_utime_total -= timeval_to_float(record->data.ru_utime);
	report->ru_stime_total -= timeval_to_float(record->data.ru_stime);
	report->kbytes_total -= record->data.doc_size;

	memcpy_static(index, record->data.server_name, record->data.server_name_len, index_len);
	memcat_static(index, index_len, "/", 1, index_len);
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
			data->req_time_total -= timeval_to_float(record->data.req_time);
			data->ru_utime_total -= timeval_to_float(record->data.ru_utime);
			data->ru_stime_total -= timeval_to_float(record->data.ru_stime);
			data->kbytes_total -= record->data.doc_size;
		}	
	}
}
/* }}} */

static inline void pinba_update_report5_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_HOSTNAME_SIZE + PINBA_SCRIPT_NAME_SIZE + 1] = {0};
	struct pinba_report5_data *data;
	PPvoid_t ppvalue;
	int index_len, dummy;

	report->time_total += timeval_to_float(record->data.req_time);
	report->ru_utime_total += timeval_to_float(record->data.ru_utime);
	report->ru_stime_total += timeval_to_float(record->data.ru_stime);
	report->kbytes_total += record->data.doc_size;

	memcpy_static(index, record->data.hostname, record->data.hostname_len, index_len);
	memcat_static(index, index_len, ":", 1, index_len);
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
		data->req_time_total = timeval_to_float(record->data.req_time);
		data->ru_utime_total = timeval_to_float(record->data.ru_utime);
		data->ru_stime_total = timeval_to_float(record->data.ru_stime);
		data->kbytes_total = record->data.doc_size;
		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report5_data *)*ppvalue;
		data->req_count++;
		data->req_time_total += timeval_to_float(record->data.req_time);
		data->ru_utime_total += timeval_to_float(record->data.ru_utime);
		data->ru_stime_total += timeval_to_float(record->data.ru_stime);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

static inline void pinba_update_report5_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_HOSTNAME_SIZE + PINBA_SCRIPT_NAME_SIZE + 1] = {0};
	struct pinba_report5_data *data;
	PPvoid_t ppvalue;
	int index_len;

	if (report->results_cnt == 0) {
		return;
	}

	report->time_total -= timeval_to_float(record->data.req_time);
	report->ru_utime_total -= timeval_to_float(record->data.ru_utime);
	report->ru_stime_total -= timeval_to_float(record->data.ru_stime);
	report->kbytes_total -= record->data.doc_size;

	memcpy_static(index, record->data.hostname, record->data.hostname_len, index_len);
	memcat_static(index, index_len, ":", 1, index_len);
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
			data->req_time_total -= timeval_to_float(record->data.req_time);
			data->ru_utime_total -= timeval_to_float(record->data.ru_utime);
			data->ru_stime_total -= timeval_to_float(record->data.ru_stime);
			data->kbytes_total -= record->data.doc_size;
		}	
	}
}
/* }}} */

static inline void pinba_update_report6_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_HOSTNAME_SIZE + PINBA_SERVER_NAME_SIZE + 1] = {0};
	struct pinba_report6_data *data;
	PPvoid_t ppvalue;
	int index_len, dummy;

	report->time_total += timeval_to_float(record->data.req_time);
	report->ru_utime_total += timeval_to_float(record->data.ru_utime);
	report->ru_stime_total += timeval_to_float(record->data.ru_stime);
	report->kbytes_total += record->data.doc_size;

	memcpy_static(index, record->data.hostname, record->data.hostname_len, index_len);
	memcat_static(index, index_len, "/", 1, index_len);
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
		data->req_time_total = timeval_to_float(record->data.req_time);
		data->ru_utime_total = timeval_to_float(record->data.ru_utime);
		data->ru_stime_total = timeval_to_float(record->data.ru_stime);
		data->kbytes_total = record->data.doc_size;

		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report6_data *)*ppvalue;
		data->req_count++;
		data->req_time_total += timeval_to_float(record->data.req_time);
		data->ru_utime_total += timeval_to_float(record->data.ru_utime);
		data->ru_stime_total += timeval_to_float(record->data.ru_stime);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

static inline void pinba_update_report6_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_HOSTNAME_SIZE + PINBA_SERVER_NAME_SIZE + 1] = {0};
	struct pinba_report6_data *data;
	PPvoid_t ppvalue;
	int index_len;

	if (report->results_cnt == 0) {
		return;
	}

	report->time_total -= timeval_to_float(record->data.req_time);
	report->ru_utime_total -= timeval_to_float(record->data.ru_utime);
	report->ru_stime_total -= timeval_to_float(record->data.ru_stime);
	report->kbytes_total -= record->data.doc_size;

	memcpy_static(index, record->data.hostname, record->data.hostname_len, index_len);
	memcat_static(index, index_len, "/", 1, index_len);
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
			data->req_time_total -= timeval_to_float(record->data.req_time);
			data->ru_utime_total -= timeval_to_float(record->data.ru_utime);
			data->ru_stime_total -= timeval_to_float(record->data.ru_stime);
			data->kbytes_total -= record->data.doc_size;
		}	
	}
}
/* }}} */

static inline void pinba_update_report7_add(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_HOSTNAME_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE] = {0};
	struct pinba_report7_data *data;
	PPvoid_t ppvalue;
	int index_len, dummy;

	report->time_total += timeval_to_float(record->data.req_time);
	report->ru_utime_total += timeval_to_float(record->data.ru_utime);
	report->ru_stime_total += timeval_to_float(record->data.ru_stime);
	report->kbytes_total += record->data.doc_size;

	memcpy_static(index, record->data.hostname, record->data.hostname_len, index_len);
	memcat_static(index, index_len, ":", 1, index_len);
	memcat_static(index, index_len, record->data.server_name, record->data.server_name_len, index_len);
	memcat_static(index, index_len, "/", 1, index_len);
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
		data->req_time_total = timeval_to_float(record->data.req_time);
		data->ru_utime_total = timeval_to_float(record->data.ru_utime);
		data->ru_stime_total = timeval_to_float(record->data.ru_stime);
		data->kbytes_total = record->data.doc_size;

		memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
		memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);
		memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);

		*ppvalue = data;
		report->results_cnt++;
	} else {
		data = (struct pinba_report7_data *)*ppvalue;
		data->req_count++;
		data->req_time_total += timeval_to_float(record->data.req_time);
		data->ru_utime_total += timeval_to_float(record->data.ru_utime);
		data->ru_stime_total += timeval_to_float(record->data.ru_stime);
		data->kbytes_total += record->data.doc_size;
	}
}
/* }}} */

static inline void pinba_update_report7_delete(pinba_report *report, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_HOSTNAME_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE] = {0};
	struct pinba_report7_data *data;
	PPvoid_t ppvalue;
	int index_len;

	if (report->results_cnt == 0) {
		return;
	}

	report->time_total -= timeval_to_float(record->data.req_time);
	report->ru_utime_total -= timeval_to_float(record->data.ru_utime);
	report->ru_stime_total -= timeval_to_float(record->data.ru_stime);
	report->kbytes_total -= record->data.doc_size;

	memcpy_static(index, record->data.hostname, record->data.hostname_len, index_len);
	memcat_static(index, index_len, ":", 1, index_len);
	memcat_static(index, index_len, record->data.server_name, record->data.server_name_len, index_len);
	memcat_static(index, index_len, "/", 1, index_len);
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
			data->req_time_total -= timeval_to_float(record->data.req_time);
			data->ru_utime_total -= timeval_to_float(record->data.ru_utime);
			data->ru_stime_total -= timeval_to_float(record->data.ru_stime);
			data->kbytes_total -= record->data.doc_size;
		}	
	}
}
/* }}} */


static inline void pinba_update_tag_info_add(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tag_info_data *data;
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, tag_found;
	pinba_word *word;

	for (i = 0; i < record->timers_cnt; i++) {
		tag_found = 0;
		timer = record->timers + i;
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

static inline void pinba_update_tag_info_delete(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tag_info_data *data;
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, tag_found;
	pinba_word *word;

	for (i = 0; i < record->timers_cnt; i++) {
		tag_found = 0;
		timer = record->timers + i;
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
			if (UNLIKELY(data->req_count == 1)) {
				free(data);
				JudySLDel(&report->results, (uint8_t *)word->str, NULL);
				report->results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
			}
		}

		/* count tag values only once per request */
		if (request_id != data->prev_del_request_id) {
			data->req_count--;
			data->prev_del_request_id = request_id;
		}
	}
}
/* }}} */

static inline void pinba_update_tag2_info_add(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
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
		timer = record->timers + i;
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
		memcat_static(index_val, index_len, "|", 1, index_len);
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

static inline void pinba_update_tag2_info_delete(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
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
		timer = record->timers + i;
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
		memcat_static(index_val, index_len, "|", 1, index_len);
		memcat_static(index_val, index_len, word2->str, word2->len, index_len);

		ppvalue = JudySLGet(report->results, index_val, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			continue;
		} else {
			data = (struct pinba_tag2_info_data *)*ppvalue;
			if (UNLIKELY(data->req_count == 1)) {
				free(data);
				JudySLDel(&report->results, (uint8_t *)index_val, NULL);
				report->results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
			}
		}

		/* count tag values only once per request */
		if (request_id != data->prev_del_request_id) {
			data->req_count--;
			data->prev_del_request_id = request_id;
		}
	}
}
/* }}} */

static inline void pinba_update_tag_report_add(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tag_report_data *data;
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, tag_found, index_len, dummy;
	uint8_t index[PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE];
	pinba_word *word;

	if (report->flags & PINBA_REPORT_CONDITIONAL) {
		if (report->cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->cond.min_time) {
			return;
		}
		if (report->cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->cond.max_time) {
			return;
		}
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag_found = 0;
		timer = record->timers + i;
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

		memcpy_static(index, record->data.script_name, record->data.script_name_len, index_len);
		memcat_static(index, index_len, "|", 1, index_len);
		memcat_static(index, index_len, word->str, word->len, index_len);

		ppvalue = JudySLGet(report->results, index, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {

			ppvalue = JudySLIns(&report->results, index, NULL);
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

static inline void pinba_update_tag_report_delete(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tag_report_data *data;
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, tag_found, index_len;
	uint8_t index[PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE];
	pinba_word *word;

	if (report->flags & PINBA_REPORT_CONDITIONAL) {
		if (report->cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->cond.min_time) {
			return;
		}
		if (report->cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->cond.max_time) {
			return;
		}
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag_found = 0;
		timer = record->timers + i;
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

		memcpy_static(index, record->data.script_name, record->data.script_name_len, index_len);
		memcat_static(index, index_len, "|", 1, index_len);
		memcat_static(index, index_len, word->str, word->len, index_len);

		ppvalue = JudySLGet(report->results, index, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			continue;
		} else {
			data = (struct pinba_tag_report_data *)*ppvalue;
			if (UNLIKELY(data->req_count == 1)) {
				free(data);
				JudySLDel(&report->results, index, NULL);
				report->results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
			}
		}

		/* count tag values only once per request */
		if (request_id != data->prev_del_request_id) {
			data->req_count--;
			data->prev_del_request_id = request_id;
		}
	}
}
/* }}} */

static inline void pinba_update_tag2_report_add(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tag2_report_data *data;
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, tag1_pos, tag2_pos, dummy;
	int index_len;
	uint8_t index_val[PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE];
	pinba_word *word1, *word2;

	if (report->flags & PINBA_REPORT_CONDITIONAL) {
		if (report->cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->cond.min_time) {
			return;
		}
		if (report->cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->cond.max_time) {
			return;
		}
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag1_pos = -1;
		tag2_pos = -1;
		timer = record->timers + i;
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

		memcpy_static(index_val, record->data.script_name, (int)record->data.script_name_len, index_len);
		memcat_static(index_val, index_len, "|", 1, index_len);
		memcat_static(index_val, index_len, word1->str, word1->len, index_len);
		memcat_static(index_val, index_len, "|", 1, index_len);
		memcat_static(index_val, index_len, word2->str, word2->len, index_len);

		ppvalue = JudySLGet(report->results, index_val, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			ppvalue = JudySLIns(&report->results, index_val, NULL);
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

static inline void pinba_update_tag2_report_delete(int request_id, pinba_tag_report *report, const pinba_stats_record *record) /* {{{ */
{
	struct pinba_tag2_report_data *data;
	PPvoid_t ppvalue;
	pinba_timer_record *timer;
	int i, j, tag1_pos, tag2_pos;
	int index_len;
	uint8_t index_val[PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE];
	pinba_word *word1, *word2;

	if (report->flags & PINBA_REPORT_CONDITIONAL) {
		if (report->cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->cond.min_time) {
			return;
		}
		if (report->cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->cond.max_time) {
			return;
		}
	}

	for (i = 0; i < record->timers_cnt; i++) {
		tag1_pos = -1;
		tag2_pos = -1;
		timer = record->timers + i;
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

		memcpy_static(index_val, record->data.script_name, (int)record->data.script_name_len, index_len);
		memcat_static(index_val, index_len, "|", 1, index_len);
		memcat_static(index_val, index_len, word1->str, word1->len, index_len);
		memcat_static(index_val, index_len, "|", 1, index_len);
		memcat_static(index_val, index_len, word2->str, word2->len, index_len);

		ppvalue = JudySLGet(report->results, index_val, NULL);

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			continue;
		} else {
			data = (struct pinba_tag2_report_data *)*ppvalue;
			if (UNLIKELY(data->req_count == 1)) {
				free(data);
				JudySLDel(&report->results, (uint8_t *)index_val, NULL);
				report->results_cnt--;
				continue;
			} else {
				data->hit_count -= timer->hit_count;
				timersub(&data->timer_value, &timer->value, &data->timer_value);
			}
		}

		/* count tag values only once per request */
		if (request_id != data->prev_del_request_id) {
			data->req_count--;
			data->prev_del_request_id = request_id;
		}
	}
}
/* }}} */


void pinba_update_tag_reports_add(int request_id, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	pinba_tag_report *report;
	PPvoid_t ppvalue;

	pthread_rwlock_rdlock(&D->tag_reports_lock);
	for (ppvalue = JudySLFirst(D->tag_reports, index, NULL); ppvalue != NULL && ppvalue != PPJERR; ppvalue = JudySLNext(D->tag_reports, index, NULL)) {
		report = (pinba_tag_report *)*ppvalue;

		pthread_rwlock_wrlock(&report->lock);
		switch (report->type) {
			case PINBA_TAG_REPORT_INFO:
				pinba_update_tag_info_add(request_id, report, record);
				break;
			case PINBA_TAG2_REPORT_INFO:
				pinba_update_tag2_info_add(request_id, report, record);
				break;
			case PINBA_TAG_REPORT:
				pinba_update_tag_report_add(request_id, report, record);
				break;
			case PINBA_TAG2_REPORT:
				pinba_update_tag2_report_add(request_id, report, record);
				break;
			default:
				pinba_error(P_WARNING, "unknown report type '%d'!", report->type);
				break;
		}
		pthread_rwlock_unlock(&report->lock);
	}
	pthread_rwlock_unlock(&D->tag_reports_lock);
}
/* }}} */

void pinba_update_tag_reports_delete(int request_id, const pinba_stats_record *record) /* {{{ */
{
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	pinba_tag_report *report;
	PPvoid_t ppvalue;

	pthread_rwlock_rdlock(&D->tag_reports_lock);
	for (ppvalue = JudySLFirst(D->tag_reports, index, NULL); ppvalue != NULL && ppvalue != PPJERR; ppvalue = JudySLNext(D->tag_reports, index, NULL)) {
		report = (pinba_tag_report *)*ppvalue;

		pthread_rwlock_wrlock(&report->lock);
		switch (report->type) {
			case PINBA_TAG_REPORT_INFO:
				pinba_update_tag_info_delete(request_id, report, record);
				break;
			case PINBA_TAG2_REPORT_INFO:
				pinba_update_tag2_info_delete(request_id, report, record);
				break;
			case PINBA_TAG_REPORT:
				pinba_update_tag_report_delete(request_id, report, record);
				break;
			case PINBA_TAG2_REPORT:
				pinba_update_tag2_report_delete(request_id, report, record);
				break;
			default:
				pinba_error(P_WARNING, "unknown report type '%d'!", report->type);
				break;
		}
		pthread_rwlock_unlock(&report->lock);
	}
	pthread_rwlock_unlock(&D->tag_reports_lock);
}
/* }}} */

struct report_job_data {
	const pinba_stats_record *record;
	int report_num;
	void (*func)(pinba_report *, const pinba_stats_record *);
};

void report_job(void *data) /* {{{ */
{
	struct report_job_data *job_data = (struct report_job_data *)data;

	pthread_rwlock_wrlock(&D->base_reports[job_data->report_num].lock);
	job_data->func(&D->base_reports[job_data->report_num], job_data->record);
	pthread_rwlock_unlock(&D->base_reports[job_data->report_num].lock);
}
/* }}} */

struct report_job_data add_data[] =  /* {{{ */
{
	{ NULL, PINBA_BASE_REPORT_INFO, pinba_update_report_info_add},
	{ NULL, PINBA_BASE_REPORT1, pinba_update_report1_add},
	{ NULL, PINBA_BASE_REPORT2, pinba_update_report2_add},
	{ NULL, PINBA_BASE_REPORT3, pinba_update_report3_add},
	{ NULL, PINBA_BASE_REPORT4, pinba_update_report4_add},
	{ NULL, PINBA_BASE_REPORT5, pinba_update_report5_add},
	{ NULL, PINBA_BASE_REPORT6, pinba_update_report6_add},
	{ NULL, PINBA_BASE_REPORT7, pinba_update_report7_add}
};
/* }}} */

void pinba_update_reports_add(const pinba_stats_record *record) /* {{{ */
{
	int i;

	for (i = 0; i < PINBA_BASE_REPORT_LAST; i++) {
		add_data[i].record = record;
		th_pool_dispatch(D->thread_pool, NULL, report_job, &add_data[i]);
	}
}
/* }}} */

struct report_job_data delete_data[] =  /* {{{ */
{
	{ NULL, PINBA_BASE_REPORT_INFO, pinba_update_report_info_delete},
	{ NULL, PINBA_BASE_REPORT1, pinba_update_report1_delete},
	{ NULL, PINBA_BASE_REPORT2, pinba_update_report2_delete},
	{ NULL, PINBA_BASE_REPORT3, pinba_update_report3_delete},
	{ NULL, PINBA_BASE_REPORT4, pinba_update_report4_delete},
	{ NULL, PINBA_BASE_REPORT5, pinba_update_report5_delete},
	{ NULL, PINBA_BASE_REPORT6, pinba_update_report6_delete},
	{ NULL, PINBA_BASE_REPORT7, pinba_update_report7_delete}
};
/* }}} */

void pinba_update_reports_delete(const pinba_stats_record *record) /* {{{ */
{
	int i;

	for (i = 0; i < PINBA_BASE_REPORT_LAST; i++) {
		add_data[i].record = record;
		th_pool_dispatch(D->thread_pool, NULL, report_job, &delete_data[i]);
	}
}
/* }}} */

static inline void pinba_report_results_dtor(pinba_report *report) /* {{{ */
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
	int i;
	pinba_report *report;

	for (i = 0; i < PINBA_BASE_REPORT_LAST; i++) {
		report = D->base_reports + i;

		pthread_rwlock_wrlock(&D->base_reports[i].lock);
		if (report->results_cnt) {
			pinba_report_results_dtor(report);

			report->time_interval = 0;
			report->results_cnt = 0;
			report->results = NULL;
			report->time_total = 0;
			report->kbytes_total = 0;
			report->ru_utime_total = 0;
			report->ru_stime_total = 0;
		}
		pthread_rwlock_unlock(&D->base_reports[i].lock);
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
			free(report);
		}
	}
	pthread_rwlock_unlock(&D->tag_reports_lock);
}
/* }}} */

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

/* 
 * vim600: sw=4 ts=4 fdm=marker
 */
