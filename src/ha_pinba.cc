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

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation        // gcc: Class implementation
#endif

#include "pinba.h"

#if defined(PINBA_ENGINE_DEBUG_ON) && !defined(DBUG_ON)
# undef DBUG_OFF
# define DBUG_ON
#endif

#if defined(PINBA_ENGINE_DEBUG_OFF) && !defined(DBUG_OFF)
# define DBUG_OFF
# undef DBUG_ON
#endif

#define MYSQL_SERVER 1
#ifdef PINBA_ENGINE_MYSQL_VERSION_5_5
# include <include/mysql_version.h>
# include <sql/field.h>
# include <sql/structs.h>
# include <sql/handler.h>
#else
# include <mysql_priv.h>
#endif
#include <my_dir.h>
#include <mysql/plugin.h>
#include <mysql.h>

#include "ha_pinba.h"

#ifdef PINBA_ENGINE_MYSQL_VERSION_5_5
# define pinba_free(a, b) my_free(a)
#else
# define pinba_free(a, b) my_free(a, b)
#endif

#ifndef hash_init
/* this is fucking annoying!
 * MySQL! or Sun! or Oracle! or whatever you're called this time of the day!
 * stop renaming the fucking functions and breaking the fucking API! 
 */

# define hash_get_key    my_hash_get_key
# define hash_init       my_hash_init
# define hash_free       my_hash_free
# define hash_search     my_hash_search
# define hash_delete     my_hash_delete

#endif


/* Global variables */
static pthread_t data_thread;
static pthread_t collector_thread;
static pthread_t stats_thread;
static int port_var = 0;
static char *address_var = NULL;
static int temp_pool_size_var = 0;
static int request_pool_size_var = 0;
static int stats_history_var = 0;
static int stats_gathering_period_var = 0;
static int tag_report_timeout_var = 0;
static int cpu_start_var = 0;
static my_bool show_protobuf_errors_var = 0;

/* global daemon struct, created once per process and used everywhere */
pinba_daemon *D;

/* prototypes */
static handler* pinba_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root);

/* Variables for pinba share methods */
static HASH pinba_open_tables; // Hash used to track open tables
pthread_mutex_t pinba_mutex;   // This is the mutex we use to init the hash

/* <utilities> {{{ */

static inline unsigned char pinba_get_table_type(TABLE *table) /* {{{ */
{
	char *str, *colon;
	size_t len;

	if (!table->s || !table->s->comment.length || !table->s->comment.str) {
		return PINBA_TABLE_UNKNOWN;
	}

	len = table->s->comment.length;
	str = table->s->comment.str;

	colon = strchr(str, ':');
	if (colon) {
		/* ignore params */
		len = colon - str;
	}

	switch(len) {
		case 12: /* sizeof("tag2_report2") */
			if (!memcmp(str, "tag2_report2", len)) {
				return PINBA_TABLE_TAG2_REPORT2;
			}
			break;
		case 11: /* sizeof("tag2_report") */
			if (!memcmp(str, "tag2_report", len)) {
				return PINBA_TABLE_TAG2_REPORT;
			}
			if (!memcmp(str, "tag_report2", len)) {
				return PINBA_TABLE_TAG_REPORT2;
			}
			break;
		case 10: /* sizeof("tag_report") - 1 */
			if (!memcmp(str, "tag_report", len)) {
				return PINBA_TABLE_TAG_REPORT;
			}
			break;
		case 9: /* sizeof("tag2_info") - 1 */
			if (!memcmp(str, "tag2_info", len)) {
				return PINBA_TABLE_TAG2_INFO;
			}
		case 8: /* sizeof("timertag") - 1 */
			if (!memcmp(str, "timertag", len)) {
				return PINBA_TABLE_TIMERTAG;
			}
			if (!memcmp(str, "tag_info", len)) {
				return PINBA_TABLE_TAG_INFO;
			}
			if (!memcmp(str, "report10", len)) {
				return PINBA_TABLE_REPORT10;
			}
			if (!memcmp(str, "report11", len)) {
				return PINBA_TABLE_REPORT11;
			}
			if (!memcmp(str, "report12", len)) {
				return PINBA_TABLE_REPORT12;
			}
			break;
		case 7: /* sizeof("request") - 1 */
			if (!memcmp(str, "request", len)) {
				return PINBA_TABLE_REQUEST;
			}
			if (!memcmp(str, "report1", len)) {
				return PINBA_TABLE_REPORT1;
			}
			if (!memcmp(str, "report2", len)) {
				return PINBA_TABLE_REPORT2;
			}
			if (!memcmp(str, "report3", len)) {
				return PINBA_TABLE_REPORT3;
			}
			if (!memcmp(str, "report4", len)) {
				return PINBA_TABLE_REPORT4;
			}
			if (!memcmp(str, "report5", len)) {
				return PINBA_TABLE_REPORT5;
			}
			if (!memcmp(str, "report6", len)) {
				return PINBA_TABLE_REPORT6;
			}
			if (!memcmp(str, "report7", len)) {
				return PINBA_TABLE_REPORT7;
			}
			if (!memcmp(str, "report8", len)) {
				return PINBA_TABLE_REPORT8;
			}
			if (!memcmp(str, "report9", len)) {
				return PINBA_TABLE_REPORT9;
			}
			break;
		case 5: /* sizeof("timer") - 1 */
			if (!memcmp(str, "timer", len)) {
				return PINBA_TABLE_TIMER;
			}
			break;
		case 4: /* sizeof("info") - 1 */
			if (!memcmp(str, "info", len)) {
				return PINBA_TABLE_REPORT_INFO;
			}
			break;
		case 3: /* sizeof("tag") - 1 */
			if (!memcmp(str, "tag", len)) {
				return PINBA_TABLE_TAG;
			}
	}
	return PINBA_TABLE_UNKNOWN;
}
/* }}} */

static inline int pinba_parse_params(TABLE *table, char ***params, int *param_num, char ***cond_names, char ***cond_values, int *cond_num) /* {{{ */
{
	char *str_copy, *colon, *comma, *p, *equal, *colon2;
	size_t len;
	int i, num = 0, c_num = 0;
	int parse_only = 0;

	if (params && param_num) {
		*params = NULL;
		*param_num = 0;
	} else {
		parse_only = 1;
	}

	if (!table->s || !table->s->comment.length || !table->s->comment.str) {
		return -1;
	}

	len = table->s->comment.length;
	str_copy = strdup(table->s->comment.str);

	colon = strchr(str_copy, ':');
	if (!colon) {
		/* no params */
		return 0;
	} 

	colon++; /* skip the colon */
	if (colon[0] == '\0') {
		/* colon was the last character */
		return -1;
	}
	
	colon2 = strchr(colon, ':');
	if (colon2) {
		*colon2 = '\0';
	}

	comma = strchr(colon, ',');
	if (!comma) {
		if (!parse_only) {
			*params = (char **)realloc(*params, (num + 1) * sizeof(char *));
			(*params)[num] = strdup(colon);
		}
		num++;
	} else {
		p = colon;
		do {
			if ((comma - p) > 0) {
				if (!parse_only) {
					*params = (char **)realloc(*params, (num + 1) * sizeof(char *));
					(*params)[num] = strndup(p, comma - p);
				}
				p = comma + 1;
				num++;
			} else {
				goto cleanup;
			}
		}
		while (p < colon2 && (comma = strchr(p, ',')) != NULL);
		
		if (!parse_only) {
			*params = (char **)realloc(*params, (num + 1) * sizeof(char *));
			(*params)[num] = strdup(p);
		}
		num++;
	}

	if (colon2) {
		p = colon2 + 1;

		/* there are some conditions in the comment */
		comma = strchr(p, ',');

		if (!parse_only) {
			*cond_names = NULL;
			*cond_values = NULL;
			*cond_num = 0;
		}

		if (!comma) {
			equal = strchr(p, '=');
			if (!equal) {
				goto cleanup;
			}
			if (!parse_only) {
				*cond_names = (char **)malloc(sizeof(char *));
				(*cond_names)[0] = strndup(p, equal - p);
				*cond_values = (char **)malloc(sizeof(char *));
				(*cond_values)[0] = strdup(equal + 1);
				c_num = 1;
			}
		} else {
			do {
				equal = strchr(p, '=');
				if (!equal) {
					goto cleanup;
				}
				if (!parse_only) {
					*cond_names = (char **)realloc(*cond_names, (c_num + 1) * sizeof(char *));
					(*cond_names)[c_num] = strndup(p, equal - p);
					*cond_values = (char **)realloc(*cond_values, (c_num + 1) * sizeof(char *));
					(*cond_values)[c_num] = strndup(equal + 1, comma - equal - 1);
					c_num++;
				}
				p = comma + 1;
			} while ((comma = strchr(p, ',')));

			if (!parse_only) {
				equal = strchr(p, '=');
				if (!equal) {
					goto cleanup;
				}
				*cond_names = (char **)realloc(*cond_names, (c_num + 1) * sizeof(char *));
				(*cond_names)[c_num] = strndup(p, equal - p);
				*cond_values = (char **)realloc(*cond_values, (c_num + 1) * sizeof(char *));
				(*cond_values)[c_num] = strdup(equal + 1);
				c_num++;
			}

		}
	}

	if (!parse_only) {
		*param_num = num;
		*cond_num = c_num;
	}

	free(str_copy);
	return num;
cleanup:

	free(str_copy);

	if (!parse_only) {
		for (i = 0; i < num; i++) {
			free((*params)[i]);		
		}
		free(*params);
		*param_num = 0;

		for (i = 0; i < c_num; i++) {
			free((*cond_names)[i]);
			free((*cond_values)[i]);
		}
		free(*cond_names);
		free(*cond_values);
		*cond_num = 0;
	}
	return -1;

}
/* }}} */

static inline int pinba_parse_conditions(PINBA_SHARE *share, pinba_std_report *report) /* {{{ */
{
	int i;

	if (!share->cond_num) {
		return 0;
	}

	for (i = 0; i < share->cond_num; i++) {
		if (strcmp(share->cond_names[i], "min_time") == 0) {
			report->flags |= PINBA_REPORT_CONDITIONAL;
			report->cond.min_time = strtod(share->cond_values[i], NULL); 
		} else if (strcmp(share->cond_names[i], "max_time") == 0) {
			report->flags |= PINBA_REPORT_CONDITIONAL;
			report->cond.max_time = strtod(share->cond_values[i], NULL); 
		} else {
			/* ignore other conditions for now */
		}
	}
	return 0;
}
/* }}} */

static inline float pinba_round(float num, int prec_index) /* {{{ */
{
	double fraction, integral;
	long fraction_int;

	fraction = modf(num, &integral);
	fraction_int = (long)(fraction*prec_index);

	num = (float)(integral + (double)fraction_int/prec_index);
	return num;
}
/* }}} */

static unsigned char* pinba_get_key(PINBA_SHARE *share, size_t *length, my_bool not_used __attribute__((unused))) /* {{{ */
{
	*length = share->table_name_length;
	return (unsigned char*) share->table_name;
}
/* }}} */

static int pinba_engine_init(void *p) /* {{{ */
{
	pinba_daemon_settings settings;
	handlerton *pinba_hton = (handlerton *)p;
	int cpu_cnt;

	DBUG_ENTER("pinba_engine_init");

	settings.stats_history = stats_history_var;
	settings.stats_gathering_period = stats_gathering_period_var;
	settings.request_pool_size = request_pool_size_var;
	settings.temp_pool_size = temp_pool_size_var;
	settings.tag_report_timeout = tag_report_timeout_var;
	settings.show_protobuf_errors = (int)show_protobuf_errors_var;
	settings.port = port_var;
	settings.address = address_var;

	if (pinba_collector_init(settings) != P_SUCCESS) {
		DBUG_RETURN(1);
	}

	if (pthread_create(&collector_thread, NULL, pinba_collector_main, NULL)) {
		pinba_collector_shutdown();
		DBUG_RETURN(1);
	}

	if (pthread_create(&data_thread, NULL, pinba_data_main, NULL)) {
		pthread_cancel(collector_thread);
		pinba_collector_shutdown();
		DBUG_RETURN(1);
	}

	if (pthread_create(&stats_thread, NULL, pinba_stats_main, NULL)) {
		pthread_cancel(collector_thread);
		pthread_cancel(data_thread);
		pinba_collector_shutdown();
		DBUG_RETURN(1);
	}

	cpu_cnt = pinba_get_processors_number();
	if (cpu_cnt >= 3) {
#ifdef PINBA_ENGINE_HAVE_PTHREAD_SETAFFINITY_NP
		cpu_set_t mask;

		CPU_ZERO(&mask);
		CPU_SET(cpu_start_var + 0, &mask);
		pthread_setaffinity_np(collector_thread, sizeof(mask), &mask);
		
		CPU_ZERO(&mask);
		CPU_SET(cpu_start_var + 1, &mask);
		pthread_setaffinity_np(data_thread, sizeof(mask), &mask);
		
		CPU_ZERO(&mask);
		CPU_SET(cpu_start_var + 2, &mask);
		pthread_setaffinity_np(stats_thread, sizeof(mask), &mask);
#endif
	}

	(void)pthread_mutex_init(&pinba_mutex, MY_MUTEX_INIT_FAST);
	(void)hash_init(&pinba_open_tables, system_charset_info, 32, 0, 0, (hash_get_key)pinba_get_key, 0, 0);

	pinba_hton->state = SHOW_OPTION_YES;
	pinba_hton->create = pinba_create_handler;

	DBUG_RETURN(0);
}
/* }}} */

static int pinba_engine_shutdown(void *p) /* {{{ */
{
	int error = 0;
	DBUG_ENTER("pinba_engine_shutdown");

	pthread_cancel(collector_thread);
	pthread_join(collector_thread, NULL);

	pthread_cancel(data_thread);
	pthread_join(data_thread, NULL);

	pthread_cancel(stats_thread);
	pthread_join(stats_thread, NULL);
	
	pinba_collector_shutdown();

	if (pinba_open_tables.records) {
		error = 1;
	}
	hash_free(&pinba_open_tables);
	pthread_mutex_destroy(&pinba_mutex);

	google::protobuf::ShutdownProtobufLibrary();

	DBUG_RETURN(0);
}
/* }}} */

static void netstr_to_key(const unsigned char *key, pinba_index_st *index) /* {{{ */
{
	index->str.len = key[0];
	if (index->str.val) {
		free(index->str.val);
	}
	if (index->str.len > 0) {
		index->str.val = (unsigned char *)strdup((const char *)key+2);
	} else {
		index->str.val = NULL;
	}
}
/* }}} */

/* </utilities> }}} */

/* <reports> {{{ */

static inline void pinba_get_tag_report_id(uint8_t *index, int type, PINBA_SHARE *share) /* {{{ */
{
	int len;

	if (share->params_num == 2) {
		len = sprintf((char *)index, "%d|%s|%s", type, share->params[0], share->params[1]);
	} else {
		len = sprintf((char *)index, "%d|%s", type, share->params[0]);
	}

	if (share->cond_num) {
		int i;
		for (i = 0; i < share->cond_num; i++) {
			len += sprintf((char *)index + len, "|%s=%s", share->cond_names[i], share->cond_values[i]);
		}
	}
}
/* }}} */

static inline void pinba_get_report_id(uint8_t *index, int type, PINBA_SHARE *share) /* {{{ */
{
	int len;

	len = sprintf((char *)index, "%d", type);

	if (share->cond_num) {
		int i;
		for (i = 0; i < share->cond_num; i++) {
			len += sprintf((char *)index + len, "|%s=%s", share->cond_names[i], share->cond_values[i]);
		}
	}
}
/* }}} */

static inline pinba_tag_report *pinba_get_tag_report(int type, PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;

	if (share->index[0] == '\0') {
		pinba_get_tag_report_id(share->index, type, share);
	}

	ppvalue = JudySLGet(D->tag_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		return NULL;
	}
	return (pinba_tag_report *)*ppvalue;
}
/* }}} */

static inline pinba_report *pinba_get_report(int type, PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;

	if (share->index[0] == '\0') {
		pinba_get_report_id(share->index, type, share);
	}

	ppvalue = JudySLGet(D->base_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		return NULL;
	}
	return (pinba_report *)*ppvalue;
}
/* }}} */

/* info */
static inline pinba_report *pinba_regenerate_info(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_report *report;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	unsigned int i;

	if (share->index[0] == '\0') {
		pinba_get_report_id(share->index, PINBA_TABLE_REPORT_INFO, share);
	}

	ppvalue = JudySLGet(D->base_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		report = (pinba_report *)calloc(1, sizeof(pinba_report));
		if (!report) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_REPORT_INFO;
		pthread_rwlock_init(&report->lock, 0);
		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->base_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}
		D->base_reports_cnt++;
		*ppvalue = report;
	} else {
		report = (pinba_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
			}
		}

		timeradd(&report->time_total, &record->data.req_time, &report->time_total);
		timeradd(&report->ru_utime_total, &record->data.ru_utime, &report->ru_utime_total);
		timeradd(&report->ru_stime_total, &record->data.ru_stime, &report->ru_stime_total);
		report->kbytes_total += record->data.doc_size;
		report->results_cnt++;
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* report1 */
static inline pinba_report *pinba_regenerate_report1(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_report *report;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	unsigned int i;
	struct pinba_report1_data *data;

	if (share->index[0] == '\0') {
		pinba_get_report_id(share->index, PINBA_TABLE_REPORT1, share);
	}

	ppvalue = JudySLGet(D->base_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		report = (pinba_report *)calloc(1, sizeof(pinba_report));
		if (!report) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_REPORT1;
		pthread_rwlock_init(&report->lock, 0);
		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->base_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}
		D->base_reports_cnt++;
		*ppvalue = report;
	} else {
		report = (pinba_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
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
				pthread_rwlock_unlock(&report->lock);
				return NULL;
			}
			data = (struct pinba_report1_data *)calloc(1, sizeof(struct pinba_report1_data));

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_report1_data *)*ppvalue;
		}

		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* report2 */
static inline pinba_report *pinba_regenerate_report2(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_report *report;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	unsigned int i;
	struct pinba_report2_data *data;

	if (share->index[0] == '\0') {
		pinba_get_report_id(share->index, PINBA_TABLE_REPORT2, share);
	}

	ppvalue = JudySLGet(D->base_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		report = (pinba_report *)calloc(1, sizeof(pinba_report));
		if (!report) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_REPORT2;
		pthread_rwlock_init(&report->lock, 0);
		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->base_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}
		D->base_reports_cnt++;
		*ppvalue = report;
	} else {
		report = (pinba_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
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
				pthread_rwlock_unlock(&report->lock);
				return NULL;
			}
			data = (struct pinba_report2_data *)calloc(1, sizeof(struct pinba_report2_data));

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_report2_data *)*ppvalue;
		}
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* report3 */
static inline pinba_report *pinba_regenerate_report3(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_report *report;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	unsigned int i;
	struct pinba_report3_data *data;

	if (share->index[0] == '\0') {
		pinba_get_report_id(share->index, PINBA_TABLE_REPORT3, share);
	}

	ppvalue = JudySLGet(D->base_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		report = (pinba_report *)calloc(1, sizeof(pinba_report));
		if (!report) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_REPORT3;
		pthread_rwlock_init(&report->lock, 0);
		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->base_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}
		D->base_reports_cnt++;
		*ppvalue = report;
	} else {
		report = (pinba_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
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
				pthread_rwlock_unlock(&report->lock);
				return NULL;
			}
			data = (struct pinba_report3_data *)calloc(1, sizeof(struct pinba_report3_data));

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_report3_data *)*ppvalue;
		}
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* report4 */
static inline pinba_report *pinba_regenerate_report4(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_report *report;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	unsigned int i;
	struct pinba_report4_data *data;
	uint8_t index[PINBA_SERVER_NAME_SIZE + PINBA_SCRIPT_NAME_SIZE + 1] = {0};
	int index_len, dummy;

	if (share->index[0] == '\0') {
		pinba_get_report_id(share->index, PINBA_TABLE_REPORT4, share);
	}

	ppvalue = JudySLGet(D->base_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		report = (pinba_report *)calloc(1, sizeof(pinba_report));
		if (!report) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_REPORT4;
		pthread_rwlock_init(&report->lock, 0);
		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->base_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}
		D->base_reports_cnt++;
		*ppvalue = report;
	} else {
		report = (pinba_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
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
				pthread_rwlock_unlock(&report->lock);
				return NULL;
			}
			data = (struct pinba_report4_data *)calloc(1, sizeof(struct pinba_report4_data));

			memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);
			memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_report4_data *)*ppvalue;
		}
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* report5 */
static inline pinba_report *pinba_regenerate_report5(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_report *report;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	unsigned int i;
	struct pinba_report5_data *data;
	uint8_t index[PINBA_HOSTNAME_SIZE + PINBA_SCRIPT_NAME_SIZE + 1] = {0};
	int index_len, dummy;

	if (share->index[0] == '\0') {
		pinba_get_report_id(share->index, PINBA_TABLE_REPORT5, share);
	}

	ppvalue = JudySLGet(D->base_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		report = (pinba_report *)calloc(1, sizeof(pinba_report));
		if (!report) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_REPORT5;
		pthread_rwlock_init(&report->lock, 0);
		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->base_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}
		D->base_reports_cnt++;
		*ppvalue = report;
	} else {
		report = (pinba_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
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
				pthread_rwlock_unlock(&report->lock);
				return NULL;
			}
			data = (struct pinba_report5_data *)calloc(1, sizeof(struct pinba_report5_data));

			memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
			memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_report5_data *)*ppvalue;
		}
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* report6 */
static inline pinba_report *pinba_regenerate_report6(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_report *report;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	unsigned int i;
	struct pinba_report6_data *data;
	uint8_t index[PINBA_HOSTNAME_SIZE + PINBA_SERVER_NAME_SIZE + 1] = {0};
	int index_len, dummy;

	if (share->index[0] == '\0') {
		pinba_get_report_id(share->index, PINBA_TABLE_REPORT6, share);
	}

	ppvalue = JudySLGet(D->base_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		report = (pinba_report *)calloc(1, sizeof(pinba_report));
		if (!report) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_REPORT6;
		pthread_rwlock_init(&report->lock, 0);
		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->base_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}
		D->base_reports_cnt++;
		*ppvalue = report;
	} else {
		report = (pinba_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
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
				pthread_rwlock_unlock(&report->lock);
				return NULL;
			}
			data = (struct pinba_report6_data *)calloc(1, sizeof(struct pinba_report6_data));

			memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
			memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_report6_data *)*ppvalue;
		}
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* report7 */
static inline pinba_report *pinba_regenerate_report7(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_report *report;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	unsigned int i;
	struct pinba_report7_data *data;
	uint8_t index[PINBA_HOSTNAME_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE] = {0};
	int index_len, dummy;

	if (share->index[0] == '\0') {
		pinba_get_report_id(share->index, PINBA_TABLE_REPORT7, share);
	}

	ppvalue = JudySLGet(D->base_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		report = (pinba_report *)calloc(1, sizeof(pinba_report));
		if (!report) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_REPORT7;
		pthread_rwlock_init(&report->lock, 0);
		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->base_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}
		D->base_reports_cnt++;
		*ppvalue = report;
	} else {
		report = (pinba_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
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
				pthread_rwlock_unlock(&report->lock);
				return NULL;
			}
			data = (struct pinba_report7_data *)calloc(1, sizeof(struct pinba_report7_data));


			memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
			memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);
			memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_report7_data *)*ppvalue;
		}
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* report8 */
static inline pinba_report *pinba_regenerate_report8(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_report *report;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	unsigned int i;
	struct pinba_report8_data *data;
	uint8_t index[PINBA_STATUS_SIZE] = {0};
	int index_len, dummy;

	if (share->index[0] == '\0') {
		pinba_get_report_id(share->index, PINBA_TABLE_REPORT8, share);
	}

	ppvalue = JudySLGet(D->base_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		report = (pinba_report *)calloc(1, sizeof(pinba_report));
		if (!report) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_REPORT8;
		pthread_rwlock_init(&report->lock, 0);
		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->base_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}
		D->base_reports_cnt++;
		*ppvalue = report;
	} else {
		report = (pinba_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
			}
		}

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
				pthread_rwlock_unlock(&report->lock);
				return NULL;
			}
			data = (struct pinba_report8_data *)calloc(1, sizeof(struct pinba_report8_data));

			data->status = record->data.status;

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_report8_data *)*ppvalue;
		}
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* report9 */
static inline pinba_report *pinba_regenerate_report9(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_report *report;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	unsigned int i;
	struct pinba_report9_data *data;
	uint8_t index[PINBA_STATUS_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE] = {0};
	int index_len, dummy;

	if (share->index[0] == '\0') {
		pinba_get_report_id(share->index, PINBA_TABLE_REPORT9, share);
	}

	ppvalue = JudySLGet(D->base_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		report = (pinba_report *)calloc(1, sizeof(pinba_report));
		if (!report) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_REPORT9;
		pthread_rwlock_init(&report->lock, 0);
		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->base_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}
		D->base_reports_cnt++;
		*ppvalue = report;
	} else {
		report = (pinba_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
			}
		}

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
				pthread_rwlock_unlock(&report->lock);
				return NULL;
			}
			data = (struct pinba_report9_data *)calloc(1, sizeof(struct pinba_report9_data));

			memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
			data->status = record->data.status;

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_report9_data *)*ppvalue;
		}
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* report10 */
static inline pinba_report *pinba_regenerate_report10(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_report *report;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	unsigned int i;
	struct pinba_report10_data *data;
	uint8_t index[PINBA_STATUS_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1] = {0};
	int index_len, dummy;

	if (share->index[0] == '\0') {
		pinba_get_report_id(share->index, PINBA_TABLE_REPORT10, share);
	}

	ppvalue = JudySLGet(D->base_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		report = (pinba_report *)calloc(1, sizeof(pinba_report));
		if (!report) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_REPORT10;
		pthread_rwlock_init(&report->lock, 0);
		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->base_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}
		D->base_reports_cnt++;
		*ppvalue = report;
	} else {
		report = (pinba_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
			}
		}

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
				pthread_rwlock_unlock(&report->lock);
				return NULL;
			}
			data = (struct pinba_report10_data *)calloc(1, sizeof(struct pinba_report10_data));

			memcpy_static(data->server_name, record->data.server_name, record->data.server_name_len, dummy);
			data->status = record->data.status;

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_report10_data *)*ppvalue;
		}
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* report11 */
static inline pinba_report *pinba_regenerate_report11(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_report *report;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	unsigned int i;
	struct pinba_report11_data *data;
	uint8_t index[PINBA_HOSTNAME_SIZE + 1 + PINBA_STATUS_SIZE] = {0};
	int index_len, dummy;

	if (share->index[0] == '\0') {
		pinba_get_report_id(share->index, PINBA_TABLE_REPORT11, share);
	}

	ppvalue = JudySLGet(D->base_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		report = (pinba_report *)calloc(1, sizeof(pinba_report));
		if (!report) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_REPORT11;
		pthread_rwlock_init(&report->lock, 0);
		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->base_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}
		D->base_reports_cnt++;
		*ppvalue = report;
	} else {
		report = (pinba_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
			}
		}

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
				pthread_rwlock_unlock(&report->lock);
				return NULL;
			}
			data = (struct pinba_report11_data *)calloc(1, sizeof(struct pinba_report11_data));

			memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
			data->status = record->data.status;

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_report11_data *)*ppvalue;
		}
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* report12 */
static inline pinba_report *pinba_regenerate_report12(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_report *report;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	unsigned int i;
	struct pinba_report12_data *data;
	uint8_t index[PINBA_STATUS_SIZE + 1 + PINBA_HOSTNAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE] = {0};
	int index_len, dummy;

	if (share->index[0] == '\0') {
		pinba_get_report_id(share->index, PINBA_TABLE_REPORT12, share);
	}

	ppvalue = JudySLGet(D->base_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		report = (pinba_report *)calloc(1, sizeof(pinba_report));
		if (!report) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_REPORT12;
		pthread_rwlock_init(&report->lock, 0);
		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->base_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}
		D->base_reports_cnt++;
		*ppvalue = report;
	} else {
		report = (pinba_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
			}
		}

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
				pthread_rwlock_unlock(&report->lock);
				return NULL;
			}
			data = (struct pinba_report12_data *)calloc(1, sizeof(struct pinba_report12_data));

			memcpy_static(data->hostname, record->data.hostname, record->data.hostname_len, dummy);
			memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
			data->status = record->data.status;

			*ppvalue = data;
			report->results_cnt++;
		} else {
			data = (struct pinba_report12_data *)*ppvalue;
		}
		data->req_count++;
		timeradd(&data->req_time_total, &record->data.req_time, &data->req_time_total);
		timeradd(&data->ru_utime_total, &record->data.ru_utime, &data->ru_utime_total);
		timeradd(&data->ru_stime_total, &record->data.ru_stime, &data->ru_stime_total);
		data->kbytes_total += record->data.doc_size;
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* tag info */
static inline pinba_tag_report *pinba_regenerate_tag_info(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_tag_report *report;
	int dummy, k;
	pinba_timer_record *timer;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	struct pinba_tag_info_data *data;
	unsigned int i, j;
	int tag_found;
	pinba_word *word;

	if (share->index[0] == '\0') {
		pinba_get_tag_report_id(share->index, PINBA_TABLE_TAG_INFO, share);
	}

	ppvalue = JudySLGet(D->tag_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pinba_tag *tag;

		/* no such report */
		ppvalue = JudySLGet(D->tag.name_index, (uint8_t *)share->params[0], NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			/* no such tag! */
			return NULL;
		}

		tag = (pinba_tag *)*ppvalue;

		report = (pinba_tag_report *)calloc(1, sizeof(pinba_tag_report));
		if (!report) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_TAG_INFO;
		report->time_interval = 1;
		report->last_requested = 0;
		report->results_cnt = 0;
		report->results = NULL;
		report->tag1_id = tag->id;
		pthread_rwlock_init(&report->lock, 0);

		memcpy_static(report->tag1, share->params[0], strlen(share->params[0]), dummy);
		
		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->tag_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}
		*ppvalue = report;
	} else {
		report = (pinba_tag_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	if (D->settings.tag_report_timeout == -1 || report->last_requested == 0) {
		/* really regenerate */
	} else {
		pthread_rwlock_unlock(&report->lock);
		return report;
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (!record->timers_cnt) {
			continue;
		}

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
			}
		}

		for (j = 0; j < record->timers_cnt; j++) {
			tag_found = 0;
			pthread_rwlock_rdlock(&D->timer_lock);
			timer = record_get_timer(&D->timer_pool, record, j);

			for (k = 0; k < timer->tag_num; k++) {
				if (report->tag1_id == timer->tag_ids[k]) {
					/* ok, timer has this tag */
					tag_found = 1;
					break;
				}
			}

			if (!tag_found) {
				/* tag not found in this timer */
				pthread_rwlock_unlock(&D->timer_lock);
				continue;
			}

			word = (pinba_word *)timer->tag_values[k];
			ppvalue = JudySLGet(report->results, (uint8_t *)word->str, NULL);

			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {

				ppvalue = JudySLIns(&report->results, (uint8_t *)word->str, NULL);
				if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
					pthread_rwlock_unlock(&D->timer_lock);
					continue;
				}

				data = (struct pinba_tag_info_data *)malloc(sizeof(struct pinba_tag_info_data));
				if (UNLIKELY(!data)) {
					pthread_rwlock_unlock(&D->timer_lock);
					continue;
				}

				data->req_count = 1;
				data->hit_count = timer->hit_count;
				data->timer_value = timer->value;
				data->prev_add_request_id = i;
				data->prev_del_request_id = -1;

				*ppvalue = data;
				report->results_cnt++;
			} else {
				data = (struct pinba_tag_info_data *)*ppvalue;
				data->hit_count += timer->hit_count;
				timeradd(&data->timer_value, &timer->value, &data->timer_value);
			}

			/* count tag values only once per request */
			if ((int)i != data->prev_add_request_id) {
				data->req_count++;
				data->prev_add_request_id = i;
			}
			pthread_rwlock_unlock(&D->timer_lock);
		}
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* tag2 info */
static inline pinba_tag_report *pinba_regenerate_tag2_info(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_tag_report *report;
	uint8_t index_val[PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE];
	int index_len, dummy, k;
	pinba_timer_record *timer;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	struct pinba_tag2_info_data *data;
	unsigned int i, j;
	int tag1_pos, tag2_pos;
	pinba_word *word1, *word2;

	if (share->index[0] == '\0') {
		pinba_get_tag_report_id(share->index, PINBA_TABLE_TAG2_INFO, share);
	}

	ppvalue = JudySLGet(D->tag_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pinba_tag *tag1, *tag2;

		/* no such report */
		ppvalue = JudySLGet(D->tag.name_index, (uint8_t *)share->params[0], NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return NULL;
		}

		tag1 = (pinba_tag *)*ppvalue;

		ppvalue = JudySLGet(D->tag.name_index, (uint8_t *)share->params[1], NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return NULL;
		}

		tag2 = (pinba_tag *)*ppvalue;

		report = (pinba_tag_report *)calloc(1, sizeof(pinba_tag_report));
		if (UNLIKELY(!report)) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_TAG2_INFO;
		report->time_interval = 1;
		report->last_requested = 0;
		report->results_cnt = 0;
		report->results = NULL;
		report->tag1_id = tag1->id;
		report->tag2_id = tag2->id;
		pthread_rwlock_init(&report->lock, 0);
		
		pthread_rwlock_wrlock(&report->lock);

		memcpy_static(report->tag1, share->params[0], strlen(share->params[0]), dummy);
		memcpy_static(report->tag2, share->params[1], strlen(share->params[1]), dummy);

		ppvalue = JudySLIns(&D->tag_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}

		*ppvalue = report;
	} else {
		report = (pinba_tag_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	if (D->settings.tag_report_timeout == -1 || report->last_requested == 0) {
		/* really regenerate */
	} else {
		pthread_rwlock_unlock(&report->lock);
		return report;
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (!record->timers_cnt) {
			continue;
		}

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
			}
		}

		for (j = 0; j < record->timers_cnt; j++) {
			tag1_pos = tag2_pos = -1;
			pthread_rwlock_rdlock(&D->timer_lock);
			timer = record_get_timer(&D->timer_pool, record, j);

			for (k = 0; k < timer->tag_num; k++) {
				if (report->tag1_id == timer->tag_ids[k]) {
					/* ok, timer has this tag */
					tag1_pos = k;
					continue;
				}
				if (report->tag2_id == timer->tag_ids[k]) {
					/* ok, timer has this tag */
					tag2_pos = k;
					continue;
				}
			}

			if (tag1_pos == -1 || tag2_pos == -1) {
				pthread_rwlock_unlock(&D->timer_lock);
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
					pthread_rwlock_unlock(&D->timer_lock);
					continue;
				}

				data = (struct pinba_tag2_info_data *)malloc(sizeof(struct pinba_tag2_info_data));
				if (UNLIKELY(!data)) {
					pthread_rwlock_unlock(&D->timer_lock);
					continue;
				}

				data->req_count = 1;
				data->hit_count = timer->hit_count;
				data->timer_value = timer->value;
				data->prev_add_request_id = i;
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
			if ((int)i != data->prev_add_request_id) {
				data->req_count++;
				data->prev_add_request_id = i;
			}
			pthread_rwlock_unlock(&D->timer_lock);
		}
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* tag report */
static inline pinba_tag_report *pinba_regenerate_tag_report(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue, ppvalue_script;
	pinba_tag_report *report;
	int dummy, k;
	pinba_timer_record *timer;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	struct pinba_tag_report_data *data;
	unsigned int i, j;
	int tag_found;
	pinba_word *word;

	if (share->index[0] == '\0') {
		pinba_get_tag_report_id(share->index, PINBA_TABLE_TAG_REPORT, share);
	}

	ppvalue = JudySLGet(D->tag_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pinba_tag *tag;

		/* no such report */
		ppvalue = JudySLGet(D->tag.name_index, (uint8_t *)share->params[0], NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			/* no such tag! */
			return NULL;
		}

		tag = (pinba_tag *)*ppvalue;

		report = (pinba_tag_report *)calloc(1, sizeof(pinba_tag_report));
		if (UNLIKELY(!report)) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_TAG_REPORT;
		report->time_interval = 1;
		report->last_requested = 0;
		report->results_cnt = 0;
		report->results = NULL;
		report->tag1_id = tag->id;
		pthread_rwlock_init(&report->lock, 0);
		
		memcpy_static(report->tag1, share->params[0], strlen(share->params[0]), dummy);

		pthread_rwlock_wrlock(&report->lock);
		
		ppvalue = JudySLIns(&D->tag_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}

		*ppvalue = report;
	} else {
		report = (pinba_tag_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	if (D->settings.tag_report_timeout == -1 || report->last_requested == 0) {
		/* really regenerate */
	} else {
		pthread_rwlock_unlock(&report->lock);
		return report;
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (!record->timers_cnt) {
			continue;
		}

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
			}
		}

		ppvalue_script = JudySLGet(report->results, (uint8_t *)record->data.script_name, NULL);

		if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
			ppvalue_script = JudySLIns(&report->results, (uint8_t *)record->data.script_name, NULL);
			if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
				continue;
			}
		}

		for (j = 0; j < record->timers_cnt; j++) {
			tag_found = 0;
			pthread_rwlock_rdlock(&D->timer_lock);
			timer = record_get_timer(&D->timer_pool, record, j);

			for (k = 0; k < timer->tag_num; k++) {
				if (report->tag1_id == timer->tag_ids[k]) {
					/* ok, timer has this tag */
					tag_found = 1;
					break;
				}
			}

			if (!tag_found) {
				/* tag not found in this timer */
				pthread_rwlock_unlock(&D->timer_lock);
				continue;
			}

			word = (pinba_word *)timer->tag_values[k];

			ppvalue = JudySLGet(*ppvalue_script, (uint8_t *)word->str, NULL);

			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {

				ppvalue = JudySLIns(ppvalue_script, (uint8_t *)word->str, NULL);
				if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
					pthread_rwlock_unlock(&D->timer_lock);
					continue;
				}

				data = (struct pinba_tag_report_data *)malloc(sizeof(struct pinba_tag_report_data));
				if (UNLIKELY(!data)) {
					pthread_rwlock_unlock(&D->timer_lock);
					continue;
				}

				data->req_count = 1;
				data->hit_count = timer->hit_count;
				data->timer_value = timer->value;
				data->prev_add_request_id = i;
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
			if ((int)i != data->prev_add_request_id) {
				data->req_count++;
				data->prev_add_request_id = i;
			}
			pthread_rwlock_unlock(&D->timer_lock);
		}
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* tag2 report */
static inline pinba_tag_report *pinba_regenerate_tag2_report(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue, ppvalue_script;
	pinba_tag_report *report;
	uint8_t index_val[PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1];
	int index_len, dummy, k;
	pinba_timer_record *timer;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	struct pinba_tag2_report_data *data;
	unsigned int i, j;
	int tag1_pos, tag2_pos;
	pinba_word *word1, *word2;

	if (share->index[0] == '\0') {
		pinba_get_tag_report_id(share->index, PINBA_TABLE_TAG2_REPORT, share);
	}

	ppvalue = JudySLGet(D->tag_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pinba_tag *tag1, *tag2;

		/* no such report */
		ppvalue = JudySLGet(D->tag.name_index, (uint8_t *)share->params[0], NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return NULL;
		}

		tag1 = (pinba_tag *)*ppvalue;

		ppvalue = JudySLGet(D->tag.name_index, (uint8_t *)share->params[1], NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return NULL;
		}

		tag2 = (pinba_tag *)*ppvalue;

		report = (pinba_tag_report *)calloc(1, sizeof(pinba_tag_report));
		if (UNLIKELY(!report)) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_TAG2_REPORT;
		report->time_interval = 1;
		report->last_requested = 0;
		report->results_cnt = 0;
		report->results = NULL;
		report->tag1_id = tag1->id;
		report->tag2_id = tag2->id;
		pthread_rwlock_init(&report->lock, 0);
	
		pthread_rwlock_wrlock(&report->lock);

		memcpy_static(report->tag1, share->params[0], strlen(share->params[0]), dummy);
		memcpy_static(report->tag2, share->params[1], strlen(share->params[1]), dummy);

		ppvalue = JudySLIns(&D->tag_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}

		*ppvalue = report;
	} else {
		report = (pinba_tag_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	if (D->settings.tag_report_timeout == -1 || report->last_requested == 0) {
		/* really regenerate */
	} else {
		pthread_rwlock_unlock(&report->lock);
		return report;
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (!record->timers_cnt) {
			continue;
		}

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
			}
		}

		ppvalue_script = JudySLGet(report->results, (uint8_t *)record->data.script_name, NULL);

		if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
			ppvalue_script = JudySLIns(&report->results, (uint8_t *)record->data.script_name, NULL);
			if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
				continue;
			}
		}

		for (j = 0; j < record->timers_cnt; j++) {
			tag1_pos = tag2_pos = -1;
			pthread_rwlock_rdlock(&D->timer_lock);
			timer = record_get_timer(&D->timer_pool, record, j);

			for (k = 0; k < timer->tag_num; k++) {
				if (report->tag1_id == timer->tag_ids[k]) {
					/* ok, timer has this tag */
					tag1_pos = k;
					continue;
				}
				if (report->tag2_id == timer->tag_ids[k]) {
					/* ok, timer has this tag */
					tag2_pos = k;
					continue;
				}
			}

			if (tag1_pos == -1 || tag2_pos == -1) {
				pthread_rwlock_unlock(&D->timer_lock);
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
					pthread_rwlock_unlock(&D->timer_lock);
					continue;
				}

				data = (struct pinba_tag2_report_data *)malloc(sizeof(struct pinba_tag2_report_data));
				if (UNLIKELY(!data)) {
					pthread_rwlock_unlock(&D->timer_lock);
					continue;
				}

				data->req_count = 1;
				data->hit_count = timer->hit_count;
				data->timer_value = timer->value;
				data->prev_add_request_id = i;
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
			if ((int)i != data->prev_add_request_id) {
				data->req_count++;
				data->prev_add_request_id = i;
			}
			pthread_rwlock_unlock(&D->timer_lock);
		}
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* tag report2 */
static inline pinba_tag_report *pinba_regenerate_tag_report2(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_tag_report *report;
	int dummy, k;
	pinba_timer_record *timer;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	struct pinba_tag_report2_data *data;
	unsigned int i, j;
	int tag_found, index_len;
	pinba_word *word;
	uint8_t index_val[PINBA_HOSTNAME_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1];

	if (share->index[0] == '\0') {
		pinba_get_tag_report_id(share->index, PINBA_TABLE_TAG_REPORT2, share);
	}

	ppvalue = JudySLGet(D->tag_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pinba_tag *tag;

		/* no such report */
		ppvalue = JudySLGet(D->tag.name_index, (uint8_t *)share->params[0], NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			/* no such tag! */
			return NULL;
		}

		tag = (pinba_tag *)*ppvalue;

		report = (pinba_tag_report *)calloc(1, sizeof(pinba_tag_report));
		if (UNLIKELY(!report)) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_TAG_REPORT2;
		report->time_interval = 1;
		report->last_requested = 0;
		report->results_cnt = 0;
		report->results = NULL;
		report->tag1_id = tag->id;
		pthread_rwlock_init(&report->lock, 0);

		memcpy_static(report->tag1, share->params[0], strlen(share->params[0]), dummy);

		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->tag_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}

		*ppvalue = report;
	} else {
		report = (pinba_tag_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	if (D->settings.tag_report_timeout == -1 || report->last_requested == 0) {
		/* really regenerate */
	} else {
		pthread_rwlock_unlock(&report->lock);
		return report;
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (!record->timers_cnt) {
			continue;
		}

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
			}
		}

		for (j = 0; j < record->timers_cnt; j++) {
			tag_found = 0;
			pthread_rwlock_rdlock(&D->timer_lock);
			timer = record_get_timer(&D->timer_pool, record, j);

			for (k = 0; k < timer->tag_num; k++) {
				if (report->tag1_id == timer->tag_ids[k]) {
					/* ok, timer has this tag */
					tag_found = 1;
					break;
				}
			}

			if (!tag_found) {
				/* tag not found in this timer */
				pthread_rwlock_unlock(&D->timer_lock);
				continue;
			}

			word = (pinba_word *)timer->tag_values[k];

			memcpy_static(index_val, record->data.hostname, record->data.hostname_len, index_len);
			index_val[index_len] = '|'; index_len++;
			memcat_static(index_val, index_len, record->data.server_name, record->data.server_name_len, index_len);
			index_val[index_len] = '|'; index_len++;
			memcat_static(index_val, index_len, record->data.script_name, record->data.script_name_len, index_len);
			index_val[index_len] = '|'; index_len++;
			memcat_static(index_val, index_len, word->str, word->len, index_len);

			ppvalue = JudySLGet(report->results, index_val, NULL);

			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {

				ppvalue = JudySLIns(&report->results, index_val, NULL);
				if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
					pthread_rwlock_unlock(&D->timer_lock);
					continue;
				}

				data = (struct pinba_tag_report2_data *)malloc(sizeof(struct pinba_tag_report2_data));
				if (UNLIKELY(!data)) {
					pthread_rwlock_unlock(&D->timer_lock);
					continue;
				}

				data->req_count = 1;
				data->hit_count = timer->hit_count;
				data->timer_value = timer->value;
				data->prev_add_request_id = i;
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
			if ((int)i != data->prev_add_request_id) {
				data->req_count++;
				data->prev_add_request_id = i;
			}
			pthread_rwlock_unlock(&D->timer_lock);
		}
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* tag2 report2 */
static inline pinba_tag_report *pinba_regenerate_tag2_report2(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_tag_report *report;
	uint8_t index_val[PINBA_HOSTNAME_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE];
	int index_len, dummy, k;
	pinba_timer_record *timer;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	struct pinba_tag2_report2_data *data;
	unsigned int i, j;
	int tag1_pos, tag2_pos;
	pinba_word *word1, *word2;

	if (share->index[0] == '\0') {
		pinba_get_tag_report_id(share->index, PINBA_TABLE_TAG2_REPORT2, share);
	}

	ppvalue = JudySLGet(D->tag_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pinba_tag *tag1, *tag2;

		/* no such report */
		ppvalue = JudySLGet(D->tag.name_index, (uint8_t *)share->params[0], NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return NULL;
		}

		tag1 = (pinba_tag *)*ppvalue;

		ppvalue = JudySLGet(D->tag.name_index, (uint8_t *)share->params[1], NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			return NULL;
		}

		tag2 = (pinba_tag *)*ppvalue;

		report = (pinba_tag_report *)calloc(1, sizeof(pinba_tag_report));
		if (UNLIKELY(!report)) {
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_TAG2_REPORT2;
		report->time_interval = 1;
		report->last_requested = 0;
		report->results_cnt = 0;
		report->results = NULL;
		report->tag1_id = tag1->id;
		report->tag2_id = tag2->id;
		pthread_rwlock_init(&report->lock, 0);

		pthread_rwlock_wrlock(&report->lock);

		memcpy_static(report->tag1, share->params[0], strlen(share->params[0]), dummy);
		memcpy_static(report->tag2, share->params[1], strlen(share->params[1]), dummy);

		ppvalue = JudySLIns(&D->tag_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			free(report);
			return NULL;
		}

		*ppvalue = report;
	} else {
		report = (pinba_tag_report *)*ppvalue;
		pthread_rwlock_wrlock(&report->lock);
	}

	if (D->settings.tag_report_timeout == -1 || report->last_requested == 0) {
		/* really regenerate */
	} else {
		pthread_rwlock_unlock(&report->lock);
		return report;
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (!record->timers_cnt) {
			continue;
		}

		if (report->std.flags & PINBA_REPORT_CONDITIONAL) {
			if (report->std.cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->std.cond.min_time) {
				continue;
			}
			if (report->std.cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->std.cond.max_time) {
				continue;
			}
		}

		for (j = 0; j < record->timers_cnt; j++) {
			tag1_pos = tag2_pos = -1;
			pthread_rwlock_rdlock(&D->timer_lock);
			timer = record_get_timer(&D->timer_pool, record, j);

			for (k = 0; k < timer->tag_num; k++) {
				if (report->tag1_id == timer->tag_ids[k]) {
					/* ok, timer has this tag */
					tag1_pos = k;
					continue;
				}
				if (report->tag2_id == timer->tag_ids[k]) {
					/* ok, timer has this tag */
					tag2_pos = k;
					continue;
				}
			}

			if (tag1_pos == -1 || tag2_pos == -1) {
				pthread_rwlock_unlock(&D->timer_lock);
				continue;
			}

			word1 = (pinba_word *)timer->tag_values[tag1_pos];
			word2 = (pinba_word *)timer->tag_values[tag2_pos];

			memcpy_static(index_val, record->data.hostname, record->data.hostname_len, index_len);
			index_val[index_len] = '|'; index_len++;
			memcat_static(index_val, index_len, record->data.server_name, record->data.server_name_len, index_len);
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
					pthread_rwlock_unlock(&D->timer_lock);
					continue;
				}

				data = (struct pinba_tag2_report2_data *)malloc(sizeof(struct pinba_tag2_report2_data));
				if (UNLIKELY(!data)) {
					pthread_rwlock_unlock(&D->timer_lock);
					continue;
				}

				data->req_count = 1;
				data->hit_count = timer->hit_count;
				data->timer_value = timer->value;
				data->prev_add_request_id = i;
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
			if ((int)i != data->prev_add_request_id) {
				data->req_count++;
				data->prev_add_request_id = i;
			}
			pthread_rwlock_unlock(&D->timer_lock);
		}
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */



/* </reports> }}} */

/* <share functions> {{{ */

static PINBA_SHARE *get_share(const char *table_name, TABLE *table) /* {{{ */
{
	PINBA_SHARE *share;
	uint length;
	char *tmp_name;
	char **params = NULL;
	int param_num = 0;
	char **cond_names = NULL;
	char **cond_values = NULL;
	int cond_num = 0;
	unsigned char type = PINBA_TABLE_UNKNOWN;

	pthread_mutex_lock(&pinba_mutex);
	length = (uint)strlen(table_name);

	if (!(share = (PINBA_SHARE*)hash_search(&pinba_open_tables, (unsigned char*) table_name, length))) {
		type = pinba_get_table_type(table);
		if (type == PINBA_TABLE_UNKNOWN) {
			pthread_mutex_unlock(&pinba_mutex);
			return NULL;
		}

		if (pinba_parse_params(table, &params, &param_num, &cond_names, &cond_values, &cond_num) < 0) {
			pthread_mutex_unlock(&pinba_mutex);
			return NULL;
		}

		if (!my_multi_malloc(MYF(MY_WME | MY_ZEROFILL), &share, sizeof(*share), &tmp_name, length+1, NullS)) {
			pthread_mutex_unlock(&pinba_mutex);
			return NULL;
		}

		share->use_count = 0;
		share->table_name_length = length;
		share->table_name = tmp_name;
		memcpy(share->table_name, table_name, length);
		share->table_name[length] = '\0';
		share->table_type = type;
		share->params = params;
		share->params_num = param_num;
		share->cond_names = cond_names;
		share->cond_values = cond_values;
		share->cond_num = cond_num;
		share->index[0] = '\0';

		if (my_hash_insert(&pinba_open_tables, (unsigned char*) share)) {
			goto error;
		}
		thr_lock_init(&share->lock);
	}
	share->use_count++;
	pthread_mutex_unlock(&pinba_mutex);

	return share;

error:
	pthread_mutex_unlock(&pinba_mutex);
	pinba_free((unsigned char *) share, MYF(0));

	return NULL;
}
/* }}} */

static int free_share(PINBA_SHARE *share) /* {{{ */
{
	pthread_mutex_lock(&pinba_mutex);
	if (!--share->use_count) {
		if (share->params_num > 0) {
			int i;

			for (i = 0; i < share->params_num; i++) {
				free(share->params[i]);
			}
			
			free(share->params);
			share->params = NULL;
			share->params_num = 0;
		}

		if (share->cond_num > 0) {
			int i;

			for (i = 0; i < share->cond_num; i++) {
				free(share->cond_names[i]);
				free(share->cond_values[i]);
			}
			
			free(share->cond_names);
			free(share->cond_values);
			share->cond_names = NULL;
			share->cond_values = NULL;
			share->cond_num = 0;
		}

		hash_delete(&pinba_open_tables, (unsigned char*) share);
		thr_lock_delete(&share->lock);
		pinba_free((unsigned char *) share, MYF(0));
	}
	pthread_mutex_unlock(&pinba_mutex);

	return 0;
}
/* }}} */

static handler* pinba_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root) /* {{{ */
{
	return new (mem_root) ha_pinba(hton, table);
}
/* }}} */

ha_pinba::ha_pinba(handlerton *hton, TABLE_SHARE *table_arg) /* {{{ */
:handler(hton, table_arg)
{
	rec_buff = NULL;
	alloced_rec_buff_length = 0;
}
/* }}} */

/* {{{ file extensions (none) */
static const char *ha_pinba_exts[] = {
	NullS
};

const char **ha_pinba::bas_ext() const
{
	return ha_pinba_exts;
}
/* }}} */

int ha_pinba::open(const char *name, int mode, uint test_if_locked) /* {{{ */
{
	DBUG_ENTER("ha_pinba::open");
	if (!(share = get_share(name, table))) {
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	thr_lock_data_init(&share->lock, &lock, NULL);
	DBUG_RETURN(0);
}
/* }}} */

int ha_pinba::close(void) /* {{{ */
{
	DBUG_ENTER("ha_pinba::close");
	DBUG_RETURN(free_share(share));
}
/* }}} */

/* </share functions> }}} */

/* <index functions> {{{ */

int ha_pinba::index_init(uint keynr, bool sorted) /* {{{ */
{
	DBUG_ENTER("ha_pinba::index_init");
	active_index = keynr;

	if (active_index < 0 || active_index >= PINBA_MAX_KEYS) {
		DBUG_RETURN(HA_ERR_WRONG_INDEX);
	}

	this_index[active_index].position = 0;
	DBUG_RETURN(0);
}
/* }}} */

int ha_pinba::index_read(unsigned char *buf, const unsigned char *key, uint key_len, enum ha_rkey_function find_flag) /* {{{ */
{
	DBUG_ENTER("ha_pinba::index_read");
	int ret;

	if (active_index < 0 || active_index >= PINBA_MAX_KEYS) {
		DBUG_RETURN(HA_ERR_WRONG_INDEX);
	}

	this_index[active_index].subindex.val = NULL;
	this_index[active_index].position = 0;
	ret = read_row_by_key(buf, active_index, key, key_len, 1);
	if (!ret) {
		this_index[active_index].position++;
	}
	DBUG_RETURN(ret);
}
/* }}} */

int ha_pinba::index_next(unsigned char *buf) /* {{{ */
{
	DBUG_ENTER("ha_pinba::index_next");
	int ret;

	if (active_index < 0 || active_index >= PINBA_MAX_KEYS) {
		DBUG_RETURN(HA_ERR_WRONG_INDEX);
	}

	ret = read_next_row(buf, active_index, true);
	if (!ret) {
		this_index[active_index].position++;
	}
	DBUG_RETURN(ret);
}
/* }}} */

int ha_pinba::index_prev(unsigned char *buf) /* {{{ */
{
	DBUG_ENTER("ha_pinba::index_prev");
	int ret;

	if (active_index < 0 || active_index >= PINBA_MAX_KEYS) {
		DBUG_RETURN(HA_ERR_WRONG_INDEX);
	}

	ret = read_next_row(buf, active_index, true);
	if (!ret) {
		this_index[active_index].position--;
	}
	DBUG_RETURN(ret);
}
/* }}} */

int ha_pinba::index_first(unsigned char *buf) /* {{{ */
{
	DBUG_ENTER("ha_pinba::index_first");
	int ret;

	if (active_index < 0 || active_index >= PINBA_MAX_KEYS) {
		DBUG_RETURN(HA_ERR_WRONG_INDEX);
	}

	this_index[active_index].position = 0;
	ret = read_index_first(buf, active_index);
	if (!ret) {
		this_index[active_index].position++;
	}
	DBUG_RETURN(ret);
}
/* }}} */

/* </index  functions> }}} */

/* <table scan functions> {{{ */

int ha_pinba::rnd_init(bool scan) /* {{{ */
{
	int i, ret = 0;
	pinba_tag_report *report;
	pinba_report *base_report;

	DBUG_ENTER("ha_pinba::rnd_init");

	pthread_rwlock_rdlock(&D->collector_lock);
	for (i = 0; i < PINBA_MAX_KEYS; i++) {
		memset(&this_index[i], 0, sizeof(pinba_index_st));
	}

	switch (share->table_type) {
		case PINBA_TABLE_REQUEST:
		case PINBA_TABLE_TIMER:
		case PINBA_TABLE_TIMERTAG:
			this_index[0].ival = -1;
			break;
	}

	pthread_rwlock_unlock(&D->collector_lock);
	DBUG_RETURN(0);
}
/* }}} */

int ha_pinba::rnd_end() /* {{{ */
{
	int ret = 0;
	pinba_tag_report *report;
	pinba_report *base_report;

	DBUG_ENTER("ha_pinba::rnd_end");

	/* Theoretically the number of records in the report may have grown
	   when we were reading it, so we won't reach the end of the array and
	   the index value may leak.
	   Hence this check. */
	switch (share->table_type) {
		case PINBA_TABLE_REPORT1:
		case PINBA_TABLE_REPORT2:
		case PINBA_TABLE_REPORT3:
		case PINBA_TABLE_REPORT4:
		case PINBA_TABLE_REPORT5:
		case PINBA_TABLE_REPORT6:
		case PINBA_TABLE_REPORT7:
		case PINBA_TABLE_REPORT8:
		case PINBA_TABLE_REPORT9:
		case PINBA_TABLE_REPORT10:
		case PINBA_TABLE_REPORT11:
		case PINBA_TABLE_REPORT12:
		case PINBA_TABLE_TAG_INFO:
		case PINBA_TABLE_TAG2_INFO:
		case PINBA_TABLE_TAG_REPORT:
		case PINBA_TABLE_TAG2_REPORT:
		case PINBA_TABLE_TAG_REPORT2:
		case PINBA_TABLE_TAG2_REPORT2:
			if (this_index[0].str.val != NULL) {
				free(this_index[0].str.val);
				this_index[0].str.val = NULL;
			}
			if (this_index[0].subindex.val != NULL) {
				free(this_index[0].subindex.val);
				this_index[0].subindex.val = NULL;
			}
			break;
		default:
			break;
	}

	DBUG_RETURN(0);
}
/* }}} */

int ha_pinba::rnd_next(unsigned char *buf) /* {{{ */
{
	int ret;

	DBUG_ENTER("ha_pinba::rnd_next");

	ret = read_next_row(buf, 0, false);
	DBUG_RETURN(ret);
}
/* }}} */

int ha_pinba::rnd_pos(unsigned char * buf, unsigned char *pos) /* {{{ */
{
	int ret;
	unsigned int key_length;
	DBUG_ENTER("ha_pinba::rnd_pos");

	if (active_index < 0 || active_index >= PINBA_MAX_KEYS) {
		DBUG_RETURN(HA_ERR_WRONG_INDEX);
	}

	memcpy(&key_length, pos, sizeof(unsigned int));
	ret = read_row_by_key(buf, 0, pos + sizeof(unsigned int), key_length, 1);
	if (!ret) {
		this_index[active_index].position++;
	}
	DBUG_RETURN(ret); 
}
/* }}} */

/* </table scan functions> }}} */

/* read wrappers for all table types */
int ha_pinba::read_index_first(unsigned char *buf, uint active_index) /* {{{ */
{
	DBUG_ENTER("ha_pinba::read_index_first");
	int ret = HA_ERR_INTERNAL_ERROR;
	Word_t index_value = 0;
	pinba_pool *p = &D->request_pool;
	pinba_pool *timer_pool = &D->timer_pool;

	if (active_index < 0 || active_index >= PINBA_MAX_KEYS) {
		DBUG_RETURN(HA_ERR_WRONG_INDEX);
	}

	switch(share->table_type) {
		case PINBA_TABLE_REQUEST:
			if (active_index > 0) {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}

			this_index[0].ival = p->out;
			this_index[0].position = p->out;

			ret = requests_fetch_row(buf, this_index[0].ival, &(this_index[0].position));
			this_index[0].ival = this_index[0].position;
			break;
		case PINBA_TABLE_TIMER:
			if (active_index == 0) {

				this_index[active_index].ival = timer_pool->out;
				this_index[active_index].position = 0;

				ret = timers_fetch_row(buf, this_index[active_index].ival, &(this_index[active_index].ival), 1);
			} else if (active_index == 1) {
				this_index[active_index].ival = p->out;
				this_index[active_index].position = 0;
				ret = timers_fetch_row_by_request_id(buf, this_index[active_index].ival, &(this_index[active_index].ival));
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		case PINBA_TABLE_TAG:
			if (active_index == 0) {
				PPvoid_t ppvalue;

				ppvalue = JudyLFirst(D->tag.table, &index_value, NULL);
				if (!ppvalue) {
					ret = HA_ERR_END_OF_FILE;
					goto failure;
				}

				this_index[active_index].ival = index_value;
				this_index[active_index].position = 0;

				ret = tags_fetch_row(buf, this_index[active_index].ival, &(this_index[active_index].ival));
			} else if (active_index == 1) {
				PPvoid_t ppvalue;
				char name[PINBA_MAX_LINE_LEN] = {0};

				ppvalue = JudySLFirst(D->tag.name_index, (uint8_t *)name, NULL);
				if (!ppvalue) {
					ret = HA_ERR_END_OF_FILE;
					goto failure;
				}

				this_index[active_index].str.len = strlen(name);
				this_index[active_index].str.val = (unsigned char *)strdup(name);
				ret = tags_fetch_row_by_name(buf, this_index[active_index].str.val, this_index[active_index].str.len);
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		case PINBA_TABLE_TIMERTAG:
			if (active_index == 0) {
				this_index[active_index].ival = timer_pool->out;
				this_index[active_index].position = 0;
				ret = tag_values_fetch_by_timer_id(buf);
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		case PINBA_TABLE_TAG_REPORT:
			if (active_index == 0) {
				uint8_t index[PINBA_SCRIPT_NAME_SIZE + 1] = {0};
				PPvoid_t ppvalue;
				pinba_tag_report *report;
				
				pthread_rwlock_rdlock(&D->collector_lock);
				pthread_rwlock_wrlock(&D->tag_reports_lock);
				pthread_rwlock_rdlock(&D->timer_lock);
				report = pinba_get_tag_report(PINBA_TABLE_TAG_REPORT, share);
		
				if (!report) {
					report = pinba_regenerate_tag_report(share);
				}

				if (this_index[active_index].str.val) {
					free(this_index[active_index].str.val);
					this_index[active_index].str.val = NULL;
					this_index[active_index].str.len = 0;
				}

				if (this_index[active_index].subindex.val) {
					free(this_index[active_index].subindex.val);
					this_index[active_index].subindex.val = NULL;
				}

				if (report) {
					pthread_rwlock_rdlock(&report->lock);

					ppvalue = JudySLFirst(report->results, index, NULL);
					if (ppvalue) {
						this_index[active_index].str.len = strlen((const char *)index);
						this_index[active_index].str.val = (unsigned char *)strdup((const char *)index);
						ret = tag_report_fetch_row_by_script(buf, this_index[active_index].str.val, this_index[active_index].str.len);
					} else {
						ret = HA_ERR_END_OF_FILE;
					}

					pthread_rwlock_unlock(&report->lock);
				} else {
					ret = HA_ERR_END_OF_FILE;
				}

				pthread_rwlock_unlock(&D->timer_lock);
				pthread_rwlock_unlock(&D->tag_reports_lock);
				pthread_rwlock_unlock(&D->collector_lock);
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		case PINBA_TABLE_TAG2_REPORT:
			if (active_index == 0) {
				uint8_t index[PINBA_SCRIPT_NAME_SIZE + 1] = {0};
				PPvoid_t ppvalue;
				pinba_tag_report *report;
				
				pthread_rwlock_rdlock(&D->collector_lock);
				pthread_rwlock_wrlock(&D->tag_reports_lock);
				pthread_rwlock_rdlock(&D->timer_lock);
				report = pinba_get_tag_report(PINBA_TABLE_TAG2_REPORT, share);
		
				if (!report) {
					report = pinba_regenerate_tag_report(share);
				}

				if (this_index[active_index].str.val) {
					free(this_index[active_index].str.val);
					this_index[active_index].str.val = NULL;
					this_index[active_index].str.len = 0;
				}

				if (this_index[active_index].subindex.val) {
					free(this_index[active_index].subindex.val);
					this_index[active_index].subindex.val = NULL;
				}

				if (report) {
					pthread_rwlock_rdlock(&report->lock);

					ppvalue = JudySLFirst(report->results, index, NULL);
					if (ppvalue) {
						this_index[active_index].str.len = strlen((const char *)index);
						this_index[active_index].str.val = (unsigned char *)strdup((const char *)index);
						ret = tag2_report_fetch_row_by_script(buf, this_index[active_index].str.val, this_index[active_index].str.len);
					} else {
						ret = HA_ERR_END_OF_FILE;
					}

					pthread_rwlock_unlock(&report->lock);
				} else {
					ret = HA_ERR_END_OF_FILE;
				}

				pthread_rwlock_unlock(&D->timer_lock);
				pthread_rwlock_unlock(&D->tag_reports_lock);
				pthread_rwlock_unlock(&D->collector_lock);
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		default:
			ret = HA_ERR_INTERNAL_ERROR;
			goto failure;
	}

failure:
	table->status = ret ? STATUS_NOT_FOUND : 0;
	DBUG_RETURN(ret);
}
/* }}} */

int ha_pinba::read_row_by_key(unsigned char *buf, uint active_index, const unsigned char *key, uint key_len, int exact) /* {{{ */
{
	DBUG_ENTER("ha_pinba::read_row_by_key");
	int ret = HA_ERR_INTERNAL_ERROR;

	if (active_index < 0 || active_index >= PINBA_MAX_KEYS) {
		DBUG_RETURN(HA_ERR_WRONG_INDEX);
	}

	switch(share->table_type) {
		case PINBA_TABLE_REQUEST:
			if (active_index > 0) {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}

			memset(&(this_index[active_index].ival), 0, sizeof(this_index[active_index].ival));
			memcpy(&(this_index[active_index].ival), key, key_len);
			ret = requests_fetch_row(buf, this_index[active_index].ival, NULL);
			break;
		case PINBA_TABLE_TIMER:
			if (active_index == 0) {
				memset(&(this_index[active_index].ival), 0, sizeof(this_index[active_index].ival));
				memcpy(&(this_index[active_index].ival), key, key_len);
				ret = timers_fetch_row(buf, this_index[active_index].ival, NULL, exact);
			} else if (active_index == 1) {
				memset(&(this_index[active_index].ival), 0, sizeof(this_index[active_index].ival));
				memcpy(&(this_index[active_index].ival), key, key_len);
				ret = timers_fetch_row_by_request_id(buf, this_index[active_index].ival, NULL);
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		case PINBA_TABLE_TAG:
			if (active_index == 0) {
				memset(&(this_index[active_index].ival), 0, sizeof(this_index[active_index].ival));
				memcpy(&(this_index[active_index].ival), key, key_len);
				ret = tags_fetch_row(buf, this_index[active_index].ival, NULL);
			} else if (active_index == 1) {
				memset(&(this_index[active_index]), 0, sizeof(this_index[active_index]));
				netstr_to_key(key, &this_index[active_index]);
				ret = tags_fetch_row_by_name(buf, this_index[active_index].str.val, this_index[active_index].str.len);
				free(this_index[active_index].str.val);
				this_index[active_index].str.val = NULL;
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		case PINBA_TABLE_TIMERTAG:
			if (active_index == 0) {
				memset(&(this_index[active_index].ival), 0, sizeof(this_index[active_index].ival));
				memcpy(&(this_index[active_index].ival), key, key_len);
				ret = tag_values_fetch_by_timer_id(buf);
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		case PINBA_TABLE_TAG_REPORT:
			if (active_index == 0) {
				memset(&(this_index[active_index].ival), 0, sizeof(this_index[active_index].ival));
				netstr_to_key(key, &this_index[active_index]);
				ret = tag_report_fetch_row_by_script(buf, this_index[active_index].str.val, this_index[active_index].str.len);
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		case PINBA_TABLE_TAG2_REPORT:
			if (active_index == 0) {
				memset(&(this_index[active_index].ival), 0, sizeof(this_index[active_index].ival));
				netstr_to_key(key, &this_index[active_index]);
				ret = tag2_report_fetch_row_by_script(buf, this_index[active_index].str.val, this_index[active_index].str.len);
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		default:
			ret = HA_ERR_INTERNAL_ERROR;
			goto failure;
	}

failure:
	table->status = ret ? STATUS_NOT_FOUND : 0;
	DBUG_RETURN(ret);
}
/* }}} */

int ha_pinba::read_next_row(unsigned char *buf, uint active_index, bool by_key) /* {{{ */
{
	DBUG_ENTER("ha_pinba::read_next_row");
	int ret = HA_ERR_INTERNAL_ERROR;

	if (active_index < 0 || active_index >= PINBA_MAX_KEYS) {
		DBUG_RETURN(HA_ERR_WRONG_INDEX);
	}

	switch(share->table_type) {
		case PINBA_TABLE_REQUEST:
			if (active_index > 0) {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}

			ret = requests_fetch_row(buf, this_index[active_index].ival, &(this_index[0].position));
			this_index[active_index].ival = this_index[0].position;
			break;
		case PINBA_TABLE_TIMER:
			if (active_index == 0) {
				ret = timers_fetch_row(buf, this_index[active_index].ival, &(this_index[active_index].ival), 0);
			} else if (active_index == 1) {
				ret = timers_fetch_row_by_request_id(buf, this_index[active_index].ival, &(this_index[active_index].ival));
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		case PINBA_TABLE_TAG:
			if (active_index == 0) {
				ret = tags_fetch_row(buf, this_index[active_index].ival, &(this_index[0].position));
				this_index[active_index].ival = this_index[0].position;
			} else if (active_index == 1) {
				PPvoid_t ppvalue;
				char name[PINBA_MAX_LINE_LEN] = {0};
				pinba_tag *tag;

				if (this_index[active_index].str.val) {
					memcpy(name, this_index[active_index].str.val, this_index[active_index].str.len);
				}

				ppvalue = JudySLNext(D->tag.name_index, (uint8_t *)name, NULL);
				if (!ppvalue) {
					ret = HA_ERR_END_OF_FILE;
					free(this_index[active_index].str.val);
					this_index[active_index].str.val = NULL;
					goto failure;
				}
				tag = (pinba_tag *)*ppvalue;

				this_index[active_index].str.len = tag->name_len;
				if (this_index[active_index].str.val) {
					free(this_index[active_index].str.val);
					this_index[active_index].str.val = NULL;
				}
				this_index[active_index].str.val = (unsigned char *)strndup(name, tag->name_len);
				ret = tags_fetch_row_by_name(buf, this_index[active_index].str.val, this_index[active_index].str.len);
				if (ret) {
					free(this_index[active_index].str.val);
					this_index[active_index].str.val = NULL;
				}
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		case PINBA_TABLE_TIMERTAG:
			ret = tag_values_fetch_next(buf, &(this_index[0].ival), &(this_index[0].position));
			if (!by_key) {
				this_index[0].position++;
			}
			break;
		case PINBA_TABLE_REPORT_INFO:
			ret = info_fetch_row(buf);
			break;
		case PINBA_TABLE_REPORT1:
			ret = report1_fetch_row(buf);
			break;
		case PINBA_TABLE_REPORT2:
			ret = report2_fetch_row(buf);
			break;
		case PINBA_TABLE_REPORT3:
			ret = report3_fetch_row(buf);
			break;
		case PINBA_TABLE_REPORT4:
			ret = report4_fetch_row(buf);
			break;
		case PINBA_TABLE_REPORT5:
			ret = report5_fetch_row(buf);
			break;
		case PINBA_TABLE_REPORT6:
			ret = report6_fetch_row(buf);
			break;
		case PINBA_TABLE_REPORT7:
			ret = report7_fetch_row(buf);
			break;
		case PINBA_TABLE_REPORT8:
			ret = report8_fetch_row(buf);
			break;
		case PINBA_TABLE_REPORT9:
			ret = report9_fetch_row(buf);
			break;
		case PINBA_TABLE_REPORT10:
			ret = report10_fetch_row(buf);
			break;
		case PINBA_TABLE_REPORT11:
			ret = report11_fetch_row(buf);
			break;
		case PINBA_TABLE_REPORT12:
			ret = report12_fetch_row(buf);
			break;
		case PINBA_TABLE_TAG_INFO:
			pthread_rwlock_rdlock(&D->collector_lock);
			pthread_rwlock_rdlock(&D->tag_reports_lock);
			ret = tag_info_fetch_row(buf);
			pthread_rwlock_unlock(&D->tag_reports_lock);
			pthread_rwlock_unlock(&D->collector_lock);
			break;
		case PINBA_TABLE_TAG2_INFO:
			pthread_rwlock_rdlock(&D->collector_lock);
			pthread_rwlock_rdlock(&D->tag_reports_lock);
			ret = tag2_info_fetch_row(buf);
			pthread_rwlock_unlock(&D->tag_reports_lock);
			pthread_rwlock_unlock(&D->collector_lock);
			break;
		case PINBA_TABLE_TAG_REPORT:
			pthread_rwlock_rdlock(&D->collector_lock);
			pthread_rwlock_rdlock(&D->tag_reports_lock);
			if (by_key) {
				pinba_tag_report *report;

				report = pinba_get_tag_report(PINBA_TABLE_TAG_REPORT, share);
				if (!report) {
					DBUG_RETURN(HA_ERR_END_OF_FILE);
				}

				ret = tag_report_fetch_row_by_script(buf, this_index[active_index].str.val, this_index[active_index].str.len);
				if (ret) {
					free(this_index[active_index].str.val);
					this_index[active_index].str.val = NULL;
				}
			} else {
				ret = tag_report_fetch_row(buf);
			}
			pthread_rwlock_unlock(&D->tag_reports_lock);
			pthread_rwlock_unlock(&D->collector_lock);
			break;
		case PINBA_TABLE_TAG2_REPORT:
			pthread_rwlock_rdlock(&D->collector_lock);
			pthread_rwlock_rdlock(&D->tag_reports_lock);
			if (by_key) {
				pinba_tag_report *report;

				report = pinba_get_tag_report(PINBA_TABLE_TAG2_REPORT, share);
				if (!report) {
					DBUG_RETURN(HA_ERR_END_OF_FILE);
				}

				ret = tag2_report_fetch_row_by_script(buf, this_index[active_index].str.val, this_index[active_index].str.len);
				if (ret) {
					free(this_index[active_index].str.val);
					this_index[active_index].str.val = NULL;
				}
			} else {
				ret = tag2_report_fetch_row(buf);
			}
			pthread_rwlock_unlock(&D->tag_reports_lock);
			pthread_rwlock_unlock(&D->collector_lock);
			break;
		case PINBA_TABLE_TAG_REPORT2:
			pthread_rwlock_rdlock(&D->collector_lock);
			pthread_rwlock_rdlock(&D->tag_reports_lock);
			ret = tag_report2_fetch_row(buf);
			pthread_rwlock_unlock(&D->tag_reports_lock);
			pthread_rwlock_unlock(&D->collector_lock);
			break;
		case PINBA_TABLE_TAG2_REPORT2:
			pthread_rwlock_rdlock(&D->collector_lock);
			pthread_rwlock_rdlock(&D->tag_reports_lock);
			ret = tag2_report2_fetch_row(buf);
			pthread_rwlock_unlock(&D->tag_reports_lock);
			pthread_rwlock_unlock(&D->collector_lock);
			break;
		default:
			/* unsupported table type */
			ret = HA_ERR_INTERNAL_ERROR;
			goto failure;
	}

	if (ret == HA_ERR_KEY_NOT_FOUND) {
		ret = HA_ERR_END_OF_FILE;
	}

failure:
	table->status = ret ? STATUS_NOT_FOUND : 0;
	DBUG_RETURN(ret);
}
/* }}} */

/* <fetchers> {{{ */

inline int ha_pinba::requests_fetch_row(unsigned char *buf, size_t index, size_t *new_index) /* {{{ */
{
	Field **field;
	pinba_pool *p = &D->request_pool;
	my_bitmap_map *old_map;
	pinba_stats_record record;

	DBUG_ENTER("ha_pinba::requests_fetch_row");

	pthread_rwlock_rdlock(&D->collector_lock);

	if (index == (size_t)-1) {
		index = p->out;
	}

	if (new_index) {
		*new_index = index;
	}

	if (index == (p->size - 1)) {
		index = 0;
	}

	if (index == p->in || index < 0 || index >= (unsigned int)p->size || p->in == p->out) {
		pthread_rwlock_unlock(&D->collector_lock);
		DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
	}

	record = REQ_POOL(p)[index];

	if (record.time.tv_sec == 0) { /* invalid record */
		pthread_rwlock_unlock(&D->collector_lock);
		DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
	}

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* index */
					(*field)->set_notnull();
					(*field)->store((long)index);
					break;
				case 1: /* hostname */
					(*field)->set_notnull();
					(*field)->store(record.data.hostname, strlen(record.data.hostname), &my_charset_bin);
					break;
				case 2: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)record.data.req_count);
					break;
				case 3: /* server_name */
					(*field)->set_notnull();
					(*field)->store(record.data.server_name, strlen(record.data.server_name), &my_charset_bin);
					break;
				case 4: /* script_name */
					(*field)->set_notnull();
					(*field)->store(record.data.script_name, strlen(record.data.script_name), &my_charset_bin);
					break;
				case 5: /* doc_size */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)(record.data.doc_size), 1000));
					break;
				case 6: /* mem_peak_usage */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)(record.data.mem_peak_usage), 1000));
					break;
				case 7: /* req_time */
					(*field)->set_notnull();
					(*field)->store(pinba_round(timeval_to_float(record.data.req_time), 1000));
					break;
				case 8: /* ru_utime */
					(*field)->set_notnull();
					(*field)->store(pinba_round(timeval_to_float(record.data.ru_utime), 10000));
					break;
				case 9: /* ru_stime */
					(*field)->set_notnull();
					(*field)->store(pinba_round(timeval_to_float(record.data.ru_stime), 10000));
					break;
				case 10: /* timers_cnt */
					(*field)->set_notnull();
					(*field)->store((long)record.timers_cnt);
					break;
				case 11: /* status */
					(*field)->set_notnull();
					(*field)->store((long)record.data.status);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);

	if (new_index) {
		*new_index = index + 1;
	}
	pthread_rwlock_unlock(&D->collector_lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::timers_fetch_row(unsigned char *buf, size_t index, size_t *new_index, int exact) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	pinba_pool *p = &D->request_pool;
	pinba_pool *timer_pool = &D->timer_pool;
	pinba_timer_record *timer;
	pinba_stats_record record;

	DBUG_ENTER("ha_pinba::timers_fetch_row");

	pthread_rwlock_rdlock(&D->collector_lock);

	if (index == (size_t)-1) {
		index = timer_pool->out;
	}

	if (new_index) {
		*new_index = index;
	}

try_next:

	if (index == (timer_pool->size - 1)) {
		index = 0;
	}

	if (index == timer_pool->in || index < 0 || index >= (unsigned int)timer_pool->size || timer_pool->in == timer_pool->out) {
		pthread_rwlock_unlock(&D->collector_lock);
		DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
	}

	timer = TIMER_POOL(timer_pool) + index;

	if (!exact && REQ_POOL(p)[timer->request_id].time.tv_sec == 0) {
		index++;
		goto try_next;
	}

	record = REQ_POOL(p)[timer->request_id];
	if (timer->num_in_request >= record.timers_cnt) {
		if (exact) {
			pthread_rwlock_unlock(&D->collector_lock);
			DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
		} else {
			goto try_next;
		}
	}

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* index */
					(*field)->set_notnull();
					(*field)->store((long)index);
					break;
				case 1: /* request_id */
					(*field)->set_notnull();
					(*field)->store((long)timer->request_id);
					break;
				case 2: /* hit_count */
					(*field)->set_notnull();
					(*field)->store(timer->hit_count);
					break;
				case 3: /* value */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(timer->value));
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);

	if (new_index) {
		*new_index = index + 1;
	}
	pthread_rwlock_unlock(&D->collector_lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::timers_fetch_row_by_request_id(unsigned char *buf, size_t index, size_t *new_index) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	pinba_pool *p = &D->request_pool;
	pinba_timer_record *timer;
	pinba_stats_record *record;

	DBUG_ENTER("ha_pinba::timers_fetch_row_by_request_id");

	pthread_rwlock_rdlock(&D->collector_lock);

	if (new_index) {
		*new_index = index;
	}

	if (index == p->in || index < 0 || index >= (unsigned int)D->settings.request_pool_size || p->in == p->out) {
		pthread_rwlock_unlock(&D->collector_lock);
		DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
	}

	record = REQ_POOL(p) + index;

	if (this_index[active_index].position >= record->timers_cnt || this_index[active_index].position < 0) {
		pthread_rwlock_unlock(&D->collector_lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	timer = record_get_timer(&D->timer_pool, record, this_index[active_index].position);
	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* index */
					(*field)->set_notnull();
					(*field)->store((long)timer->index);
					break;
				case 1: /* request_id */
					(*field)->set_notnull();
					(*field)->store((long)index);
					break;
				case 2: /* hit_count */
					(*field)->set_notnull();
					(*field)->store(timer->hit_count);
					break;
				case 3: /* value */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(timer->value));
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);

	/* XXX this smells funny */
	if (new_index && (size_t)this_index[active_index].position == (size_t)(record->timers_cnt - 1)) {
		*new_index = index + 1;
		this_index[active_index].position = -1;
	}
	pthread_rwlock_unlock(&D->collector_lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::tags_fetch_row(unsigned char *buf, size_t index, size_t *new_index) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	pinba_tag *tag;

	DBUG_ENTER("ha_pinba::tags_fetch_row");

	pthread_rwlock_rdlock(&D->collector_lock);

	if (new_index) {
		*new_index = index;
	}

	tag = pinba_tag_get_by_id(index);
	if (!tag) {
		pthread_rwlock_unlock(&D->collector_lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}
	
	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* id */
					(*field)->set_notnull();
					(*field)->store((long)index);
					break;
				case 1: /* name */
					(*field)->set_notnull();
					(*field)->store(tag->name, tag->name_len, &my_charset_bin);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);

	if (new_index) {
		*new_index = index + 1;
	}
	pthread_rwlock_unlock(&D->collector_lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::tags_fetch_row_by_name(unsigned char* buf, const unsigned char *name, uint name_len) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	pinba_tag *tag;

	DBUG_ENTER("ha_pinba::tags_fetch_row_by_name");

	pthread_rwlock_rdlock(&D->collector_lock);

	tag = pinba_tag_get_by_name(name);
	if (!tag) {
		pthread_rwlock_unlock(&D->collector_lock);
		DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
	}
	
	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* id */
					(*field)->set_notnull();
					(*field)->store((long)tag->id);
					break;
				case 1: /* name */
					(*field)->set_notnull();
					(*field)->store(tag->name, tag->name_len, &my_charset_bin);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);

	pthread_rwlock_unlock(&D->collector_lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::tag_values_fetch_next(unsigned char *buf, size_t *index, size_t *position) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	pinba_pool *timer_pool = &D->timer_pool;
	pinba_pool *p = &D->request_pool;
	pinba_timer_record *timer;
	pinba_stats_record *record;

	DBUG_ENTER("ha_pinba::tag_values_fetch_row");

	pthread_rwlock_rdlock(&D->collector_lock);

retry_next:

	if (*index == (size_t)-1) {
		*index = timer_pool->out;
	}

	if (*index == (timer_pool->size - 1)) {
		*index = 0;
	}

	if (*index == timer_pool->in || *index < 0 || *index >= (unsigned int)timer_pool->size || timer_pool->in == timer_pool->out) {
		pthread_rwlock_unlock(&D->collector_lock);
		DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
	}

	timer = TIMER_POOL(timer_pool) + *index;

	record = REQ_POOL(p) + timer->request_id;

	/* XXX */
	if (timer->num_in_request >= record->timers_cnt) {
		(*position) = 0;
		(*index)++;
		goto retry_next;
	}

	if (*position >= timer->tag_num) {
		(*position) = 0;
		(*index)++;
		goto retry_next;
	}

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* timer_id */
					(*field)->set_notnull();
					(*field)->store((long)timer->index);
					break;
				case 1: /* tad_id */
					(*field)->set_notnull();
					(*field)->store((long)timer->tag_ids[*position]);
					break;
				case 2: /* name */
					(*field)->set_notnull();
					(*field)->store(timer->tag_values[*position]->str, timer->tag_values[*position]->len, &my_charset_bin);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);

	pthread_rwlock_unlock(&D->collector_lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::tag_values_fetch_by_timer_id(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	pinba_pool *timer_pool = &D->timer_pool;
	pinba_pool *p = &D->request_pool;
	pinba_timer_record *timer;
	pinba_stats_record *record;

	DBUG_ENTER("ha_pinba::tag_values_fetch_by_timer_id");

	pthread_rwlock_rdlock(&D->collector_lock);

	if (this_index[0].ival == (timer_pool->size - 1)) {
		this_index[0].ival = 0;
	}

	if (this_index[0].ival == timer_pool->in || this_index[0].ival < 0 || this_index[0].ival >= (unsigned int)timer_pool->size || timer_pool->in == timer_pool->out) {
		pthread_rwlock_unlock(&D->collector_lock);
		DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
	}

	timer = TIMER_POOL(timer_pool) + this_index[0].ival;

	if (timer->tag_num == 0) {
		pthread_rwlock_unlock(&D->collector_lock);
		DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
	}

	record = REQ_POOL(p) + timer->request_id;

	if (timer->num_in_request >= record->timers_cnt) {
		pthread_rwlock_unlock(&D->collector_lock);
		DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
	}

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* timer_id */
					(*field)->set_notnull();
					(*field)->store((long)timer->index);
					break;
				case 1: /* tag_id */
					(*field)->set_notnull();
					(*field)->store((long)timer->tag_ids[this_index[0].position]);
					break;
				case 2: /* name */
					(*field)->set_notnull();
					(*field)->store(timer->tag_values[this_index[0].position]->str, timer->tag_values[this_index[0].position]->len, &my_charset_bin);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);

	pthread_rwlock_unlock(&D->collector_lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::report1_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_report1_data *data;
	PPvoid_t ppvalue;
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	pinba_report *report;
	
	DBUG_ENTER("ha_pinba::report1_fetch_row");

	report = pinba_get_report(PINBA_TABLE_REPORT1, share);

	if (!report) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	if (this_index[0].position == 0 || this_index[0].str.val == NULL) {
		pthread_rwlock_rdlock(&report->lock);
		ppvalue = JudySLFirst(report->results, index, NULL);
	} else {
		pthread_rwlock_rdlock(&report->lock);

		strcpy((char *)index, (char *)this_index[0].str.val);
		ppvalue = JudySLNext(report->results, index, NULL);
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].str.val = (unsigned char *)strdup((char *)index);
	this_index[0].position++;

	data = (struct pinba_report1_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 1: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 2: /* req_time_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->req_time_total));
					break;
				case 3: /* req_time_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->req_time_total)/timeval_to_float(report->time_total));
					break;
				case 4: /* req_time_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->req_time_total)/(float)report->time_interval);
					break;
				case 5: /* ru_utime_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->ru_utime_total));
					break;
				case 6: /* ru_utime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_utime_total)/(float)timeval_to_float(report->ru_utime_total));
					break;
				case 7: /* ru_utime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_utime_total)/(float)report->time_interval);
					break;
				case 8: /* ru_stime_total */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)data->req_count);
					break;
				case 9: /* ru_stime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_stime_total)/(float)timeval_to_float(report->ru_stime_total));
					break;
				case 10: /* ru_stime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)report->time_interval);
					break;
				case 11: /* traffic_total */
					(*field)->set_notnull();
					(*field)->store(data->kbytes_total);
					break;
				case 12: /* traffic_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->kbytes_total/report->kbytes_total);
					break;
				case 13: /* traffic_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->kbytes_total/(float)report->time_interval);
					break;
				case 14: /* script_name */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((const char *)index), &my_charset_bin);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::report2_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_report2_data *data;
	PPvoid_t ppvalue;
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	pinba_report *report;

	DBUG_ENTER("ha_pinba::report2_fetch_row");

	report = pinba_get_report(PINBA_TABLE_REPORT2, share);

	if (!report) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	if (this_index[0].position == 0 || this_index[0].str.val == NULL) {
		pthread_rwlock_rdlock(&report->lock);
		ppvalue = JudySLFirst(report->results, index, NULL);
	} else {
		pthread_rwlock_rdlock(&report->lock);
		strcpy((char *)index, (char *)this_index[0].str.val);
		ppvalue = JudySLNext(report->results, index, NULL);
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].str.val = (unsigned char *)strdup((char *)index);
	this_index[0].position++;

	data = (struct pinba_report2_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 1: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 2: /* req_time_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->req_time_total));
					break;
				case 3: /* req_time_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->req_time_total)/(float)timeval_to_float(report->time_total));
					break;
				case 4: /* req_time_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->req_time_total)/(float)report->time_interval);
					break;
				case 5: /* ru_utime_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->ru_utime_total));
					break;
				case 6: /* ru_utime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_utime_total)/(float)timeval_to_float(report->ru_utime_total));
					break;
				case 7: /* ru_utime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_utime_total)/(float)report->time_interval);
					break;
				case 8: /* ru_stime_total */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)data->req_count);
					break;
				case 9: /* ru_stime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_stime_total)/(float)timeval_to_float(report->ru_stime_total));
					break;
				case 10: /* ru_stime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)report->time_interval);
					break;
				case 11: /* traffic_total */
					(*field)->set_notnull();
					(*field)->store(data->kbytes_total);
					break;
				case 12: /* traffic_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->kbytes_total/(float)report->kbytes_total);
					break;
				case 13: /* traffic_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->kbytes_total/(float)report->time_interval);
					break;
				case 14: /* server_name */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((const char *)index), &my_charset_bin);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::report3_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_report3_data *data;
	PPvoid_t ppvalue;
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	pinba_report *report;

	DBUG_ENTER("ha_pinba::report3_fetch_row");

	report = pinba_get_report(PINBA_TABLE_REPORT3, share);

	if (!report) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	if (this_index[0].position == 0 || this_index[0].str.val == NULL) {
		pthread_rwlock_rdlock(&report->lock);
		ppvalue = JudySLFirst(report->results, index, NULL);
	} else {
		pthread_rwlock_rdlock(&report->lock);
		strcpy((char *)index, (char *)this_index[0].str.val);
		ppvalue = JudySLNext(report->results, index, NULL);
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].str.val = (unsigned char *)strdup((char *)index);
	this_index[0].position++;

	data = (struct pinba_report3_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 1: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 2: /* req_time_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->req_time_total));
					break;
				case 3: /* req_time_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->req_time_total)/(float)timeval_to_float(report->time_total));
					break;
				case 4: /* req_time_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->req_time_total)/(float)report->time_interval);
					break;
				case 5: /* ru_utime_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->ru_utime_total));
					break;
				case 6: /* ru_utime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_utime_total)/(float)timeval_to_float(report->ru_utime_total));
					break;
				case 7: /* ru_utime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_utime_total)/(float)report->time_interval);
					break;
				case 8: /* ru_stime_total */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)data->req_count);
					break;
				case 9: /* ru_stime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_stime_total)/(float)timeval_to_float(report->ru_stime_total));
					break;
				case 10: /* ru_stime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)report->time_interval);
					break;
				case 11: /* traffic_total */
					(*field)->set_notnull();
					(*field)->store(data->kbytes_total);
					break;
				case 12: /* traffic_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->kbytes_total/(float)report->kbytes_total);
					break;
				case 13: /* traffic_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->kbytes_total/(float)report->time_interval);
					break;
				case 14: /* hostname */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((const char *)index), &my_charset_bin);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::report4_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_report4_data *data;
	PPvoid_t ppvalue;
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	pinba_report *report;
	
	DBUG_ENTER("ha_pinba::report4_fetch_row");

	report = pinba_get_report(PINBA_TABLE_REPORT4, share);

	if (!report) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	if (this_index[0].position == 0 || this_index[0].str.val == NULL) {
		pthread_rwlock_rdlock(&report->lock);
		ppvalue = JudySLFirst(report->results, index, NULL);
	} else {
		pthread_rwlock_rdlock(&report->lock);
		strcpy((char *)index, (char *)this_index[0].str.val);
		ppvalue = JudySLNext(report->results, index, NULL);
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].str.val = (unsigned char *)strdup((char *)index);
	this_index[0].position++;

	data = (struct pinba_report4_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 1: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 2: /* req_time_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->req_time_total));
					break;
				case 3: /* req_time_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->req_time_total)/(float)timeval_to_float(report->time_total));
					break;
				case 4: /* req_time_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->req_time_total)/(float)report->time_interval);
					break;
				case 5: /* ru_utime_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->ru_utime_total));
					break;
				case 6: /* ru_utime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_utime_total)/(float)timeval_to_float(report->ru_utime_total));
					break;
				case 7: /* ru_utime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_utime_total)/(float)report->time_interval);
					break;
				case 8: /* ru_stime_total */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)data->req_count);
					break;
				case 9: /* ru_stime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_stime_total)/(float)timeval_to_float(report->ru_stime_total));
					break;
				case 10: /* ru_stime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)report->time_interval);
					break;
				case 11: /* traffic_total */
					(*field)->set_notnull();
					(*field)->store(data->kbytes_total);
					break;
				case 12: /* traffic_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->kbytes_total/report->kbytes_total);
					break;
				case 13: /* traffic_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->kbytes_total/(float)report->time_interval);
					break;
				case 14: /* server_name */
					(*field)->set_notnull();
					(*field)->store((const char *)data->server_name, strlen((const char *)data->server_name), &my_charset_bin);
					break;
				case 15: /* script_name */
					(*field)->set_notnull();
					(*field)->store((const char *)data->script_name, strlen((const char *)data->script_name), &my_charset_bin);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::report5_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_report5_data *data;
	PPvoid_t ppvalue;
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	pinba_report *report;
	
	DBUG_ENTER("ha_pinba::report5_fetch_row");

	report = pinba_get_report(PINBA_TABLE_REPORT5, share);

	if (!report) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	if (this_index[0].position == 0 || this_index[0].str.val == NULL) {
		pthread_rwlock_rdlock(&report->lock);
		ppvalue = JudySLFirst(report->results, index, NULL);
	} else {
		pthread_rwlock_rdlock(&report->lock);
		strcpy((char *)index, (char *)this_index[0].str.val);
		ppvalue = JudySLNext(report->results, index, NULL);
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].str.val = (unsigned char *)strdup((char *)index);
	this_index[0].position++;

	data = (struct pinba_report5_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 1: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 2: /* req_time_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->req_time_total));
					break;
				case 3: /* req_time_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->req_time_total)/timeval_to_float(report->time_total));
					break;
				case 4: /* req_time_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->req_time_total)/(float)report->time_interval);
					break;
				case 5: /* ru_utime_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->ru_utime_total));
					break;
				case 6: /* ru_utime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_utime_total)/(float)timeval_to_float(report->ru_utime_total));
					break;
				case 7: /* ru_utime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_utime_total)/(float)report->time_interval);
					break;
				case 8: /* ru_stime_total */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)data->req_count);
					break;
				case 9: /* ru_stime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_stime_total)/(float)timeval_to_float(report->ru_stime_total));
					break;
				case 10: /* ru_stime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)report->time_interval);
					break;
				case 11: /* traffic_total */
					(*field)->set_notnull();
					(*field)->store(data->kbytes_total);
					break;
				case 12: /* traffic_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->kbytes_total/report->kbytes_total);
					break;
				case 13: /* traffic_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->kbytes_total/(float)report->time_interval);
					break;
				case 14: /* hostname */
					(*field)->set_notnull();
					(*field)->store((const char *)data->hostname, strlen((const char *)data->hostname), &my_charset_bin);
					break;
				case 15: /* script_name */
					(*field)->set_notnull();
					(*field)->store((const char *)data->script_name, strlen((const char *)data->script_name), &my_charset_bin);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::report6_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_report6_data *data;
	PPvoid_t ppvalue;
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	pinba_report *report;
	
	DBUG_ENTER("ha_pinba::report6_fetch_row");

	report = pinba_get_report(PINBA_TABLE_REPORT6, share);

	if (!report) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	if (this_index[0].position == 0 || this_index[0].str.val == NULL) {
		pthread_rwlock_rdlock(&report->lock);
		ppvalue = JudySLFirst(report->results, index, NULL);
	} else {
		pthread_rwlock_rdlock(&report->lock);
		strcpy((char *)index, (char *)this_index[0].str.val);
		ppvalue = JudySLNext(report->results, index, NULL);
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].str.val = (unsigned char *)strdup((char *)index);
	this_index[0].position++;

	data = (struct pinba_report6_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 1: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 2: /* req_time_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->req_time_total));
					break;
				case 3: /* req_time_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->req_time_total)/timeval_to_float(report->time_total));
					break;
				case 4: /* req_time_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->req_time_total)/(float)report->time_interval);
					break;
				case 5: /* ru_utime_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->ru_utime_total));
					break;
				case 6: /* ru_utime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_utime_total)/(float)timeval_to_float(report->ru_utime_total));
					break;
				case 7: /* ru_utime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_utime_total)/(float)report->time_interval);
					break;
				case 8: /* ru_stime_total */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)data->req_count);
					break;
				case 9: /* ru_stime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_stime_total)/(float)timeval_to_float(report->ru_stime_total));
					break;
				case 10: /* ru_stime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)report->time_interval);
					break;
				case 11: /* traffic_total */
					(*field)->set_notnull();
					(*field)->store(data->kbytes_total);
					break;
				case 12: /* traffic_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->kbytes_total/report->kbytes_total);
					break;
				case 13: /* traffic_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->kbytes_total/(float)report->time_interval);
					break;
				case 14: /* hostname */
					(*field)->set_notnull();
					(*field)->store((const char *)data->hostname, strlen((const char *)data->hostname), &my_charset_bin);
					break;
				case 15: /* server_name */
					(*field)->set_notnull();
					(*field)->store((const char *)data->server_name, strlen((const char *)data->server_name), &my_charset_bin);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::report7_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_report7_data *data;
	PPvoid_t ppvalue;
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	pinba_report *report;
	
	DBUG_ENTER("ha_pinba::report7_fetch_row");

	report = pinba_get_report(PINBA_TABLE_REPORT7, share);

	if (!report) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	if (this_index[0].position == 0 || this_index[0].str.val == NULL) {
		pthread_rwlock_rdlock(&report->lock);
		ppvalue = JudySLFirst(report->results, index, NULL);
	} else {
		pthread_rwlock_rdlock(&report->lock);
		strcpy((char *)index, (char *)this_index[0].str.val);
		ppvalue = JudySLNext(report->results, index, NULL);
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].str.val = (unsigned char *)strdup((char *)index);
	this_index[0].position++;

	data = (struct pinba_report7_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 1: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 2: /* req_time_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->req_time_total));
					break;
				case 3: /* req_time_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->req_time_total)/timeval_to_float(report->time_total));
					break;
				case 4: /* req_time_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->req_time_total)/(float)report->time_interval);
					break;
				case 5: /* ru_utime_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->ru_utime_total));
					break;
				case 6: /* ru_utime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_utime_total)/(float)timeval_to_float(report->ru_utime_total));
					break;
				case 7: /* ru_utime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_utime_total)/(float)report->time_interval);
					break;
				case 8: /* ru_stime_total */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)data->req_count);
					break;
				case 9: /* ru_stime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_stime_total)/(float)timeval_to_float(report->ru_stime_total));
					break;
				case 10: /* ru_stime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)report->time_interval);
					break;
				case 11: /* traffic_total */
					(*field)->set_notnull();
					(*field)->store(data->kbytes_total);
					break;
				case 12: /* traffic_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->kbytes_total/report->kbytes_total);
					break;
				case 13: /* traffic_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->kbytes_total/(float)report->time_interval);
					break;
				case 14: /* hostname */
					(*field)->set_notnull();
					(*field)->store((const char *)data->hostname, strlen((const char *)data->hostname), &my_charset_bin);
					break;
				case 15: /* server_name */
					(*field)->set_notnull();
					(*field)->store((const char *)data->server_name, strlen((const char *)data->server_name), &my_charset_bin);
					break;
				case 16: /* script_name */
					(*field)->set_notnull();
					(*field)->store((const char *)data->script_name, strlen((const char *)data->script_name), &my_charset_bin);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::report8_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	pinba_report *report;
	struct pinba_report8_data *data;
	PPvoid_t ppvalue;
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	
	DBUG_ENTER("ha_pinba::report8_fetch_row");

	report = pinba_get_report(PINBA_TABLE_REPORT8, share);

	if (!report) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}
	
	if (this_index[0].position == 0 || this_index[0].str.val == NULL) {
		pthread_rwlock_rdlock(&report->lock);
		ppvalue = JudySLFirst(report->results, index, NULL);
	} else {
		pthread_rwlock_rdlock(&report->lock);
		strcpy((char *)index, (char *)this_index[0].str.val);
		ppvalue = JudySLNext(report->results, index, NULL);
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].str.val = (unsigned char *)strdup((char *)index);
	this_index[0].position++;

	data = (struct pinba_report8_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 1: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 2: /* req_time_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->req_time_total));
					break;
				case 3: /* req_time_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->req_time_total)/timeval_to_float(report->time_total));
					break;
				case 4: /* req_time_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->req_time_total)/(float)report->time_interval);
					break;
				case 5: /* ru_utime_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->ru_utime_total));
					break;
				case 6: /* ru_utime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_utime_total)/(float)timeval_to_float(report->ru_utime_total));
					break;
				case 7: /* ru_utime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_utime_total)/(float)report->time_interval);
					break;
				case 8: /* ru_stime_total */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)data->req_count);
					break;
				case 9: /* ru_stime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_stime_total)/(float)timeval_to_float(report->ru_stime_total));
					break;
				case 10: /* ru_stime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)report->time_interval);
					break;
				case 11: /* traffic_total */
					(*field)->set_notnull();
					(*field)->store(data->kbytes_total);
					break;
				case 12: /* traffic_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->kbytes_total/report->kbytes_total);
					break;
				case 13: /* traffic_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->kbytes_total/(float)report->time_interval);
					break;
				case 14: /* status */
					(*field)->set_notnull();
					(*field)->store((long)data->status);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::report9_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_report9_data *data;
	PPvoid_t ppvalue;
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	pinba_report *report;
	
	DBUG_ENTER("ha_pinba::report9_fetch_row");

	report = pinba_get_report(PINBA_TABLE_REPORT9, share);

	if (!report) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	if (this_index[0].position == 0 || this_index[0].str.val == NULL) {
		pthread_rwlock_rdlock(&report->lock);
		ppvalue = JudySLFirst(report->results, index, NULL);
	} else {
		pthread_rwlock_rdlock(&report->lock);
		strcpy((char *)index, (char *)this_index[0].str.val);
		ppvalue = JudySLNext(report->results, index, NULL);
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].str.val = (unsigned char *)strdup((char *)index);
	this_index[0].position++;

	data = (struct pinba_report9_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 1: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 2: /* req_time_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->req_time_total));
					break;
				case 3: /* req_time_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->req_time_total)/timeval_to_float(report->time_total));
					break;
				case 4: /* req_time_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->req_time_total)/(float)report->time_interval);
					break;
				case 5: /* ru_utime_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->ru_utime_total));
					break;
				case 6: /* ru_utime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_utime_total)/(float)timeval_to_float(report->ru_utime_total));
					break;
				case 7: /* ru_utime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_utime_total)/(float)report->time_interval);
					break;
				case 8: /* ru_stime_total */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)data->req_count);
					break;
				case 9: /* ru_stime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_stime_total)/(float)timeval_to_float(report->ru_stime_total));
					break;
				case 10: /* ru_stime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)report->time_interval);
					break;
				case 11: /* traffic_total */
					(*field)->set_notnull();
					(*field)->store(data->kbytes_total);
					break;
				case 12: /* traffic_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->kbytes_total/report->kbytes_total);
					break;
				case 13: /* traffic_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->kbytes_total/(float)report->time_interval);
					break;
				case 14: /* script_name */
					(*field)->set_notnull();
					(*field)->store((const char *)data->script_name, strlen((const char *)data->script_name), &my_charset_bin);
					break;
				case 15: /* status */
					(*field)->set_notnull();
					(*field)->store((long)data->status);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::report10_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_report10_data *data;
	PPvoid_t ppvalue;
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	pinba_report *report;
	
	DBUG_ENTER("ha_pinba::report10_fetch_row");

	report = pinba_get_report(PINBA_TABLE_REPORT10, share);

	if (!report) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	if (this_index[0].position == 0 || this_index[0].str.val == NULL) {
		pthread_rwlock_rdlock(&report->lock);
		ppvalue = JudySLFirst(report->results, index, NULL);
	} else {
		pthread_rwlock_rdlock(&report->lock);
		strcpy((char *)index, (char *)this_index[0].str.val);
		ppvalue = JudySLNext(report->results, index, NULL);
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].str.val = (unsigned char *)strdup((char *)index);
	this_index[0].position++;

	data = (struct pinba_report10_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 1: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 2: /* req_time_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->req_time_total));
					break;
				case 3: /* req_time_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->req_time_total)/timeval_to_float(report->time_total));
					break;
				case 4: /* req_time_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->req_time_total)/(float)report->time_interval);
					break;
				case 5: /* ru_utime_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->ru_utime_total));
					break;
				case 6: /* ru_utime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_utime_total)/(float)timeval_to_float(report->ru_utime_total));
					break;
				case 7: /* ru_utime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_utime_total)/(float)report->time_interval);
					break;
				case 8: /* ru_stime_total */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)data->req_count);
					break;
				case 9: /* ru_stime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_stime_total)/(float)timeval_to_float(report->ru_stime_total));
					break;
				case 10: /* ru_stime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)report->time_interval);
					break;
				case 11: /* traffic_total */
					(*field)->set_notnull();
					(*field)->store(data->kbytes_total);
					break;
				case 12: /* traffic_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->kbytes_total/report->kbytes_total);
					break;
				case 13: /* traffic_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->kbytes_total/(float)report->time_interval);
					break;
				case 14: /* server_name */
					(*field)->set_notnull();
					(*field)->store((const char *)data->server_name, strlen((const char *)data->server_name), &my_charset_bin);
					break;
				case 15: /* status */
					(*field)->set_notnull();
					(*field)->store((long)data->status);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::report11_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_report11_data *data;
	PPvoid_t ppvalue;
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	pinba_report *report;
	
	DBUG_ENTER("ha_pinba::report11_fetch_row");

	report = pinba_get_report(PINBA_TABLE_REPORT11, share);

	if (!report) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	if (this_index[0].position == 0 || this_index[0].str.val == NULL) {
		pthread_rwlock_rdlock(&report->lock);
		ppvalue = JudySLFirst(report->results, index, NULL);
	} else {
		pthread_rwlock_rdlock(&report->lock);
		strcpy((char *)index, (char *)this_index[0].str.val);
		ppvalue = JudySLNext(report->results, index, NULL);
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].str.val = (unsigned char *)strdup((char *)index);
	this_index[0].position++;

	data = (struct pinba_report11_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 1: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 2: /* req_time_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->req_time_total));
					break;
				case 3: /* req_time_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->req_time_total)/timeval_to_float(report->time_total));
					break;
				case 4: /* req_time_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->req_time_total)/(float)report->time_interval);
					break;
				case 5: /* ru_utime_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->ru_utime_total));
					break;
				case 6: /* ru_utime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_utime_total)/(float)timeval_to_float(report->ru_utime_total));
					break;
				case 7: /* ru_utime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_utime_total)/(float)report->time_interval);
					break;
				case 8: /* ru_stime_total */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)data->req_count);
					break;
				case 9: /* ru_stime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_stime_total)/(float)timeval_to_float(report->ru_stime_total));
					break;
				case 10: /* ru_stime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)report->time_interval);
					break;
				case 11: /* traffic_total */
					(*field)->set_notnull();
					(*field)->store(data->kbytes_total);
					break;
				case 12: /* traffic_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->kbytes_total/report->kbytes_total);
					break;
				case 13: /* traffic_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->kbytes_total/(float)report->time_interval);
					break;
				case 14: /* hostname */
					(*field)->set_notnull();
					(*field)->store((const char *)data->hostname, strlen((const char *)data->hostname), &my_charset_bin);
					break;
				case 15: /* status */
					(*field)->set_notnull();
					(*field)->store((long)data->status);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::report12_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_report12_data *data;
	PPvoid_t ppvalue;
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};
	pinba_report *report;
	
	DBUG_ENTER("ha_pinba::report12_fetch_row");

	report = pinba_get_report(PINBA_TABLE_REPORT12, share);

	if (!report) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	if (this_index[0].position == 0 || this_index[0].str.val == NULL) {
		pthread_rwlock_rdlock(&report->lock);
		ppvalue = JudySLFirst(report->results, index, NULL);
	} else {
		pthread_rwlock_rdlock(&report->lock);
		strcpy((char *)index, (char *)this_index[0].str.val);
		ppvalue = JudySLNext(report->results, index, NULL);
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].str.val = (unsigned char *)strdup((char *)index);
	this_index[0].position++;

	data = (struct pinba_report12_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 1: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 2: /* req_time_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->req_time_total));
					break;
				case 3: /* req_time_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->req_time_total)/timeval_to_float(report->time_total));
					break;
				case 4: /* req_time_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->req_time_total)/(float)report->time_interval);
					break;
				case 5: /* ru_utime_total */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->ru_utime_total));
					break;
				case 6: /* ru_utime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_utime_total)/(float)timeval_to_float(report->ru_utime_total));
					break;
				case 7: /* ru_utime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_utime_total)/(float)report->time_interval);
					break;
				case 8: /* ru_stime_total */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)data->req_count);
					break;
				case 9: /* ru_stime_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)timeval_to_float(data->ru_stime_total)/(float)timeval_to_float(report->ru_stime_total));
					break;
				case 10: /* ru_stime_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)timeval_to_float(data->ru_stime_total)/(float)report->time_interval);
					break;
				case 11: /* traffic_total */
					(*field)->set_notnull();
					(*field)->store(data->kbytes_total);
					break;
				case 12: /* traffic_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->kbytes_total/report->kbytes_total);
					break;
				case 13: /* traffic_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->kbytes_total/(float)report->time_interval);
					break;
				case 14: /* hostname */
					(*field)->set_notnull();
					(*field)->store((const char *)data->hostname, strlen((const char *)data->hostname), &my_charset_bin);
					break;
				case 15: /* script_name */
					(*field)->set_notnull();
					(*field)->store((const char *)data->script_name, strlen((const char *)data->script_name), &my_charset_bin);
					break;
				case 16: /* status */
					(*field)->set_notnull();
					(*field)->store((long)data->status);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::info_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	pinba_report *report;
	
	DBUG_ENTER("ha_pinba::info_fetch_row");

	report = pinba_get_report(PINBA_TABLE_REPORT_INFO, share);

	if (!report) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	pthread_rwlock_rdlock(&report->lock);
	if (this_index[0].position == 0) {
		/* report->time_interval = pinba_get_time_interval(); */
	} else {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].position++;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)report->results_cnt);
					break;
				case 1: /* req_time_total */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(report->time_total), 1000));
					break;
				case 2: /* ru_utime_total */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(report->ru_utime_total), 1000));
					break;
				case 3: /* ru_stime_total */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(report->ru_stime_total), 1000));
					break;
				case 4: /* time_interval */
					(*field)->set_notnull();
					(*field)->store((long)report->time_interval);
					break;
				case 5: /* kbytes_total */
					(*field)->set_notnull();
					(*field)->store(pinba_round(report->kbytes_total, 1000));
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::tag_info_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_tag_info_data *data;
	pinba_tag_report *report;
	PPvoid_t ppvalue;
	uint8_t index[PINBA_TAG_VALUE_SIZE] = {0};
	
	DBUG_ENTER("ha_pinba::tag_info_fetch_row");

	if (!share->params || share->params[0] == '\0') {
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	if (this_index[0].position == 0) {
		time_t now = time(NULL);

		report = pinba_get_tag_report(PINBA_TABLE_TAG_INFO, share);
		if (!report) {
			pthread_rwlock_unlock(&D->tag_reports_lock);
			pthread_rwlock_wrlock(&D->tag_reports_lock);
			pthread_rwlock_rdlock(&D->timer_lock);
			report = pinba_regenerate_tag_info(share);
			pthread_rwlock_unlock(&D->timer_lock);
			pthread_rwlock_unlock(&D->tag_reports_lock);
			pthread_rwlock_rdlock(&D->tag_reports_lock);
		}

		if (!report) {
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}

		pthread_rwlock_wrlock(&report->lock);
		report->last_requested = now;
		ppvalue = JudySLFirst(report->results, index, NULL);
	} else {
		report = pinba_get_tag_report(PINBA_TABLE_TAG_INFO, share);
		if (!report) {
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}

		pthread_rwlock_wrlock(&report->lock);
		strcpy((char *)index, (char *)this_index[0].str.val);
		ppvalue = JudySLNext(report->results, index, NULL);
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].str.val = (unsigned char *)strdup((char *)index);
	this_index[0].position++;

	data = (struct pinba_tag_info_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* tag_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((const char *)index), &my_charset_bin);
					break;
				case 1: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 2: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 3: /* hit_count */
					(*field)->set_notnull();
					(*field)->store((long)data->hit_count);
					break;
				case 4: /* hit_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->hit_count/(float)report->time_interval);
					break;
				case 5: /* timer_value */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->timer_value));
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::tag2_info_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_tag2_info_data *data;
	pinba_tag_report *report;
	PPvoid_t ppvalue;
	uint8_t index[PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE] = {0};
	
	DBUG_ENTER("ha_pinba::tag2_info_fetch_row");

	if (!share->params || share->params[0] == '\0') {
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	if (this_index[0].position == 0) {
		time_t now = time(NULL);

		report = pinba_get_tag_report(PINBA_TABLE_TAG2_INFO, share);
		if (!report) {
			pthread_rwlock_unlock(&D->tag_reports_lock);
			pthread_rwlock_wrlock(&D->tag_reports_lock);
			pthread_rwlock_rdlock(&D->timer_lock);
			report = pinba_regenerate_tag2_info(share);
			pthread_rwlock_unlock(&D->timer_lock);
			pthread_rwlock_unlock(&D->tag_reports_lock);
			pthread_rwlock_rdlock(&D->tag_reports_lock);
		}
		if (!report) {
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}

		pthread_rwlock_rdlock(&report->lock);
		report->last_requested = now;
		ppvalue = JudySLFirst(report->results, index, NULL);
	} else {
		report = pinba_get_tag_report(PINBA_TABLE_TAG2_INFO, share);
		if (!report) {
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}

		pthread_rwlock_rdlock(&report->lock);
		strcpy((char *)index, (char *)this_index[0].str.val);
		ppvalue = JudySLNext(report->results, index, NULL);
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].str.val = (unsigned char *)strdup((char *)index);
	this_index[0].position++;

	data = (struct pinba_tag2_info_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* tag1_value */
					(*field)->set_notnull();
					(*field)->store((const char *)data->tag1_value, strlen((const char *)data->tag1_value), &my_charset_bin);
					break;
				case 1: /* tag2_value */
					(*field)->set_notnull();
					(*field)->store((const char *)data->tag2_value, strlen((const char *)data->tag2_value), &my_charset_bin);
					break;
				case 2: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 3: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 4: /* hit_count */
					(*field)->set_notnull();
					(*field)->store((long)data->hit_count);
					break;
				case 5: /* hit_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->hit_count/(float)report->time_interval);
					break;
				case 6: /* timer_value */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->timer_value));
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::tag_report_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_tag_report_data *data;
	pinba_tag_report *report;
	PPvoid_t ppvalue, ppvalue_script;
	uint8_t index_script[PINBA_SCRIPT_NAME_SIZE + 1] = {0};
	uint8_t index[PINBA_TAG_VALUE_SIZE + 1] = {0};
	
	DBUG_ENTER("ha_pinba::tag_report_fetch_row");

	if (!share->params || share->params[0] == '\0') {
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	report = pinba_get_tag_report(PINBA_TABLE_TAG_REPORT, share);
	if (!report) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	pthread_rwlock_rdlock(&report->lock);
	if (this_index[0].str.val == NULL) {
		time_t now = time(NULL);
		report->last_requested = now;
		ppvalue_script = JudySLFirst(report->results, index_script, NULL);
	
		if (!ppvalue_script) {
			pthread_rwlock_unlock(&report->lock);
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}

		ppvalue = JudySLFirst(*ppvalue_script, index, NULL);
		if (!ppvalue) {
			pthread_rwlock_unlock(&report->lock);
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}
		this_index[0].str.val = (unsigned char *)strdup((char *)index_script);
	} else {
		strcpy((char *)index_script, (char *)this_index[0].str.val);

		ppvalue_script = JudySLGet(report->results, index_script, NULL);
		if (!ppvalue_script) {
			pthread_rwlock_unlock(&report->lock);
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}

repeat_with_next_script:
		if (this_index[0].subindex.val == NULL) {
			index[0] = '\0';
			ppvalue = JudySLFirst(*ppvalue_script, index, NULL);
		} else {
			strcpy((char *)index, (char *)this_index[0].subindex.val);
			ppvalue = JudySLNext(*ppvalue_script, index, NULL);
			free(this_index[0].subindex.val);
			this_index[0].subindex.val = NULL;
		}

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			
			ppvalue_script = JudySLNext(report->results, index_script, NULL);
			
			free(this_index[0].str.val);
			this_index[0].str.val = NULL;

			if (ppvalue_script) {
				this_index[0].str.val = (unsigned char *)strdup((char *)index_script);
				free(this_index[0].subindex.val);
				this_index[0].subindex.val = NULL;
				goto repeat_with_next_script;
			} else {
				pthread_rwlock_unlock(&report->lock);
				DBUG_RETURN(HA_ERR_END_OF_FILE);
			}
		}

	}

	this_index[0].subindex.val = (unsigned char *)strdup((char *)index);
	data = (struct pinba_tag_report_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* script_name */
					(*field)->set_notnull();
					(*field)->store((const char *)data->script_name, strlen((const char *)data->script_name), &my_charset_bin);
					break;
				case 1: /* tag_value */
					(*field)->set_notnull();
					(*field)->store((const char *)data->tag_value, strlen((const char *)data->tag_value), &my_charset_bin);
					break;
				case 2: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 3: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 4: /* hit_count */
					(*field)->set_notnull();
					(*field)->store((long)data->hit_count);
					break;
				case 5: /* hit_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->hit_count/(float)report->time_interval);
					break;
				case 6: /* timer_value */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->timer_value));
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::tag_report_fetch_row_by_script(unsigned char *buf, const unsigned char *name, uint name_len) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_tag_report_data *data;
	pinba_tag_report *report;
	PPvoid_t ppvalue, ppvalue_script;
	uint8_t index[PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE] = {0};
	
	DBUG_ENTER("ha_pinba::tag_report_fetch_row_by_script");

	if (!share->params || share->params[0] == '\0') {
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	report = pinba_get_tag_report(PINBA_TABLE_TAG_REPORT, share);
	if (!report) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	pthread_rwlock_rdlock(&report->lock);

	if (!this_index[0].str.val) {
		ppvalue_script = JudySLFirst(report->results, index, NULL);
		if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}
		this_index[0].str.val = (unsigned char *)strdup((char *)index);
		index[0] = '\0';
	} else {
		ppvalue_script = JudySLGet(report->results, this_index[0].str.val, NULL);
		if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}
	}

	if (this_index[0].subindex.val == NULL) {
		time_t now = time(NULL);
		report->last_requested = now;
		ppvalue = JudySLFirst(*ppvalue_script, index, NULL);
	} else {
		strcpy((char *)index, (char *)this_index[0].subindex.val);
		ppvalue = JudySLNext(*ppvalue_script, index, NULL);
		free(this_index[0].subindex.val);
		this_index[0].subindex.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].subindex.val = (unsigned char *)strdup((char *)index);

	data = (struct pinba_tag_report_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* script_name */
					(*field)->set_notnull();
					(*field)->store((const char *)data->script_name, strlen((const char *)data->script_name), &my_charset_bin);
					break;
				case 1: /* tag_value */
					(*field)->set_notnull();
					(*field)->store((const char *)data->tag_value, strlen((const char *)data->tag_value), &my_charset_bin);
					break;
				case 2: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 3: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 4: /* hit_count */
					(*field)->set_notnull();
					(*field)->store((long)data->hit_count);
					break;
				case 5: /* hit_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->hit_count/(float)report->time_interval);
					break;
				case 6: /* timer_value */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->timer_value));
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::tag2_report_fetch_row_by_script(unsigned char *buf, const unsigned char *name, uint name_len) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_tag2_report_data *data;
	pinba_tag_report *report;
	PPvoid_t ppvalue, ppvalue_script;
	uint8_t index[PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE] = {0};
	
	DBUG_ENTER("ha_pinba::tag_report_fetch_row_by_script");

	if (!share->params || share->params[0] == '\0') {
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	report = pinba_get_tag_report(PINBA_TABLE_TAG2_REPORT, share);
	if (!report) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	pthread_rwlock_rdlock(&report->lock);
	if (!this_index[0].str.val) {
		ppvalue_script = JudySLFirst(report->results, index, NULL);
		if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}
		this_index[0].str.val = (unsigned char *)strdup((char *)index);
		index[0] = '\0';
	} else {
		ppvalue_script = JudySLGet(report->results, this_index[0].str.val, NULL);
		if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}
	}

	if (this_index[0].subindex.val == NULL) {
		time_t now = time(NULL);
		report->last_requested = now;
		ppvalue = JudySLFirst(*ppvalue_script, index, NULL);
	} else {
		strcpy((char *)index, (char *)this_index[0].subindex.val);
		ppvalue = JudySLNext(*ppvalue_script, index, NULL);
		free(this_index[0].subindex.val);
		this_index[0].subindex.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].subindex.val = (unsigned char *)strdup((char *)index);

	data = (struct pinba_tag2_report_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* script_name */
					(*field)->set_notnull();
					(*field)->store((const char *)data->script_name, strlen((const char *)data->script_name), &my_charset_bin);
					break;
				case 1: /* tag1_value */
					(*field)->set_notnull();
					(*field)->store((const char *)data->tag1_value, strlen((const char *)data->tag1_value), &my_charset_bin);
					break;
				case 2: /* tag2_value */
					(*field)->set_notnull();
					(*field)->store((const char *)data->tag2_value, strlen((const char *)data->tag2_value), &my_charset_bin);
					break;
				case 3: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 4: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 5: /* hit_count */
					(*field)->set_notnull();
					(*field)->store((long)data->hit_count);
					break;
				case 6: /* hit_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->hit_count/(float)report->time_interval);
					break;
				case 7: /* timer_value */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->timer_value));
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int  ha_pinba::tag2_report_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_tag2_report_data *data;
	pinba_tag_report *report;
	PPvoid_t ppvalue, ppvalue_script;
	uint8_t index_script[PINBA_SCRIPT_NAME_SIZE + 1] = {0};
	uint8_t index[PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1] = {0};
	
	DBUG_ENTER("ha_pinba::tag2_report_fetch_row");

	if (!share->params || share->params[0] == '\0') {
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	report = pinba_get_tag_report(PINBA_TABLE_TAG_REPORT, share);
	if (!report) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	pthread_rwlock_rdlock(&report->lock);
	if (this_index[0].str.val == NULL) {
		time_t now = time(NULL);
		report->last_requested = now;
		ppvalue_script = JudySLFirst(report->results, index_script, NULL);
	
		if (!ppvalue_script) {
			pthread_rwlock_unlock(&report->lock);
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}

		ppvalue = JudySLFirst(*ppvalue_script, index, NULL);
		if (!ppvalue) {
			pthread_rwlock_unlock(&report->lock);
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}
		this_index[0].str.val = (unsigned char *)strdup((char *)index_script);
	} else {
		strcpy((char *)index_script, (char *)this_index[0].str.val);

		ppvalue_script = JudySLGet(report->results, index_script, NULL);
		if (!ppvalue_script) {
			pthread_rwlock_unlock(&report->lock);
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}

repeat_with_next_script:
		if (this_index[0].subindex.val == NULL) {
			index[0] = '\0';
			ppvalue = JudySLFirst(*ppvalue_script, index, NULL);
		} else {
			strcpy((char *)index, (char *)this_index[0].subindex.val);
			ppvalue = JudySLNext(*ppvalue_script, index, NULL);
			free(this_index[0].subindex.val);
			this_index[0].subindex.val = NULL;
		}

		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			
			ppvalue_script = JudySLNext(report->results, index_script, NULL);
			
			free(this_index[0].str.val);
			this_index[0].str.val = NULL;

			if (ppvalue_script) {
				this_index[0].str.val = (unsigned char *)strdup((char *)index_script);
				free(this_index[0].subindex.val);
				this_index[0].subindex.val = NULL;
				goto repeat_with_next_script;
			} else {
				pthread_rwlock_unlock(&report->lock);
				DBUG_RETURN(HA_ERR_END_OF_FILE);
			}
		}

	}

	this_index[0].subindex.val = (unsigned char *)strdup((char *)index);
	data = (struct pinba_tag2_report_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* script_name */
					(*field)->set_notnull();
					(*field)->store((const char *)data->script_name, strlen((const char *)data->script_name), &my_charset_bin);
					break;
				case 1: /* tag1_value */
					(*field)->set_notnull();
					(*field)->store((const char *)data->tag1_value, strlen((const char *)data->tag1_value), &my_charset_bin);
					break;
				case 2: /* tag2_value */
					(*field)->set_notnull();
					(*field)->store((const char *)data->tag2_value, strlen((const char *)data->tag2_value), &my_charset_bin);
					break;
				case 3: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 4: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 5: /* hit_count */
					(*field)->set_notnull();
					(*field)->store((long)data->hit_count);
					break;
				case 6: /* hit_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->hit_count/(float)report->time_interval);
					break;
				case 7: /* timer_value */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->timer_value));
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::tag_report2_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_tag_report2_data *data;
	pinba_tag_report *report;
	PPvoid_t ppvalue;
	uint8_t index[PINBA_HOSTNAME_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE] = {0};

	DBUG_ENTER("ha_pinba::tag_report2_fetch_row");

	if (!share->params || share->params[0] == '\0') {
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	if (this_index[0].position == 0) {
		time_t now = time(NULL);

		report = pinba_get_tag_report(PINBA_TABLE_TAG_REPORT2, share);
		if (!report) {
			pthread_rwlock_unlock(&D->tag_reports_lock);
			pthread_rwlock_wrlock(&D->tag_reports_lock);
			pthread_rwlock_rdlock(&D->timer_lock);
			report = pinba_regenerate_tag_report2(share);
			pthread_rwlock_unlock(&D->timer_lock);
			pthread_rwlock_unlock(&D->tag_reports_lock);
			pthread_rwlock_rdlock(&D->tag_reports_lock);
		}

		if (!report) {
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}

		pthread_rwlock_rdlock(&report->lock);
		report->last_requested = now;
		ppvalue = JudySLFirst(report->results, index, NULL);
	} else {
		report = pinba_get_tag_report(PINBA_TABLE_TAG_REPORT2, share);
		if (!report) {
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}

		pthread_rwlock_rdlock(&report->lock);
		strcpy((char *)index, (char *)this_index[0].str.val);
		ppvalue = JudySLNext(report->results, index, NULL);
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].str.val = (unsigned char *)strdup((char *)index);
	this_index[0].position++;

	data = (struct pinba_tag_report2_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* script_name */
					(*field)->set_notnull();
					(*field)->store((const char *)data->script_name, strlen((const char *)data->script_name), &my_charset_bin);
					break;
				case 1: /* tag_value */
					(*field)->set_notnull();
					(*field)->store((const char *)data->tag_value, strlen((const char *)data->tag_value), &my_charset_bin);
					break;
				case 2: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 3: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 4: /* hit_count */
					(*field)->set_notnull();
					(*field)->store((long)data->hit_count);
					break;
				case 5: /* hit_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->hit_count/(float)report->time_interval);
					break;
				case 6: /* timer_value */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->timer_value));
					break;
				case 7: /* hostname */
					(*field)->set_notnull();
					(*field)->store((const char *)data->hostname, strlen((const char *)data->hostname), &my_charset_bin);
					break;
				case 8: /* server_name */
					(*field)->set_notnull();
					(*field)->store((const char *)data->server_name, strlen((const char *)data->server_name), &my_charset_bin);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int  ha_pinba::tag2_report2_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_tag2_report2_data *data;
	pinba_tag_report *report;
	PPvoid_t ppvalue;
	uint8_t index[PINBA_HOSTNAME_SIZE + 1 + PINBA_SERVER_NAME_SIZE + 1 + PINBA_SCRIPT_NAME_SIZE + 1 + PINBA_TAG_VALUE_SIZE + 1 + PINBA_TAG_VALUE_SIZE] = {0};

	DBUG_ENTER("ha_pinba::tag2_report2_fetch_row");

	if (!share->params || share->params[0] == '\0') {
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	if (this_index[0].position == 0) {
		time_t now = time(NULL);

		report = pinba_get_tag_report(PINBA_TABLE_TAG2_REPORT2, share);
		if (!report) {
			pthread_rwlock_unlock(&D->tag_reports_lock);
			pthread_rwlock_wrlock(&D->tag_reports_lock);
			pthread_rwlock_rdlock(&D->timer_lock);
			report = pinba_regenerate_tag2_report2(share);
			pthread_rwlock_unlock(&D->timer_lock);
			pthread_rwlock_unlock(&D->tag_reports_lock);
			pthread_rwlock_rdlock(&D->tag_reports_lock);
		}
		if (!report) {
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}

		pthread_rwlock_rdlock(&report->lock);
		report->last_requested = now;
		ppvalue = JudySLFirst(report->results, index, NULL);
	} else {
		report = pinba_get_tag_report(PINBA_TABLE_TAG2_REPORT2, share);
		if (!report) {
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}

		pthread_rwlock_rdlock(&report->lock);
		strcpy((char *)index, (char *)this_index[0].str.val);
		ppvalue = JudySLNext(report->results, index, NULL);
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].str.val = (unsigned char *)strdup((char *)index);
	this_index[0].position++;

	data = (struct pinba_tag2_report2_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* script_name */
					(*field)->set_notnull();
					(*field)->store((const char *)data->script_name, strlen((const char *)data->script_name), &my_charset_bin);
					break;
				case 1: /* tag1_value */
					(*field)->set_notnull();
					(*field)->store((const char *)data->tag1_value, strlen((const char *)data->tag1_value), &my_charset_bin);
					break;
				case 2: /* tag2_value */
					(*field)->set_notnull();
					(*field)->store((const char *)data->tag2_value, strlen((const char *)data->tag2_value), &my_charset_bin);
					break;
				case 3: /* req_count */
					(*field)->set_notnull();
					(*field)->store((long)data->req_count);
					break;
				case 4: /* req_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->req_count/(float)report->time_interval);
					break;
				case 5: /* hit_count */
					(*field)->set_notnull();
					(*field)->store((long)data->hit_count);
					break;
				case 6: /* hit_per_sec */
					(*field)->set_notnull();
					(*field)->store((float)data->hit_count/(float)report->time_interval);
					break;
				case 7: /* timer_value */
					(*field)->set_notnull();
					(*field)->store(timeval_to_float(data->timer_value));
					break;
				case 8: /* hostname */
					(*field)->set_notnull();
					(*field)->store((const char *)data->hostname, strlen((const char *)data->hostname), &my_charset_bin);
					break;
				case 9: /* server_name */
					(*field)->set_notnull();
					(*field)->store((const char *)data->server_name, strlen((const char *)data->server_name), &my_charset_bin);
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

/* </fetchers> }}} */

void ha_pinba::position(const unsigned char *record) /* {{{ */
{
	DBUG_ENTER("ha_pinba::position");
	DBUG_VOID_RETURN;
}
/* }}} */

int ha_pinba::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *create_info) /* {{{ */
{
	DBUG_ENTER("ha_pinba::create");

	if (pinba_get_table_type(table_arg) == PINBA_TABLE_UNKNOWN) {
		DBUG_RETURN(HA_WRONG_CREATE_OPTION);
	}

	if (pinba_parse_params(table_arg, NULL, NULL, NULL, NULL, NULL) < 0) {
		DBUG_RETURN(HA_WRONG_CREATE_OPTION);
	}

	DBUG_RETURN(0);
}
/* }}} */

int ha_pinba::delete_all_rows() /* {{{ */
{
	DBUG_ENTER("ha_example::delete_all_rows");

	switch (share->table_type) {
		case PINBA_TABLE_REQUEST:
			/* we only support 'delete from request' */
			break;
		default:
			DBUG_RETURN(HA_ERR_WRONG_COMMAND);
	}

	pthread_rwlock_wrlock(&D->collector_lock);
	/* destroy & reinitialize the request pool */
	pinba_pool_destroy(&D->request_pool);
	pinba_pool_init(&D->request_pool, D->request_pool.size, D->request_pool.element_size, D->request_pool.dtor);
	pthread_rwlock_unlock(&D->collector_lock);

	DBUG_RETURN(0);
}
/* }}} */


#define HANDLE_REPORT(__id__)												\
			{																\
				pinba_report *report;										\
																			\
				pthread_rwlock_rdlock(&D->collector_lock);					\
				pthread_rwlock_rdlock(&D->base_reports_lock);				\
				report = pinba_get_report(PINBA_TABLE_REPORT ## __id__, share);	\
																			\
				if (!report) {												\
					pthread_rwlock_unlock(&D->base_reports_lock);			\
					pthread_rwlock_wrlock(&D->base_reports_lock);			\
					report = pinba_regenerate_report ## __id__(share);		\
					pthread_rwlock_unlock(&D->base_reports_lock);			\
					pthread_rwlock_rdlock(&D->base_reports_lock);			\
				}															\
																			\
				stats.records = 0;											\
				if (report) {												\
					stats.records = report->results_cnt;					\
				}															\
				pthread_rwlock_unlock(&D->base_reports_lock);				\
				pthread_rwlock_unlock(&D->collector_lock);					\
			}

int ha_pinba::info(uint flag) /* {{{ */
{
	pinba_pool *p = &D->request_pool;
	DBUG_ENTER("ha_pinba::info");

	switch(share->table_type) {
		case PINBA_TABLE_REQUEST:
			pthread_rwlock_rdlock(&D->collector_lock);
			stats.records = pinba_pool_num_records(p);
			pthread_rwlock_unlock(&D->collector_lock);
			break;
		case PINBA_TABLE_TIMER:
			pthread_rwlock_rdlock(&D->collector_lock);
			stats.records = pinba_pool_num_records(&D->timer_pool);
			pthread_rwlock_unlock(&D->collector_lock);
			break;
		case PINBA_TABLE_TAG:
			pthread_rwlock_rdlock(&D->collector_lock);
			stats.records = JudyLCount(D->tag.table, 0, -1, NULL);
			pthread_rwlock_unlock(&D->collector_lock);
			break;
		case PINBA_TABLE_TIMERTAG:
			pthread_rwlock_rdlock(&D->collector_lock);
			stats.records = D->timertags_cnt;
			pthread_rwlock_unlock(&D->collector_lock);
			break;
		case PINBA_TABLE_REPORT_INFO:
			{
				pinba_report *report;

				pthread_rwlock_rdlock(&D->collector_lock);
				pthread_rwlock_rdlock(&D->base_reports_lock);
				report = pinba_get_report(PINBA_TABLE_REPORT_INFO, share);

				if (!report) {
					pthread_rwlock_unlock(&D->base_reports_lock);
					pthread_rwlock_wrlock(&D->base_reports_lock);
					report = pinba_regenerate_info(share);
					pthread_rwlock_unlock(&D->base_reports_lock);
					pthread_rwlock_rdlock(&D->base_reports_lock);
				}
				
				stats.records = 0;
				if (report) {
					stats.records = 1;
				}
				pthread_rwlock_unlock(&D->base_reports_lock);
				pthread_rwlock_unlock(&D->collector_lock);
			}
			break;
		case PINBA_TABLE_REPORT1:
			HANDLE_REPORT(1);
			break;
		case PINBA_TABLE_REPORT2:
			HANDLE_REPORT(2);
			break;
		case PINBA_TABLE_REPORT3:
			HANDLE_REPORT(3);
			break;
		case PINBA_TABLE_REPORT4:
			HANDLE_REPORT(4);
			break;
		case PINBA_TABLE_REPORT5:
			HANDLE_REPORT(5);
			break;
		case PINBA_TABLE_REPORT6:
			HANDLE_REPORT(6);
			break;
		case PINBA_TABLE_REPORT7:
			HANDLE_REPORT(7);
			break;
		case PINBA_TABLE_REPORT8:
			HANDLE_REPORT(8);
			break;
		case PINBA_TABLE_REPORT9:
			HANDLE_REPORT(9);
			break;
		case PINBA_TABLE_REPORT10:
			HANDLE_REPORT(10);
			break;
		case PINBA_TABLE_REPORT11:
			HANDLE_REPORT(11);
			break;
		case PINBA_TABLE_REPORT12:
			HANDLE_REPORT(12);
			break;
		case PINBA_TABLE_TAG_INFO:
			{
				pinba_tag_report *report;

				pthread_rwlock_rdlock(&D->collector_lock);
				pthread_rwlock_wrlock(&D->tag_reports_lock);
				pthread_rwlock_rdlock(&D->timer_lock);
				report = pinba_get_tag_report(PINBA_TABLE_TAG_INFO, share);

				if (!report) {
					report = pinba_regenerate_tag_info(share);
				}
				
				stats.records = 0;
				if (report) {
					stats.records = report->results_cnt;
				}
				pthread_rwlock_unlock(&D->timer_lock);
				pthread_rwlock_unlock(&D->tag_reports_lock);
				pthread_rwlock_unlock(&D->collector_lock);
			}
			break;
		case PINBA_TABLE_TAG2_INFO:
			{
				pinba_tag_report *report;

				pthread_rwlock_rdlock(&D->collector_lock);
				pthread_rwlock_wrlock(&D->tag_reports_lock);
				pthread_rwlock_rdlock(&D->timer_lock);
				report = pinba_get_tag_report(PINBA_TABLE_TAG2_INFO, share);
		
				if (!report) {
					report = pinba_regenerate_tag2_info(share);
				}

				stats.records = 0;
				if (report) {
					stats.records = report->results_cnt;
				}
				pthread_rwlock_unlock(&D->timer_lock);
				pthread_rwlock_unlock(&D->tag_reports_lock);
				pthread_rwlock_unlock(&D->collector_lock);
			}
			break;
		case PINBA_TABLE_TAG_REPORT:
			{
				pinba_tag_report *report;
				
				pthread_rwlock_rdlock(&D->collector_lock);
				pthread_rwlock_wrlock(&D->tag_reports_lock);
				pthread_rwlock_rdlock(&D->timer_lock);
				report = pinba_get_tag_report(PINBA_TABLE_TAG_REPORT, share);
		
				if (!report) {
					report = pinba_regenerate_tag_report(share);
				}

				stats.records = 0;
				if (report) {
					stats.records = report->results_cnt;
				}
				pthread_rwlock_unlock(&D->timer_lock);
				pthread_rwlock_unlock(&D->tag_reports_lock);
				pthread_rwlock_unlock(&D->collector_lock);
			}
			break;
		case PINBA_TABLE_TAG2_REPORT:
			{
				pinba_tag_report *report;

				pthread_rwlock_rdlock(&D->collector_lock);
				pthread_rwlock_wrlock(&D->tag_reports_lock);
				pthread_rwlock_rdlock(&D->timer_lock);
				report = pinba_get_tag_report(PINBA_TABLE_TAG2_REPORT, share);
		
				if (!report) {
					report = pinba_regenerate_tag2_report(share);
				}

				stats.records = 0;
				if (report) {
					stats.records = report->results_cnt;
				}
				pthread_rwlock_unlock(&D->timer_lock);
				pthread_rwlock_unlock(&D->tag_reports_lock);
				pthread_rwlock_unlock(&D->collector_lock);
			}
			break;
		case PINBA_TABLE_TAG_REPORT2:
			{
				pinba_tag_report *report;
				pthread_rwlock_rdlock(&D->collector_lock);
				pthread_rwlock_wrlock(&D->tag_reports_lock);
				pthread_rwlock_rdlock(&D->timer_lock);
				report = pinba_get_tag_report(PINBA_TABLE_TAG_REPORT2, share);

				if (!report) {
					report = pinba_regenerate_tag_report2(share);
				}

				stats.records = 0;
				if (report) {
					stats.records = report->results_cnt;
				}
				pthread_rwlock_unlock(&D->timer_lock);
				pthread_rwlock_unlock(&D->tag_reports_lock);
				pthread_rwlock_unlock(&D->collector_lock);
			}
			break;
		case PINBA_TABLE_TAG2_REPORT2:
			{
				pinba_tag_report *report;
				pthread_rwlock_rdlock(&D->collector_lock);
				pthread_rwlock_wrlock(&D->tag_reports_lock);
				pthread_rwlock_rdlock(&D->timer_lock);
				report = pinba_get_tag_report(PINBA_TABLE_TAG2_REPORT2, share);

				if (!report) {
					report = pinba_regenerate_tag2_report2(share);
				}

				stats.records = 0;
				if (report) {
					stats.records = report->results_cnt;
				}
				pthread_rwlock_unlock(&D->timer_lock);
				pthread_rwlock_unlock(&D->tag_reports_lock);
				pthread_rwlock_unlock(&D->collector_lock);
			}
			break;
		default:
			stats.records = 2; /* dummy */
			break;
	}
	DBUG_RETURN(0);
}
/* }}} */

THR_LOCK_DATA **ha_pinba::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type) /* {{{ */
{
	if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK) {
		lock.type = lock_type;
	}
	*to++ = &lock;
	return to;
}
/* }}} */

/* conf variables {{{ */

static MYSQL_SYSVAR_INT(port, 
  port_var,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "UDP port to listen at", 
  NULL, 
  NULL, 
  30002,
  0,
  65536,
  0);

static MYSQL_SYSVAR_STR(address, 
  address_var,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "IP address to listen at (leave it empty if you want to listen at any IP)", 
  NULL, 
  NULL, 
  NULL);

static MYSQL_SYSVAR_INT(temp_pool_size, 
  temp_pool_size_var,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Temporary pool size", 
  NULL, 
  NULL, 
  10000,
  10,
  INT_MAX,
  0);

static MYSQL_SYSVAR_INT(request_pool_size, 
  request_pool_size_var,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Request pool size", 
  NULL, 
  NULL, 
  1000000,
  10,
  INT_MAX,
  0);

static MYSQL_SYSVAR_INT(stats_history, 
  stats_history_var,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Request stats history (seconds)", 
  NULL, 
  NULL, 
  900, /* 15 * 60 sec */
  1,
  INT_MAX,
  0);

static MYSQL_SYSVAR_INT(stats_gathering_period, 
  stats_gathering_period_var,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Request stats gathering period (microseconds)",
  NULL, 
  NULL, 
  10000,
  10,
  INT_MAX,
  0);

static MYSQL_SYSVAR_INT(tag_report_timeout, 
  tag_report_timeout_var,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Tag report timeout (in seconds)", 
  NULL, 
  NULL, 
  600,
  -1,
  INT_MAX,
  0);

static MYSQL_SYSVAR_BOOL(show_protobuf_errors, 
  show_protobuf_errors_var,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Show protobuf errors and warnings", 
  NULL, 
  NULL, 
  FALSE);

static MYSQL_SYSVAR_INT(cpu_start, 
  cpu_start_var,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Set CPU affinity offset", 
  NULL, 
  NULL, 
  0,
  0,
  INT_MAX,
  0);

static struct st_mysql_sys_var* system_variables[]= {
	MYSQL_SYSVAR(port),
	MYSQL_SYSVAR(address),
	MYSQL_SYSVAR(temp_pool_size),
	MYSQL_SYSVAR(request_pool_size),
	MYSQL_SYSVAR(stats_history),
	MYSQL_SYSVAR(stats_gathering_period),
	MYSQL_SYSVAR(tag_report_timeout),
	MYSQL_SYSVAR(show_protobuf_errors),
	MYSQL_SYSVAR(cpu_start),
	NULL
};
/* }}} */

struct st_mysql_storage_engine pinba_storage_engine =
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

mysql_declare_plugin(pinba) /* {{{ */
{
	MYSQL_STORAGE_ENGINE_PLUGIN,
	&pinba_storage_engine,
	"PINBA",
	"Antony Dovgal",
	"Pinba engine",
	PLUGIN_LICENSE_GPL,
	pinba_engine_init,          /* Plugin Init */
	pinba_engine_shutdown,      /* Plugin Deinit */
	0x0100, /* VERSION 1.0 */
	NULL,                       /* status variables                */
	system_variables,           /* system variables                */
	NULL                        /* config options                  */
}
mysql_declare_plugin_end;
/* }}} */

/* 
 * vim600: sw=4 ts=4 fdm=marker
 */
