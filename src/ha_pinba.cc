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
#include <my_pthread.h>

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
static int data_pool_size_var = 0;
static int temp_pool_size_var = 0;
static int temp_pool_size_limit_var = 0;
static int request_pool_size_var = 0;
static int stats_history_var = 0;
static int stats_gathering_period_var = 0;
static int cpu_start_var = 0;
static int histogram_max_time_var = 0;

/* global daemon struct, created once per process and used everywhere */
pinba_daemon *D;

/* prototypes */
static handler* pinba_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root);
static void pinba_share_destroy(PINBA_SHARE *share);

/* Variables for pinba share methods */
static HASH pinba_open_tables; // Hash used to track open tables
pthread_mutex_t pinba_mutex;   // This is the mutex we use to init the hash

/* <utilities> {{{ */

#define SET_IF_PASSED(_tag_report_, _value_)	\
	if (_tag_report_) {							\
		*_tag_report_ = _value_;				\
	}

static inline unsigned char pinba_get_table_type(char *str, size_t len, int *tag_report) /* {{{ */
{
	char *colon;

	SET_IF_PASSED(tag_report, 0);

	if (!str || !len) {
		return PINBA_TABLE_UNKNOWN;
	}

	colon = strchr(str, ':');
	if (colon) {
		/* ignore params */
		len = colon - str;
	}

	if (len > 3 && memcmp(str, "hv.", 3) == 0) {
		return PINBA_TABLE_HISTOGRAM_VIEW;
	}

	switch(len) {
		case 12: /* sizeof("tag2_report2") */
			if (!memcmp(str, "tag2_report2", len)) {
				SET_IF_PASSED(tag_report, 1);
				return PINBA_TABLE_TAG2_REPORT2;
			}
			if (!memcmp(str, "tagN_report2", len)) {
				SET_IF_PASSED(tag_report, 1);
				return PINBA_TABLE_TAGN_REPORT2;
			}
			break;
		case 11: /* sizeof("tag2_report") */
			if (!memcmp(str, "tag2_report", len)) {
				SET_IF_PASSED(tag_report, 1);
				return PINBA_TABLE_TAG2_REPORT;
			}
			if (!memcmp(str, "tag_report2", len)) {
				SET_IF_PASSED(tag_report, 1);
				return PINBA_TABLE_TAG_REPORT2;
			}
			if (!memcmp(str, "tagN_report", len)) {
				SET_IF_PASSED(tag_report, 1);
				return PINBA_TABLE_TAGN_REPORT;
			}
			break;
		case 10: /* sizeof("tag_report") - 1 */
			if (!memcmp(str, "tag_report", len)) {
				SET_IF_PASSED(tag_report, 1);
				return PINBA_TABLE_TAG_REPORT;
			}
			break;
		case 9: /* sizeof("tag2_info") - 1 */
			if (!memcmp(str, "tag2_info", len)) {
				SET_IF_PASSED(tag_report, 1);
				return PINBA_TABLE_TAG2_INFO;
			}
			if (!memcmp(str, "tagN_info", len)) {
				SET_IF_PASSED(tag_report, 1);
				return PINBA_TABLE_TAGN_INFO;
			}
		case 8: /* sizeof("timertag") - 1 */
			if (!memcmp(str, "timertag", len)) {
				SET_IF_PASSED(tag_report, 1);
				return PINBA_TABLE_TIMERTAG;
			}
			if (!memcmp(str, "tag_info", len)) {
				SET_IF_PASSED(tag_report, 1);
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
			if (!memcmp(str, "report13", len)) {
				return PINBA_TABLE_REPORT13;
			}
			if (!memcmp(str, "report14", len)) {
				return PINBA_TABLE_REPORT14;
			}
			if (!memcmp(str, "report15", len)) {
				return PINBA_TABLE_REPORT15;
			}
			if (!memcmp(str, "report16", len)) {
				return PINBA_TABLE_REPORT16;
			}
			if (!memcmp(str, "report17", len)) {
				return PINBA_TABLE_REPORT17;
			}
			if (!memcmp(str, "report18", len)) {
				return PINBA_TABLE_REPORT18;
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
		case 6:
			if (!memcmp(str, "status", len)) {
				return PINBA_TABLE_STATUS;
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

static inline int pinba_parse_params(TABLE *table, unsigned char type, PINBA_SHARE *share) /* {{{ */
{
	char *str_copy, *comma, *p, *equal, *end;
	char *colon[3];
	size_t len;
	int num = 0, c_num = 0;
	int parse_only = 0, tag_report;
	unsigned char tmp_hv_type;

	if (type == PINBA_TABLE_HISTOGRAM_VIEW) {
		len = table->s->comment.length - 3;
		str_copy = strdup(table->s->comment.str + 3);

		tmp_hv_type = pinba_get_table_type(str_copy, len, &tag_report);
		if (tmp_hv_type == PINBA_TABLE_UNKNOWN) {
			free(str_copy);
			return -1;
		}

		if (share) {
			share->hv_table_type = tmp_hv_type;
		}
	} else {
		len = table->s->comment.length;
		str_copy = strdup(table->s->comment.str);
	}

	if (!share) {
		parse_only = 1;
	} else {
		share->tag_report = tag_report;
	}

	colon[0] = strchr(str_copy, ':');
	if (!colon[0]) {
		/* no params */
		free(str_copy);
		return 0;
	}

	colon[0]++; /* skip the colon */
	if (colon[0][0] == '\0') {
		/* colon was the last character */
		free(str_copy);
		return -1;
	}

	colon[1] = strchr(colon[0], ':');
	if (colon[1]) {
		*colon[1] = '\0';
	}

	/* parameters are strings separated by commas: "<report_name>:param1,param2" */

	comma = strchr(colon[0], ',');
	if (!comma) {
		if (strlen(colon[0]) > 0) {
			if (!parse_only) {
				share->params = (char **)realloc(share->params, (num + 1) * sizeof(char *));
				share->params[num] = strdup(colon[0]);
			}
			num++;
		}
	} else {
		p = colon[0];
		if (colon[1]) {
			end = colon[1];
		} else {
			end = str_copy + len;
		}
		do {
			if ((comma - p) > 0) {
				if (!parse_only) {
					share->params = (char **)realloc(share->params, (num + 1) * sizeof(char *));
					share->params[num] = strndup(p, comma - p);
				}
				p = comma + 1;
				num++;
			} else {
				num = -1;
				goto out;
			}
		} while (p < end && (comma = strchr(p, ',')) != NULL);

		if (!parse_only && p < end) {
			share->params = (char **)realloc(share->params, (num + 1) * sizeof(char *));
			share->params[num] = strdup(p);
			num++;
		}
	}

	if (!parse_only) {
		share->params_num = num;
	}

	if (!colon[1]) {
		free(str_copy);
		return num;
	}

	colon[2] = strchr(colon[1] + 1, ':');
	if (colon[2]) {
		*colon[2] = '\0';
	}

	/* conditions are values separated by commas: "<report_name>:<params>:cond1=value1,cond2=value2" */

	if (colon[1]) {

		p = colon[1] + 1;
		if (colon[2]) {
			end = colon[2];
		} else {
			end = str_copy + len;
		}

		/* there are some conditions in the comment */
		comma = strchr(p, ',');
		if (p < end) {
			do {
				equal = strchr(p, '=');
				if (!equal) {
					num = -1;
					goto out;
				}
				if (!parse_only) {
					share->cond_names = (char **)realloc(share->cond_names, (c_num + 1) * sizeof(char *));
					share->cond_names[c_num] = strndup(p, equal - p);
					share->cond_values = (char **)realloc(share->cond_values, (c_num + 1) * sizeof(char *));
					share->cond_values[c_num] = strndup(equal + 1, comma - equal - 1);
					c_num++;
				}
				p = comma ? comma + 1 : end;
				comma = strchr(p, ',');
			} while (p < end);
		}
	}

	if (!parse_only) {
		share->cond_num = c_num;
	}

	/* percentiles are just ints separated by commas: <report_name>:<params>:<conditions>:75,25 */

	if (colon[2]) {
		int value;
		p = colon[2] + 1;
		end = str_copy + len;

		if (p < end) {
			comma = strchr(p, ',');
			do {
				if (comma) {
					*comma = '\0';
				}

				value = atoi(p);
				if (value <= 0 || value > 100) {
					num = -1;
					goto out;
				}

				if (!parse_only) {
					share->percentiles = (int *)realloc(share->percentiles, (share->percentiles_num + 1) * sizeof(int));
					share->percentiles[share->percentiles_num] = value;
					share->percentiles_num++;
				}
				p = comma ? comma + 1 : end;
				comma = strchr(p, ',');
			} while (p < end);
		}
	}

out:

	free(str_copy);
	return num;
}
/* }}} */

#define PINBA_TAG_PARAM_PREFIX "tag."
#define PINBA_TAG_PARAM_PREFIX_LEN strlen(PINBA_TAG_PARAM_PREFIX)

static inline int pinba_parse_conditions(PINBA_SHARE *share, pinba_std_report *report) /* {{{ */
{
	int i;

	report->histogram_max_time = histogram_max_time_var;
	report->histogram_segment = (float)histogram_max_time_var/(float)PINBA_HISTOGRAM_SIZE;

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
		} else if (strcmp(share->cond_names[i], "histogram_max_time") == 0) {
			report->histogram_max_time = strtod(share->cond_values[i], NULL);
			report->histogram_segment = (float)report->histogram_max_time/(float)PINBA_HISTOGRAM_SIZE;
		} else if (strlen(share->cond_names[i]) > PINBA_TAG_PARAM_PREFIX_LEN && memcmp(share->cond_names[i], PINBA_TAG_PARAM_PREFIX, PINBA_TAG_PARAM_PREFIX_LEN) == 0) {
			/* found a tag */
			report->flags |= PINBA_REPORT_TAGGED;
			report->cond.tags_cnt++;
			report->cond.tag_names = (char **)realloc(report->cond.tag_names, report->cond.tags_cnt * sizeof(char *));
			report->cond.tag_names[report->cond.tags_cnt - 1] = strndup(share->cond_names[i] + PINBA_TAG_PARAM_PREFIX_LEN /* cut off the prefix */, PINBA_TAG_NAME_SIZE - 1);
			report->cond.tag_values = (char **)realloc(report->cond.tag_values, report->cond.tags_cnt * sizeof(char *));
			report->cond.tag_values[report->cond.tags_cnt - 1] = strndup(share->cond_values[i], PINBA_TAG_VALUE_SIZE - 1);
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
	settings.data_pool_size = data_pool_size_var ? data_pool_size_var : temp_pool_size_var;
	settings.temp_pool_size = temp_pool_size_var;

	/* default value of temp_pool_size_limit is temp_pool_size * 10 */
	if (!temp_pool_size_limit_var || temp_pool_size_limit_var < temp_pool_size_var) {
		settings.temp_pool_size_limit = temp_pool_size_var * 10;
	} else {
		settings.temp_pool_size_limit = temp_pool_size_limit_var;
	}
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
	DBUG_ENTER("pinba_engine_shutdown");

	pthread_cancel(collector_thread);
	pthread_join(collector_thread, NULL);

	pthread_cancel(data_thread);
	pthread_join(data_thread, NULL);

	pthread_cancel(stats_thread);
	pthread_join(stats_thread, NULL);

	pinba_collector_shutdown();

	hash_free(&pinba_open_tables);
	pthread_mutex_destroy(&pinba_mutex);

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
		index->str.val[index->str.len] = '\0';
	} else {
		index->str.val = NULL;
	}
}
/* }}} */

static inline int pinba_tags_to_string(char **tag_names, char **tag_values, int tags_cnt, char **str, int *str_len) /* {{{ */
{
	int i;

	if (!tags_cnt) {
		return 0;
	}

	*str_len = 0;
	for (i = 0; i < tags_cnt; i++) {
		*str_len += strlen(tag_names[i]) + strlen("=") + strlen(tag_values[i]) + strlen(",");
	}

	(*str) = (char *)malloc(*str_len + 1);
	*str_len = 0;
	for (i = 0; i < tags_cnt; i++) {
		*str_len += sprintf(*str + *str_len, "%s=%s,", tag_names[i], tag_values[i]);
	}

	*str_len -= 1; /* cut off the last comma */

	(*str)[*str_len] = '\0';
	return 1;
}
/* }}} */

static inline float pinba_histogram_value(pinba_std_report *report, int *data, unsigned int percent_value) /* {{{ */
{
	unsigned int i, num;
	float rem;

	if (!percent_value) {
		percent_value = 1;
	}

	num = 0;
	for (i = 0; i < PINBA_HISTOGRAM_SIZE; i++) {
		num += *(data + i);

		if (num >= percent_value) {
			rem = 1 - (((float)num - (float)percent_value) / (float)*(data + i));
			return report->histogram_segment * ((float)i + rem);
		}
	}
	/* check for empty report here */
	if (!num) {
		return 0;
	}
	return report->histogram_segment * PINBA_HISTOGRAM_SIZE;
}
/* }}} */

/* </utilities> }}} */

/* <reports> {{{ */

static inline void pinba_get_tag_report_id(PINBA_SHARE *share) /* {{{ */
{
	int len, i;
	unsigned char type;

	type = share->table_type;
	if (share->table_type == PINBA_TABLE_HISTOGRAM_VIEW) {
		type = share->hv_table_type;
	}

	len = sprintf((char *)share->index, "%d", type);
	for (i = 0; i < share->params_num; i++) {
		len += snprintf((char *)share->index + len, sizeof(share->index) - len, "|%s", share->params[i]);
	}

	if (share->cond_num) {
		int i;
		for (i = 0; i < share->cond_num; i++) {
			len += sprintf((char *)share->index + len, "|%s=%s", share->cond_names[i], share->cond_values[i]);
		}
	}
}
/* }}} */

static inline void pinba_get_report_id(PINBA_SHARE *share) /* {{{ */
{
	int len;
	unsigned char type;

	type = share->table_type;
	if (share->table_type == PINBA_TABLE_HISTOGRAM_VIEW) {
		type = share->hv_table_type;
	}

	len = sprintf((char *)share->index, "%d", type);

	if (share->cond_num) {
		int i;
		for (i = 0; i < share->cond_num; i++) {
			len += sprintf((char *)share->index + len, "|%s=%s", share->cond_names[i], share->cond_values[i]);
		}
	}
}
/* }}} */

static inline pinba_tag_report *pinba_get_tag_report(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;

	if (share->index[0] == '\0') {
		pinba_get_tag_report_id(share);
	}

	ppvalue = JudySLGet(D->tag_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		return NULL;
	}
	return (pinba_tag_report *)*ppvalue;
}
/* }}} */

static inline pinba_report *pinba_get_report(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;

	if (share->index[0] == '\0') {
		pinba_get_report_id(share);
	}

	ppvalue = JudySLGet(D->base_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		return NULL;
	}
	return (pinba_report *)*ppvalue;
}
/* }}} */

#include "pinba_regenerate_report.h"

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
		pinba_get_tag_report_id(share);
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

		report->tag_id = (int *)malloc(sizeof(int));
		if (!report->tag_id) {
			free(report);
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_TAG_INFO;
		report->time_interval = 1;
		report->results_cnt = 0;
		report->results = NULL;
		report->tag_id[0] = tag->id;
		report->tag_cnt = 1;
		report->add_func = pinba_update_tag_info_add;
		report->delete_func = pinba_update_tag_info_delete;
		pthread_rwlock_init(&report->lock, 0);

		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->tag_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			pinba_std_report_dtor(report);
			free(report->tag_id);
			free(report);
			return NULL;
		}

		if (pinba_tag_reports_array_add(report) < 0) {
			JudySLDel(&D->tag_reports, share->index, NULL);
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			pinba_std_report_dtor(report);
			free(report->tag_id);
			free(report);
			return NULL;
		}
		*ppvalue = report;
	} else {
		report = (pinba_tag_report *)*ppvalue;
		return report;
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (!record->timers_cnt) {
			continue;
		}

		CHECK_REPORT_CONDITIONS_CONTINUE(report, record);

		for (j = 0; j < record->timers_cnt; j++) {
			tag_found = 0;
			timer = record_get_timer(&D->timer_pool, record, j);

			for (k = 0; k < timer->tag_num; k++) {
				if (report->tag_id[0] == timer->tag_ids[k]) {
					/* ok, timer has this tag */
					tag_found = 1;
					break;
				}
			}

			if (!tag_found) {
				/* tag not found in this timer */
				continue;
			}

			word = (pinba_word *)timer->tag_values[k];
			ppvalue = JudySLGet(report->results, (uint8_t *)word->str, NULL);

			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {

				ppvalue = JudySLIns(&report->results, (uint8_t *)word->str, NULL);
				if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
					continue;
				}

				data = (struct pinba_tag_info_data *)calloc(1, sizeof(struct pinba_tag_info_data));
				if (UNLIKELY(!data)) {
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
			timeradd(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
			timeradd(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
			PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

			/* count tag values only once per request */
			if ((int)i != data->prev_add_request_id) {
				data->req_count++;
				data->prev_add_request_id = i;
			}
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
		pinba_get_tag_report_id(share);
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

		report->tag_id = (int *)malloc(sizeof(int) * 2);
		if (!report->tag_id) {
			free(report);
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_TAG2_INFO;
		report->time_interval = 1;
		report->results_cnt = 0;
		report->results = NULL;
		report->tag_id[0] = tag1->id;
		report->tag_id[1] = tag2->id;
		report->tag_cnt = 2;
		report->add_func = pinba_update_tag2_info_add;
		report->delete_func = pinba_update_tag2_info_delete;
		pthread_rwlock_init(&report->lock, 0);

		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->tag_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			pinba_std_report_dtor(report);
			free(report->tag_id);
			free(report);
			return NULL;
		}

		if (pinba_tag_reports_array_add(report) < 0) {
			JudySLDel(&D->tag_reports, share->index, NULL);
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			pinba_std_report_dtor(report);
			free(report->tag_id);
			free(report);
			return NULL;
		}
		*ppvalue = report;
	} else {
		report = (pinba_tag_report *)*ppvalue;
		return report;
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (!record->timers_cnt) {
			continue;
		}

		CHECK_REPORT_CONDITIONS_CONTINUE(report, record);

		for (j = 0; j < record->timers_cnt; j++) {
			tag1_pos = tag2_pos = -1;
			timer = record_get_timer(&D->timer_pool, record, j);

			for (k = 0; k < timer->tag_num; k++) {
				if (report->tag_id[0] == timer->tag_ids[k]) {
					/* ok, timer has this tag */
					tag1_pos = k;
					continue;
				}
				if (report->tag_id[1] == timer->tag_ids[k]) {
					/* ok, timer has this tag */
					tag2_pos = k;
					continue;
				}
			}

			if (tag1_pos == -1 || tag2_pos == -1) {
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
				if (UNLIKELY(!data)) {
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
			timeradd(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
			timeradd(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
			PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

			/* count tag values only once per request */
			if ((int)i != data->prev_add_request_id) {
				data->req_count++;
				data->prev_add_request_id = i;
			}
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
		pinba_get_tag_report_id(share);
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

		report->tag_id = (int *)malloc(sizeof(int));
		if (!report->tag_id) {
			free(report);
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_TAG_REPORT;
		report->time_interval = 1;
		report->results_cnt = 0;
		report->results = NULL;
		report->tag_id[0] = tag->id;
		report->tag_cnt = 1;
		report->add_func = pinba_update_tag_report_add;
		report->delete_func = pinba_update_tag_report_delete;
		pthread_rwlock_init(&report->lock, 0);

		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->tag_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			pinba_std_report_dtor(report);
			free(report->tag_id);
			free(report);
			return NULL;
		}

		if (pinba_tag_reports_array_add(report) < 0) {
			JudySLDel(&D->tag_reports, share->index, NULL);
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			pinba_std_report_dtor(report);
			free(report->tag_id);
			free(report);
			return NULL;
		}
		*ppvalue = report;
	} else {
		report = (pinba_tag_report *)*ppvalue;
		return report;
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (!record->timers_cnt) {
			continue;
		}

		CHECK_REPORT_CONDITIONS_CONTINUE(report, record);

		ppvalue_script = NULL;

		for (j = 0; j < record->timers_cnt; j++) {
			tag_found = 0;
			timer = record_get_timer(&D->timer_pool, record, j);

			for (k = 0; k < timer->tag_num; k++) {
				if (report->tag_id[0] == timer->tag_ids[k]) {
					/* ok, timer has this tag */
					tag_found = 1;
					break;
				}
			}

			if (!tag_found) {
				/* tag not found in this timer */
				continue;
			}

			word = (pinba_word *)timer->tag_values[k];

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
				if (UNLIKELY(!data)) {
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
			timeradd(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
			timeradd(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
			PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

			/* count tag values only once per request */
			if ((int)i != data->prev_add_request_id) {
				data->req_count++;
				data->prev_add_request_id = i;
			}
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
		pinba_get_tag_report_id(share);
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

		report->tag_id = (int *)malloc(sizeof(int) * 2);
		if (!report->tag_id) {
			free(report);
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_TAG2_REPORT;
		report->time_interval = 1;
		report->results_cnt = 0;
		report->results = NULL;
		report->tag_id[0] = tag1->id;
		report->tag_id[1] = tag2->id;
		report->tag_cnt = 2;
		report->add_func = pinba_update_tag2_report_add;
		report->delete_func = pinba_update_tag2_report_delete;
		pthread_rwlock_init(&report->lock, 0);

		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->tag_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			pinba_std_report_dtor(report);
			free(report->tag_id);
			free(report);
			return NULL;
		}

		if (pinba_tag_reports_array_add(report) < 0) {
			JudySLDel(&D->tag_reports, share->index, NULL);
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			pinba_std_report_dtor(report);
			free(report->tag_id);
			free(report);
			return NULL;
		}
		*ppvalue = report;
	} else {
		report = (pinba_tag_report *)*ppvalue;
		return report;
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (!record->timers_cnt) {
			continue;
		}

		CHECK_REPORT_CONDITIONS_CONTINUE(report, record);

		ppvalue_script = NULL;

		for (j = 0; j < record->timers_cnt; j++) {
			tag1_pos = tag2_pos = -1;
			timer = record_get_timer(&D->timer_pool, record, j);

			for (k = 0; k < timer->tag_num; k++) {
				if (report->tag_id[0] == timer->tag_ids[k]) {
					/* ok, timer has this tag */
					tag1_pos = k;
					continue;
				}
				if (report->tag_id[1] == timer->tag_ids[k]) {
					/* ok, timer has this tag */
					tag2_pos = k;
					continue;
				}
			}

			if (tag1_pos == -1 || tag2_pos == -1) {
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
			timeradd(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
			timeradd(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
			PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

			/* count tag values only once per request */
			if ((int)i != data->prev_add_request_id) {
				data->req_count++;
				data->prev_add_request_id = i;
			}
		}
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* tag report2 */
static inline pinba_tag_report *pinba_regenerate_tag_report2(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue, ppvalue_script;
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
		pinba_get_tag_report_id(share);
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

		report->tag_id = (int *)malloc(sizeof(int));
		if (!report->tag_id) {
			free(report);
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_TAG_REPORT2;
		report->time_interval = 1;
		report->results_cnt = 0;
		report->results = NULL;
		report->tag_id[0] = tag->id;
		report->tag_cnt = 1;
		report->add_func = pinba_update_tag_report2_add;
		report->delete_func = pinba_update_tag_report2_delete;
		pthread_rwlock_init(&report->lock, 0);

		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->tag_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			pinba_std_report_dtor(report);
			free(report->tag_id);
			free(report);
			return NULL;
		}

		if (pinba_tag_reports_array_add(report) < 0) {
			JudySLDel(&D->tag_reports, share->index, NULL);
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			pinba_std_report_dtor(report);
			free(report->tag_id);
			free(report);
			return NULL;
		}
		*ppvalue = report;
	} else {
		report = (pinba_tag_report *)*ppvalue;
		return report;
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (!record->timers_cnt) {
			continue;
		}

		CHECK_REPORT_CONDITIONS_CONTINUE(report, record);

		ppvalue_script = NULL;

		for (j = 0; j < record->timers_cnt; j++) {
			tag_found = 0;
			timer = record_get_timer(&D->timer_pool, record, j);

			for (k = 0; k < timer->tag_num; k++) {
				if (report->tag_id[0] == timer->tag_ids[k]) {
					/* ok, timer has this tag */
					tag_found = 1;
					break;
				}
			}

			if (!tag_found) {
				/* tag not found in this timer */
				continue;
			}

			word = (pinba_word *)timer->tag_values[k];

			memcpy_static(index_val, record->data.hostname, record->data.hostname_len, index_len);
			index_val[index_len] = '|'; index_len++;
			memcat_static(index_val, index_len, record->data.server_name, record->data.server_name_len, index_len);
			index_val[index_len] = '|'; index_len++;
			memcat_static(index_val, index_len, word->str, word->len, index_len);

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

				data = (struct pinba_tag_report2_data *)calloc(1, sizeof(struct pinba_tag_report2_data));
				if (UNLIKELY(!data)) {
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
			timeradd(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
			timeradd(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
			PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

			/* count tag values only once per request */
			if ((int)i != data->prev_add_request_id) {
				data->req_count++;
				data->prev_add_request_id = i;
			}
		}
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* tag2 report2 */
static inline pinba_tag_report *pinba_regenerate_tag2_report2(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue, ppvalue_script;
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
		pinba_get_tag_report_id(share);
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

		report->tag_id = (int *)malloc(sizeof(int) * 2);
		if (!report->tag_id) {
			free(report);
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_TAG2_REPORT2;
		report->time_interval = 1;
		report->results_cnt = 0;
		report->results = NULL;
		report->tag_id[0] = tag1->id;
		report->tag_id[1] = tag2->id;
		report->tag_cnt = 2;
		report->add_func = pinba_update_tag2_report2_add;
		report->delete_func = pinba_update_tag2_report2_delete;
		pthread_rwlock_init(&report->lock, 0);

		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->tag_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			pinba_std_report_dtor(report);
			free(report->tag_id);
			free(report);
			return NULL;
		}

		if (pinba_tag_reports_array_add(report) < 0) {
			JudySLDel(&D->tag_reports, share->index, NULL);
			pthread_rwlock_unlock(&report->lock);
			pthread_rwlock_destroy(&report->lock);
			pinba_std_report_dtor(report);
			free(report->tag_id);
			free(report);
			return NULL;
		}
		*ppvalue = report;
	} else {
		report = (pinba_tag_report *)*ppvalue;
		return report;
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (!record->timers_cnt) {
			continue;
		}

		CHECK_REPORT_CONDITIONS_CONTINUE(report, record);

		ppvalue_script = NULL;

		for (j = 0; j < record->timers_cnt; j++) {
			tag1_pos = tag2_pos = -1;
			timer = record_get_timer(&D->timer_pool, record, j);

			for (k = 0; k < timer->tag_num; k++) {
				if (report->tag_id[0] == timer->tag_ids[k]) {
					/* ok, timer has this tag */
					tag1_pos = k;
					continue;
				}
				if (report->tag_id[1] == timer->tag_ids[k]) {
					/* ok, timer has this tag */
					tag2_pos = k;
					continue;
				}
			}

			if (tag1_pos == -1 || tag2_pos == -1) {
				continue;
			}

			word1 = (pinba_word *)timer->tag_values[tag1_pos];
			word2 = (pinba_word *)timer->tag_values[tag2_pos];

			memcpy_static(index_val, record->data.hostname, record->data.hostname_len, index_len);
			index_val[index_len] = '|'; index_len++;
			memcat_static(index_val, index_len, record->data.server_name, record->data.server_name_len, index_len);
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
			timeradd(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
			timeradd(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
			PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

			/* count tag values only once per request */
			if ((int)i != data->prev_add_request_id) {
				data->req_count++;
				data->prev_add_request_id = i;
			}
		}
	}
	pthread_rwlock_unlock(&report->lock);
	return report;
}
/* }}} */

/* tagN_info */
static inline pinba_tag_report *pinba_regenerate_tagN_info(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue;
	pinba_tag_report *report;
	int index_len, dummy, k, *tag_id = NULL;
	pinba_timer_record *timer;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	struct pinba_tagN_info_data *data;
	unsigned int i, j;
	pinba_word *word, **words;

	if (share->index[0] == '\0') {
		pinba_get_tag_report_id(share);
	}

	ppvalue = JudySLGet(D->tag_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pinba_tag *tag;

		tag_id = (int *)calloc(share->params_num, sizeof(int));
		if (!tag_id) {
			return NULL;
		}

		for (i = 0; i < share->params_num; i++) {
			ppvalue = JudySLGet(D->tag.name_index, (uint8_t *)share->params[i], NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				/* tag not found */
				free(tag_id);
				return NULL;
			}

			tag = (pinba_tag *)*ppvalue;
			tag_id[i] = tag->id;
		}

		report = (pinba_tag_report *)calloc(1, sizeof(pinba_tag_report));
		if (UNLIKELY(!report)) {
			free(tag_id);
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_TAGN_INFO;
		report->time_interval = 1;
		report->results_cnt = 0;
		report->results = NULL;
		report->tag_id = tag_id;
		report->tag_cnt = share->params_num;
		report->add_func = pinba_update_tagN_info_add;
		report->delete_func = pinba_update_tagN_info_delete;
		report->index_len = PINBA_TAG_VALUE_SIZE * share->params_num + share->params_num;
		report->index = (uint8_t *)malloc(report->index_len + 1);
		if (!report->index) {
			free(tag_id);
			free(report);
			return NULL;
		}

		report->words = (pinba_word **)malloc(report->tag_cnt * sizeof(pinba_word *));
		if (!report->words) {
			free(report->index);
			free(tag_id);
			free(report);
			return NULL;
		}

		pthread_rwlock_init(&report->lock, 0);
		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->tag_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			goto cleanup;
		}

		if (pinba_tag_reports_array_add(report) < 0) {
			goto cleanup;
		}
		*ppvalue = report;
	} else {
		report = (pinba_tag_report *)*ppvalue;
		return report;
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (!record->timers_cnt) {
			continue;
		}

		CHECK_REPORT_CONDITIONS_CONTINUE(report, record);

		for (j = 0; j < record->timers_cnt; j++) {
			int found_tag_cnt = 0;

			timer = record_get_timer(&D->timer_pool, record, j);

			for (k = 0; k < timer->tag_num; k++) {
				int h;
				for (h = 0; h < report->tag_cnt; h++) {
					if (report->tag_id[h] == timer->tag_ids[k]) {
						found_tag_cnt++;
						report->words[h] = (pinba_word *)timer->tag_values[k];
						goto continue2;
					}
				}
continue2:
				;
			}

			if (found_tag_cnt != share->params_num) {
				/* not all of the tags were found, skip the record */
				continue;
			}

			index_len = 0;
			for (k = 0; k < share->params_num; k++) {
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

				data->tag_value = (char *)calloc(share->params_num, PINBA_TAG_VALUE_SIZE);
				if (UNLIKELY(!data->tag_value)) {
					free(data);
					continue;
				}

				data->req_count = 1;
				data->hit_count = timer->hit_count;
				data->timer_value = timer->value;
				data->prev_add_request_id = i;
				data->prev_del_request_id = -1;

				for (k = 0; k < share->params_num; k++) {
					word = report->words[k];
					memcpy(data->tag_value + PINBA_TAG_VALUE_SIZE * k, word->str, word->len);
				}

				*ppvalue = data;
				report->results_cnt++;
			} else {
				data = (struct pinba_tagN_info_data *)*ppvalue;
				data->hit_count += timer->hit_count;
				timeradd(&data->timer_value, &timer->value, &data->timer_value);
			}
			timeradd(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
			timeradd(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
			PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

			/* count tag values only once per request */
			if ((int)i != data->prev_add_request_id) {
				data->req_count++;
				data->prev_add_request_id = i;
			}
		}
	}
	pthread_rwlock_unlock(&report->lock);
	return report;

cleanup:
	if (tag_id) {
		free(tag_id);
	}
	JudySLDel(&D->tag_reports, share->index, NULL);
	pthread_rwlock_unlock(&report->lock);
	pthread_rwlock_destroy(&report->lock);
	pinba_std_report_dtor(report);
	free(report->words);
	free(report->index);
	free(report);
	return NULL;
}
/* }}} */

/* tagN_report */
static inline pinba_tag_report *pinba_regenerate_tagN_report(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue, ppvalue_script;
	pinba_tag_report *report;
	int index_len, dummy, k, *tag_id = NULL;
	pinba_timer_record *timer;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	struct pinba_tagN_report_data *data;
	unsigned int i, j;
	pinba_word *word;

	if (share->index[0] == '\0') {
		pinba_get_tag_report_id(share);
	}

	ppvalue = JudySLGet(D->tag_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pinba_tag *tag;

		tag_id = (int *)calloc(share->params_num, sizeof(int));
		if (!tag_id) {
			return NULL;
		}

		for (i = 0; i < share->params_num; i++) {
			ppvalue = JudySLGet(D->tag.name_index, (uint8_t *)share->params[i], NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				/* tag not found */
				free(tag_id);
				return NULL;
			}

			tag = (pinba_tag *)*ppvalue;
			tag_id[i] = tag->id;
		}

		report = (pinba_tag_report *)calloc(1, sizeof(pinba_tag_report));
		if (UNLIKELY(!report)) {
			free(tag_id);
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_TAGN_REPORT;
		report->time_interval = 1;
		report->results_cnt = 0;
		report->results = NULL;
		report->tag_id = tag_id;
		report->tag_cnt = share->params_num;
		report->add_func = pinba_update_tagN_report_add;
		report->delete_func = pinba_update_tagN_report_delete;
		report->index_len = PINBA_TAG_VALUE_SIZE * share->params_num + share->params_num;
		report->index = (uint8_t *)malloc(report->index_len + 1);
		if (!report->index) {
			free(tag_id);
			free(report);
			return NULL;
		}

		report->words = (pinba_word **)malloc(report->tag_cnt * sizeof(pinba_word *));
		if (!report->words) {
			free(tag_id);
			free(report->index);
			free(report);
			return NULL;
		}

		pthread_rwlock_init(&report->lock, 0);
		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->tag_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			goto cleanup;
		}

		if (pinba_tag_reports_array_add(report) < 0) {
			goto cleanup;
		}
		*ppvalue = report;
	} else {
		report = (pinba_tag_report *)*ppvalue;
		return report;
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (!record->timers_cnt) {
			continue;
		}

		CHECK_REPORT_CONDITIONS_CONTINUE(report, record);

		ppvalue_script = NULL;

		for (j = 0; j < record->timers_cnt; j++) {
			int found_tag_cnt = 0;

			timer = record_get_timer(&D->timer_pool, record, j);

			for (k = 0; k < timer->tag_num; k++) {
				int h;
				for (h = 0; h < report->tag_cnt; h++) {
					if (report->tag_id[h] == timer->tag_ids[k]) {
						found_tag_cnt++;
						report->words[h] = (pinba_word *)timer->tag_values[k];
						goto continue2;
					}
				}
continue2:
				;
			}

			if (found_tag_cnt != share->params_num) {
				/* not all of the tags were found, skip the record */
				continue;
			}


			index_len = 0;
			for (k = 0; k < share->params_num; k++) {
				word = report->words[k];
				memcpy(report->index + index_len, word->str, word->len);
				index_len += word->len;
				report->index[index_len] = '|';
				index_len ++;
			}
			report->index[index_len] = '\0';

			if (!ppvalue_script) {
				ppvalue_script = JudySLIns(&report->results, (uint8_t *)record->data.script_name, NULL);
				if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
					continue;
				}
			}

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

				data->tag_value = (char *)calloc(share->params_num, PINBA_TAG_VALUE_SIZE);
				if (UNLIKELY(!data->tag_value)) {
					free(data);
					continue;
				}

				data->req_count = 1;
				data->hit_count = timer->hit_count;
				data->timer_value = timer->value;
				data->prev_add_request_id = i;
				data->prev_del_request_id = -1;

				memcpy_static(data->script_name, record->data.script_name, record->data.script_name_len, dummy);
				for (k = 0; k < share->params_num; k++) {
					word = report->words[k];
					memcpy(data->tag_value + PINBA_TAG_VALUE_SIZE * k, word->str, word->len);
				}

				*ppvalue = data;
				report->results_cnt++;
			} else {
				data = (struct pinba_tagN_report_data *)*ppvalue;
				data->hit_count += timer->hit_count;
				timeradd(&data->timer_value, &timer->value, &data->timer_value);
			}
			timeradd(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
			timeradd(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
			PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

			/* count tag values only once per request */
			if ((int)i != data->prev_add_request_id) {
				data->req_count++;
				data->prev_add_request_id = i;
			}
		}
	}
	pthread_rwlock_unlock(&report->lock);
	return report;

cleanup:
	if (tag_id) {
		free(tag_id);
	}
	JudySLDel(&D->tag_reports, share->index, NULL);
	pthread_rwlock_unlock(&report->lock);
	pthread_rwlock_destroy(&report->lock);
	pinba_std_report_dtor(report);
	free(report->words);
	free(report->index);
	free(report);
	return NULL;
}
/* }}} */

/* tagN_report2 */
static inline pinba_tag_report *pinba_regenerate_tagN_report2(PINBA_SHARE *share) /* {{{ */
{
	PPvoid_t ppvalue, ppvalue_script;
	pinba_tag_report *report;
	int index_len, dummy, k, *tag_id = NULL;
	pinba_timer_record *timer;
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	struct pinba_tagN_report2_data *data;
	unsigned int i, j;
	pinba_word *word;

	if (share->index[0] == '\0') {
		pinba_get_tag_report_id(share);
	}

	ppvalue = JudySLGet(D->tag_reports, share->index, NULL);
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		pinba_tag *tag;

		tag_id = (int *)calloc(share->params_num, sizeof(int));
		if (!tag_id) {
			return NULL;
		}

		for (i = 0; i < share->params_num; i++) {
			ppvalue = JudySLGet(D->tag.name_index, (uint8_t *)share->params[i], NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				/* tag not found */
				free(tag_id);
				return NULL;
			}

			tag = (pinba_tag *)*ppvalue;
			tag_id[i] = tag->id;
		}

		report = (pinba_tag_report *)calloc(1, sizeof(pinba_tag_report));
		if (UNLIKELY(!report)) {
			free(tag_id);
			return NULL;
		}

		pinba_parse_conditions(share, (pinba_std_report *)report);

		report->std.type = PINBA_TABLE_TAGN_REPORT2;
		report->time_interval = 1;
		report->results_cnt = 0;
		report->results = NULL;
		report->tag_id = tag_id;
		report->tag_cnt = share->params_num;
		report->add_func = pinba_update_tagN_report2_add;
		report->delete_func = pinba_update_tagN_report2_delete;
		report->index_len = PINBA_TAG_VALUE_SIZE * share->params_num + share->params_num;
		report->index = (uint8_t *)malloc(report->index_len + 1);
		if (!report->index) {
			free(tag_id);
			free(report);
			return NULL;
		}

		report->words = (pinba_word **)malloc(report->tag_cnt * sizeof(pinba_word *));
		if (!report->words) {
			free(tag_id);
			free(report->index);
			free(report);
			return NULL;
		}

		pthread_rwlock_init(&report->lock, 0);
		pthread_rwlock_wrlock(&report->lock);

		ppvalue = JudySLIns(&D->tag_reports, share->index, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			goto cleanup;
		}

		if (pinba_tag_reports_array_add(report) < 0) {
			goto cleanup;
		}
		*ppvalue = report;
	} else {
		report = (pinba_tag_report *)*ppvalue;
		return report;
	}

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (!record->timers_cnt) {
			continue;
		}

		CHECK_REPORT_CONDITIONS_CONTINUE(report, record);

		ppvalue_script = NULL;

		for (j = 0; j < record->timers_cnt; j++) {
			int found_tag_cnt = 0;

			timer = record_get_timer(&D->timer_pool, record, j);

			for (k = 0; k < timer->tag_num; k++) {
				int h;
				for (h = 0; h < report->tag_cnt; h++) {
					if (report->tag_id[h] == timer->tag_ids[k]) {
						found_tag_cnt++;
						report->words[h] = (pinba_word *)timer->tag_values[k];
						goto continue2;
					}
				}
continue2:
				;
			}

			if (found_tag_cnt != share->params_num) {
				/* not all of the tags were found, skip the record */
				continue;
			}

			memcpy(report->index, record->data.hostname, record->data.hostname_len);
			index_len = record->data.hostname_len;
			report->index[index_len] = '|'; index_len++;
			memcpy(report->index + index_len, record->data.server_name, record->data.server_name_len);
			index_len += record->data.server_name_len;
			report->index[index_len] = '|'; index_len++;

			for (k = 0; k < share->params_num; k++) {
				word = report->words[k];
				memcpy(report->index + index_len, word->str, word->len);
				index_len += word->len;
				report->index[index_len] = '|';
				index_len ++;
			}
			report->index[index_len] = '\0';

			if (!ppvalue_script) {
				ppvalue_script = JudySLIns(&report->results, (uint8_t *)record->data.script_name, NULL);
				if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {
					continue;
				}
			}

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

				data->tag_value = (char *)calloc(share->params_num, PINBA_TAG_VALUE_SIZE);
				if (UNLIKELY(!data->tag_value)) {
					free(data);
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
				for (k = 0; k < share->params_num; k++) {
					word = report->words[k];
					memcpy(data->tag_value + PINBA_TAG_VALUE_SIZE * k, word->str, word->len);
				}

				*ppvalue = data;
				report->results_cnt++;
			} else {
				data = (struct pinba_tagN_report2_data *)*ppvalue;
				data->hit_count += timer->hit_count;
				timeradd(&data->timer_value, &timer->value, &data->timer_value);
			}
			timeradd(&data->ru_utime_value, &timer->ru_utime, &data->ru_utime_value);
			timeradd(&data->ru_stime_value, &timer->ru_stime, &data->ru_stime_value);
			PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data->histogram_data, timer->value, timer->hit_count);

			/* count tag values only once per request */
			if ((int)i != data->prev_add_request_id) {
				data->req_count++;
				data->prev_add_request_id = i;
			}
		}
	}
	pthread_rwlock_unlock(&report->lock);
	return report;

cleanup:
	if (tag_id) {
		free(tag_id);
	}
	JudySLDel(&D->tag_reports, share->index, NULL);
	pthread_rwlock_unlock(&report->lock);
	pthread_rwlock_destroy(&report->lock);
	pinba_std_report_dtor(report);
	free(report->words);
	free(report->index);
	free(report);
	return NULL;
}
/* }}} */

/* </reports> }}} */

/* <share functions> {{{ */

static PINBA_SHARE *get_share(const char *table_name, TABLE *table) /* {{{ */
{
	PINBA_SHARE *share;
	uint length;
	char *tmp_name;
	unsigned char type = PINBA_TABLE_UNKNOWN;
	int tag_report;

	pthread_mutex_lock(&pinba_mutex);
	length = (uint)strlen(table_name);

	if (!(share = (PINBA_SHARE*)hash_search(&pinba_open_tables, (unsigned char*) table_name, length))) {

		if (!table->s) {
			pthread_mutex_unlock(&pinba_mutex);
			return NULL;
		}

		type = pinba_get_table_type(table->s->comment.str, table->s->comment.length, &tag_report);
		if (type == PINBA_TABLE_UNKNOWN) {
			pthread_mutex_unlock(&pinba_mutex);
			return NULL;
		}

		if (!my_multi_malloc(MYF(MY_WME | MY_ZEROFILL), &share, sizeof(*share), &tmp_name, length+1, NullS)) {
			pthread_mutex_unlock(&pinba_mutex);
			return NULL;
		}

		if (pinba_parse_params(table, type, share) < 0) {
			goto error;
		}

		share->table_type = type;
		share->use_count = 0;
		share->table_name_length = length;
		share->table_name = tmp_name;
		memcpy(share->table_name, table_name, length);
		share->table_name[length] = '\0';
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
	pinba_share_destroy(share);
	pthread_mutex_unlock(&pinba_mutex);
	pinba_free((unsigned char *) share, MYF(0));

	return NULL;
}
/* }}} */

static void pinba_share_destroy(PINBA_SHARE *share) /* {{{ */
{
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

	if (share->percentiles_num > 0) {
		free(share->percentiles);
		share->percentiles = NULL;
		share->percentiles_num = 0;
	}
}
/* }}} */

static int free_share(PINBA_SHARE *share) /* {{{ */
{
	pthread_mutex_lock(&pinba_mutex);
	if (!--share->use_count) {
		pinba_share_destroy(share);
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

	if (active_index >= PINBA_MAX_KEYS) {
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

	if (active_index >= PINBA_MAX_KEYS) {
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

	if (active_index >= PINBA_MAX_KEYS) {
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

	if (active_index >= PINBA_MAX_KEYS) {
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

	if (active_index >= PINBA_MAX_KEYS) {
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
	int i;

	DBUG_ENTER("ha_pinba::rnd_init");

	for (i = 0; i < PINBA_MAX_KEYS; i++) {
		memset(&this_index[i], 0, sizeof(pinba_index_st));
	}

	switch (share->table_type) {
		case PINBA_TABLE_REQUEST:
		case PINBA_TABLE_TIMER:
		case PINBA_TABLE_TIMERTAG:
			this_index[0].ival = -1;
			this_index[0].position = -1;
			break;
	}

	DBUG_RETURN(0);
}
/* }}} */

int ha_pinba::rnd_end() /* {{{ */
{
	DBUG_ENTER("ha_pinba::rnd_end");

	switch (share->table_type) {
		case PINBA_TABLE_REQUEST:
		case PINBA_TABLE_TAG:
		case PINBA_TABLE_TIMER:
		case PINBA_TABLE_TIMERTAG:
			DBUG_RETURN(0);
	}

	/* Theoretically the number of records in the report may have grown
	   when we were reading it, so we won't reach the end of the array and
	   the index value may leak.
	   Hence this check. */
	if (this_index[0].str.val != NULL) {
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
	}
	if (this_index[0].subindex.val != NULL) {
		free(this_index[0].subindex.val);
		this_index[0].subindex.val = NULL;
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
	DBUG_ENTER("ha_pinba::rnd_pos");

	ret = read_row_by_pos(buf, my_get_ptr(pos, ref_length));
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

	if (active_index >= PINBA_MAX_KEYS) {
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

			ret = requests_fetch_row(buf, this_index[0].ival, &(this_index[0].position), 1);
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

				pthread_rwlock_wrlock(&D->tag_reports_lock);
				pthread_rwlock_rdlock(&D->timer_lock);
				report = pinba_get_tag_report(share);

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

				pthread_rwlock_wrlock(&D->tag_reports_lock);
				pthread_rwlock_rdlock(&D->timer_lock);
				report = pinba_get_tag_report(share);

				if (!report) {
					report = pinba_regenerate_tag2_report(share);
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
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		case PINBA_TABLE_TAG_REPORT2:
			if (active_index == 0) {
				uint8_t index[PINBA_SCRIPT_NAME_SIZE + 1] = {0};
				PPvoid_t ppvalue;
				pinba_tag_report *report;

				pthread_rwlock_wrlock(&D->tag_reports_lock);
				pthread_rwlock_rdlock(&D->timer_lock);
				report = pinba_get_tag_report(share);

				if (!report) {
					report = pinba_regenerate_tag_report2(share);
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
						ret = tag_report2_fetch_row_by_script(buf, this_index[active_index].str.val, this_index[active_index].str.len);
					} else {
						ret = HA_ERR_END_OF_FILE;
					}

					pthread_rwlock_unlock(&report->lock);
				} else {
					ret = HA_ERR_END_OF_FILE;
				}

				pthread_rwlock_unlock(&D->timer_lock);
				pthread_rwlock_unlock(&D->tag_reports_lock);
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		case PINBA_TABLE_TAG2_REPORT2:
			if (active_index == 0) {
				uint8_t index[PINBA_SCRIPT_NAME_SIZE + 1] = {0};
				PPvoid_t ppvalue;
				pinba_tag_report *report;

				pthread_rwlock_wrlock(&D->tag_reports_lock);
				pthread_rwlock_rdlock(&D->timer_lock);
				report = pinba_get_tag_report(share);

				if (!report) {
					report = pinba_regenerate_tag2_report2(share);
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
						ret = tag2_report2_fetch_row_by_script(buf, this_index[active_index].str.val, this_index[active_index].str.len);
					} else {
						ret = HA_ERR_END_OF_FILE;
					}

					pthread_rwlock_unlock(&report->lock);
				} else {
					ret = HA_ERR_END_OF_FILE;
				}

				pthread_rwlock_unlock(&D->timer_lock);
				pthread_rwlock_unlock(&D->tag_reports_lock);
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		case PINBA_TABLE_TAGN_REPORT:
			if (active_index == 0) {
				uint8_t index[PINBA_SCRIPT_NAME_SIZE + 1] = {0};
				PPvoid_t ppvalue;
				pinba_tag_report *report;

				pthread_rwlock_wrlock(&D->tag_reports_lock);
				pthread_rwlock_rdlock(&D->timer_lock);
				report = pinba_get_tag_report(share);

				if (!report) {
					report = pinba_regenerate_tagN_report(share);
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
						ret = tagN_report_fetch_row_by_script(buf, this_index[active_index].str.val, this_index[active_index].str.len);
					} else {
						ret = HA_ERR_END_OF_FILE;
					}

					pthread_rwlock_unlock(&report->lock);
				} else {
					ret = HA_ERR_END_OF_FILE;
				}

				pthread_rwlock_unlock(&D->timer_lock);
				pthread_rwlock_unlock(&D->tag_reports_lock);
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		case PINBA_TABLE_TAGN_REPORT2:
			if (active_index == 0) {
				uint8_t index[PINBA_SCRIPT_NAME_SIZE + 1] = {0};
				PPvoid_t ppvalue;
				pinba_tag_report *report;

				pthread_rwlock_wrlock(&D->tag_reports_lock);
				pthread_rwlock_rdlock(&D->timer_lock);
				report = pinba_get_tag_report(share);

				if (!report) {
					report = pinba_regenerate_tagN_report2(share);
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
						ret = tagN_report2_fetch_row_by_script(buf, this_index[active_index].str.val, this_index[active_index].str.len);
					} else {
						ret = HA_ERR_END_OF_FILE;
					}

					pthread_rwlock_unlock(&report->lock);
				} else {
					ret = HA_ERR_END_OF_FILE;
				}

				pthread_rwlock_unlock(&D->timer_lock);
				pthread_rwlock_unlock(&D->tag_reports_lock);
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;

		case PINBA_TABLE_HISTOGRAM_VIEW:
			if (share->hv_table_type == PINBA_TABLE_REPORT_INFO) {
				ret = histogram_fetch_row(buf);
			} else {
				ret = HA_ERR_END_OF_FILE;
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
	pinba_pool *p = &D->request_pool;

	if (active_index >= PINBA_MAX_KEYS) {
		DBUG_RETURN(HA_ERR_WRONG_INDEX);
	}

	switch(share->table_type) {
		case PINBA_TABLE_REQUEST:
			if (active_index > 0) {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}

			if (!exact) {
				if (this_index[active_index].ival == (size_t)-1) {
					this_index[active_index].ival = p->out;
				}
			} else {
				memset(&(this_index[active_index].ival), 0, sizeof(this_index[active_index].ival));
				memcpy(&(this_index[active_index].ival), key, key_len);
			}
			ret = requests_fetch_row(buf, this_index[active_index].ival, NULL, exact);
			if (!exact) {
				this_index[active_index].ival++;
			}
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
		case PINBA_TABLE_HISTOGRAM_VIEW:
			if (share->hv_table_type == PINBA_TABLE_REPORT_INFO) {
				ret = histogram_fetch_row(buf);
			} else {
				if (active_index == 0) {
					memset(&(this_index[active_index].ival), 0, sizeof(this_index[active_index].ival));
					netstr_to_key(key, &this_index[active_index]);
					ret = histogram_fetch_row_by_key(buf, this_index[active_index].str.val, this_index[active_index].str.len);
				} else {
					ret = HA_ERR_WRONG_INDEX;
					goto failure;
				}
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
		case PINBA_TABLE_TAG_REPORT2:
			if (active_index == 0) {
				memset(&(this_index[active_index].ival), 0, sizeof(this_index[active_index].ival));
				netstr_to_key(key, &this_index[active_index]);
				ret = tag_report2_fetch_row_by_script(buf, this_index[active_index].str.val, this_index[active_index].str.len);
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		case PINBA_TABLE_TAG2_REPORT2:
			if (active_index == 0) {
				memset(&(this_index[active_index].ival), 0, sizeof(this_index[active_index].ival));
				netstr_to_key(key, &this_index[active_index]);
				ret = tag2_report2_fetch_row_by_script(buf, this_index[active_index].str.val, this_index[active_index].str.len);
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		case PINBA_TABLE_TAGN_REPORT:
			if (active_index == 0) {
				memset(&(this_index[active_index].ival), 0, sizeof(this_index[active_index].ival));
				netstr_to_key(key, &this_index[active_index]);
				ret = tagN_report_fetch_row_by_script(buf, this_index[active_index].str.val, this_index[active_index].str.len);
			} else {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}
			break;
		case PINBA_TABLE_TAGN_REPORT2:
			if (active_index == 0) {
				memset(&(this_index[active_index].ival), 0, sizeof(this_index[active_index].ival));
				netstr_to_key(key, &this_index[active_index]);
				ret = tagN_report2_fetch_row_by_script(buf, this_index[active_index].str.val, this_index[active_index].str.len);
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

int ha_pinba::read_row_by_pos(unsigned char *buf, my_off_t position) /* {{{ */
{
	DBUG_ENTER("ha_pinba::read_row_by_pos");
	int ret = HA_ERR_INTERNAL_ERROR;
	pinba_pool *p = &D->request_pool;
	pinba_pool *timers = &D->timer_pool;

	switch(share->table_type) {
		case PINBA_TABLE_REQUEST:
			if ((p->out + position) < p->size) {
				ret = requests_fetch_row(buf, p->out + position, NULL, 0);
			} else {
				ret = requests_fetch_row(buf, position - (p->size - p->out), NULL, 0);
			}
			break;
		case PINBA_TABLE_TIMER:
			if ((timers->out + position) < timers->size) {
				ret = timers_fetch_row(buf, timers->out + position, NULL, 0);
			} else {
				ret = timers_fetch_row(buf, position - (timers->size - timers->out), NULL, 0);
			}
			break;
		default:
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

int ha_pinba::read_next_row(unsigned char *buf, uint active_index, bool by_key) /* {{{ */
{
	DBUG_ENTER("ha_pinba::read_next_row");
	int ret = HA_ERR_INTERNAL_ERROR;

	if (active_index >= PINBA_MAX_KEYS) {
		DBUG_RETURN(HA_ERR_WRONG_INDEX);
	}

	switch(share->table_type) {
		case PINBA_TABLE_REQUEST:
			if (active_index > 0) {
				ret = HA_ERR_WRONG_INDEX;
				goto failure;
			}

			ret = requests_fetch_row(buf, this_index[active_index].ival, &(this_index[0].position), 0);
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
		case PINBA_TABLE_HISTOGRAM_VIEW:
			if (share->hv_table_type == PINBA_TABLE_REPORT_INFO) {
				ret = histogram_fetch_row(buf);
			} else {
				if (by_key == 0) {
					ret = HA_ERR_END_OF_FILE;
				} else if (active_index == 0) {
					ret = histogram_fetch_row_by_key(buf, this_index[active_index].str.val, this_index[active_index].str.len);
				} else {
					ret = HA_ERR_WRONG_INDEX;
					goto failure;
				}
			}
			break;
		case PINBA_TABLE_STATUS:
			ret = status_fetch_row(buf);
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
		case PINBA_TABLE_REPORT13:
			ret = report13_fetch_row(buf);
			break;
		case PINBA_TABLE_REPORT14:
			ret = report14_fetch_row(buf);
			break;
		case PINBA_TABLE_REPORT15:
			ret = report15_fetch_row(buf);
			break;
		case PINBA_TABLE_REPORT16:
			ret = report16_fetch_row(buf);
			break;
		case PINBA_TABLE_REPORT17:
			ret = report17_fetch_row(buf);
			break;
		case PINBA_TABLE_REPORT18:
			ret = report18_fetch_row(buf);
			break;
		case PINBA_TABLE_TAG_INFO:
			pthread_rwlock_rdlock(&D->tag_reports_lock);
			ret = tag_info_fetch_row(buf);
			pthread_rwlock_unlock(&D->tag_reports_lock);
			break;
		case PINBA_TABLE_TAG2_INFO:
			pthread_rwlock_rdlock(&D->tag_reports_lock);
			ret = tag2_info_fetch_row(buf);
			pthread_rwlock_unlock(&D->tag_reports_lock);
			break;
		case PINBA_TABLE_TAG_REPORT:
			pthread_rwlock_rdlock(&D->tag_reports_lock);
			if (by_key) {
				pinba_tag_report *report;

				report = pinba_get_tag_report(share);
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
			break;
		case PINBA_TABLE_TAG2_REPORT:
			pthread_rwlock_rdlock(&D->tag_reports_lock);
			if (by_key) {
				pinba_tag_report *report;

				report = pinba_get_tag_report(share);
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
			break;
		case PINBA_TABLE_TAG_REPORT2:
			pthread_rwlock_rdlock(&D->tag_reports_lock);
			if (by_key) {
				pinba_tag_report *report;

				report = pinba_get_tag_report(share);
				if (!report) {
					DBUG_RETURN(HA_ERR_END_OF_FILE);
				}

				ret = tag_report2_fetch_row_by_script(buf, this_index[active_index].str.val, this_index[active_index].str.len);
				if (ret) {
					free(this_index[active_index].str.val);
					this_index[active_index].str.val = NULL;
				}
			} else {
				ret = tag_report2_fetch_row(buf);
			}
			pthread_rwlock_unlock(&D->tag_reports_lock);
			break;
		case PINBA_TABLE_TAG2_REPORT2:
			pthread_rwlock_rdlock(&D->tag_reports_lock);
			if (by_key) {
				pinba_tag_report *report;

				report = pinba_get_tag_report(share);
				if (!report) {
					DBUG_RETURN(HA_ERR_END_OF_FILE);
				}

				ret = tag2_report2_fetch_row_by_script(buf, this_index[active_index].str.val, this_index[active_index].str.len);
				if (ret) {
					free(this_index[active_index].str.val);
					this_index[active_index].str.val = NULL;
				}
			} else {
				ret = tag2_report2_fetch_row(buf);
			}
			pthread_rwlock_unlock(&D->tag_reports_lock);
			break;
		case PINBA_TABLE_TAGN_INFO:
			pthread_rwlock_rdlock(&D->tag_reports_lock);
			ret = tagN_info_fetch_row(buf);
			pthread_rwlock_unlock(&D->tag_reports_lock);
			break;
		case PINBA_TABLE_TAGN_REPORT:
			pthread_rwlock_rdlock(&D->tag_reports_lock);
			if (by_key) {
				pinba_tag_report *report;

				report = pinba_get_tag_report(share);
				if (!report) {
					DBUG_RETURN(HA_ERR_END_OF_FILE);
				}

				ret = tagN_report_fetch_row_by_script(buf, this_index[active_index].str.val, this_index[active_index].str.len);
				if (ret) {
					free(this_index[active_index].str.val);
					this_index[active_index].str.val = NULL;
				}
			} else {
				ret = tagN_report_fetch_row(buf);
			}
			pthread_rwlock_unlock(&D->tag_reports_lock);
			break;
		case PINBA_TABLE_TAGN_REPORT2:
			pthread_rwlock_rdlock(&D->tag_reports_lock);
			if (by_key) {
				pinba_tag_report *report;

				report = pinba_get_tag_report(share);
				if (!report) {
					DBUG_RETURN(HA_ERR_END_OF_FILE);
				}

				ret = tagN_report2_fetch_row_by_script(buf, this_index[active_index].str.val, this_index[active_index].str.len);
				if (ret) {
					free(this_index[active_index].str.val);
					this_index[active_index].str.val = NULL;
				}
			} else {
				ret = tagN_report2_fetch_row(buf);
			}
			pthread_rwlock_unlock(&D->tag_reports_lock);
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

inline int ha_pinba::requests_fetch_row(unsigned char *buf, size_t index, size_t *new_index, int exact) /* {{{ */
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

retry_again:

	if (index == (p->size - 1)) {
		index = 0;
	}

	if (new_index) {
		*new_index = index;
	}

	if (index == p->in || index >= (unsigned int)p->size) {
		pthread_rwlock_unlock(&D->collector_lock);
		DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
	}

	record = REQ_POOL(p)[index];

	if (record.time.tv_sec == 0) { /* invalid record */
		if (exact) {
			pthread_rwlock_unlock(&D->collector_lock);
			DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
		} else {
			index++;
			goto retry_again;
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
				case 12: /* memory_footprint */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)record.data.memory_footprint, 1000));
					break;
				case 14: /* tags_cnt */
					(*field)->set_notnull();
					(*field)->store((long)record.data.tags_cnt);
					break;
				case 15: /* tags */
					{
						if (record.data.tags_cnt) {
							char *tags;
							int tags_len;

							if (pinba_tags_to_string(record.data.tag_names, record.data.tag_values, record.data.tags_cnt, &tags, &tags_len)) {
								(*field)->set_notnull();
								(*field)->store(tags, tags_len, &my_charset_bin);
								free(tags);
								break;
							}
						}

						(*field)->set_notnull();
						(*field)->store("", 0, &my_charset_bin);
					}
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

	if (index == timer_pool->in || index >= (unsigned int)timer_pool->size) {
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

	if (index == p->in || index >= (unsigned int)D->settings.request_pool_size || p->in == p->out) {
		pthread_rwlock_unlock(&D->collector_lock);
		DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
	}

	record = REQ_POOL(p) + index;

	if (this_index[active_index].position >= record->timers_cnt) {
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

	*new_index = index;

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

	if (*index == timer_pool->in || *index >= (unsigned int)timer_pool->size || timer_pool->in == timer_pool->out) {
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

	if (this_index[0].ival == timer_pool->in || this_index[0].ival >= (unsigned int)timer_pool->size || timer_pool->in == timer_pool->out) {
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

#define REPORT_PERCENTILE_FIELD(last_field_num, data, cnt)																\
	if ((*field)->field_index > (last_field_num) && (*field)->field_index <= (last_field_num) + share->percentiles_num) {	\
		int p_num = (*field)->field_index - (last_field_num) - 1;									\
		(*field)->set_notnull();																\
		(*field)->store(pinba_histogram_value((pinba_std_report *)report, data, cnt * ((float)share->percentiles[p_num]/100))); \
	} else {																					\
		(*field)->set_null();																	\
	}


#define REPORT_FETCH_TOP_BLOCK(report_num)									\
	Field **field;															\
	my_bitmap_map *old_map;													\
	struct pinba_report ##report_num## _data *data;							\
	PPvoid_t ppvalue;														\
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};								\
	pinba_report *report;													\
																			\
	DBUG_ENTER("ha_pinba::report ##report_num## _fetch_row");				\
																			\
	report = pinba_get_report(share);										\
	if (!report) {															\
		DBUG_RETURN(HA_ERR_END_OF_FILE);									\
	}																		\
																			\
	pthread_rwlock_rdlock(&report->lock);									\
	if (this_index[0].position == 0 || this_index[0].str.val == NULL) {		\
		ppvalue = JudySLFirst(report->results, index, NULL);				\
	} else {																\
		strcpy((char *)index, (char *)this_index[0].str.val);				\
		ppvalue = JudySLNext(report->results, index, NULL);					\
		free(this_index[0].str.val);										\
		this_index[0].str.val = NULL;										\
	}																		\
																			\
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {							\
		pthread_rwlock_unlock(&report->lock);								\
		DBUG_RETURN(HA_ERR_END_OF_FILE);									\
	}																		\
																			\
	this_index[0].str.val = (unsigned char *)strdup((char *)index);			\
	this_index[0].position++;												\
																			\
	data = (struct pinba_report ##report_num## _data *)*ppvalue;			\
																			\
	old_map = dbug_tmp_use_all_columns(table, table->write_set);

inline int ha_pinba::report1_fetch_row(unsigned char *buf) /* {{{ */
{
	REPORT_FETCH_TOP_BLOCK(1);

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
				case 15: /* memory_footprint_total */
					(*field)->set_notnull();
					(*field)->store(data->memory_footprint);
					break;
				case 16: /* memory_footprint_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->memory_footprint/report->memory_footprint);
					break;
				case 17: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->req_count / 2));
					break;
				case 18: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(18, data->histogram_data, data->req_count);
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
	REPORT_FETCH_TOP_BLOCK(2);

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
				case 15: /* memory_footprint_total */
					(*field)->set_notnull();
					(*field)->store(data->memory_footprint);
					break;
				case 16: /* memory_footprint_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->memory_footprint/report->memory_footprint);
					break;
				case 17: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->req_count / 2));
					break;
				case 18: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(18, data->histogram_data, data->req_count);
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
	REPORT_FETCH_TOP_BLOCK(3);

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
				case 15: /* memory_footprint_total */
					(*field)->set_notnull();
					(*field)->store(data->memory_footprint);
					break;
				case 16: /* memory_footprint_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->memory_footprint/report->memory_footprint);
					break;
				case 17: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->req_count / 2));
					break;
				case 19: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(19, data->histogram_data, data->req_count);
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
	REPORT_FETCH_TOP_BLOCK(4);

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
				case 16: /* memory_footprint_total */
					(*field)->set_notnull();
					(*field)->store(data->memory_footprint);
					break;
				case 17: /* memory_footprint_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->memory_footprint/report->memory_footprint);
					break;
				case 18: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->req_count / 2));
					break;
				case 19: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(19, data->histogram_data, data->req_count);
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
	REPORT_FETCH_TOP_BLOCK(5);

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
				case 16: /* memory_footprint_total */
					(*field)->set_notnull();
					(*field)->store(data->memory_footprint);
					break;
				case 17: /* memory_footprint_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->memory_footprint/report->memory_footprint);
					break;
				case 18: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->req_count / 2));
					break;
				case 19: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(19, data->histogram_data, data->req_count);
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
	REPORT_FETCH_TOP_BLOCK(6);

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
				case 16: /* memory_footprint_total */
					(*field)->set_notnull();
					(*field)->store(data->memory_footprint);
					break;
				case 17: /* memory_footprint_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->memory_footprint/report->memory_footprint);
					break;
				case 18: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->req_count / 2));
					break;
				case 19: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(19, data->histogram_data, data->req_count);
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
	REPORT_FETCH_TOP_BLOCK(7);

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
				case 17: /* memory_footprint_total */
					(*field)->set_notnull();
					(*field)->store(data->memory_footprint);
					break;
				case 18: /* memory_footprint_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->memory_footprint/report->memory_footprint);
					break;
				case 19: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->req_count / 2));
					break;
				case 20: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(20, data->histogram_data, data->req_count);
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
	REPORT_FETCH_TOP_BLOCK(8);

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
				case 15: /* memory_footprint_total */
					(*field)->set_notnull();
					(*field)->store(data->memory_footprint);
					break;
				case 16: /* memory_footprint_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->memory_footprint/report->memory_footprint);
					break;
				case 17: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->req_count / 2));
					break;
				case 18: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(18, data->histogram_data, data->req_count);
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
	REPORT_FETCH_TOP_BLOCK(9);

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
				case 16: /* memory_footprint_total */
					(*field)->set_notnull();
					(*field)->store(data->memory_footprint);
					break;
				case 17: /* memory_footprint_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->memory_footprint/report->memory_footprint);
					break;
				case 18: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->req_count / 2));
					break;
				case 19: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(19, data->histogram_data, data->req_count);
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
	REPORT_FETCH_TOP_BLOCK(10);

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
				case 16: /* memory_footprint_total */
					(*field)->set_notnull();
					(*field)->store(data->memory_footprint);
					break;
				case 17: /* memory_footprint_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->memory_footprint/report->memory_footprint);
					break;
				case 18: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->req_count / 2));
					break;
				case 19: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(19, data->histogram_data, data->req_count);
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
	REPORT_FETCH_TOP_BLOCK(11);

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
				case 16: /* memory_footprint_total */
					(*field)->set_notnull();
					(*field)->store(data->memory_footprint);
					break;
				case 17: /* memory_footprint_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->memory_footprint/report->memory_footprint);
					break;
				case 18: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->req_count / 2));
					break;
				case 19: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(19, data->histogram_data, data->req_count);
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
	REPORT_FETCH_TOP_BLOCK(12);

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
				case 17: /* memory_footprint_total */
					(*field)->set_notnull();
					(*field)->store(data->memory_footprint);
					break;
				case 18: /* memory_footprint_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->memory_footprint/report->memory_footprint);
					break;
				case 19: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->req_count / 2));
					break;
				case 20: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(20, data->histogram_data, data->req_count);
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::report13_fetch_row(unsigned char *buf) /* {{{ */
{
	REPORT_FETCH_TOP_BLOCK(13);

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
				case 14: /* schema */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((const char *)index), &my_charset_bin);
					break;
				case 15: /* memory_footprint_total */
					(*field)->set_notnull();
					(*field)->store(data->memory_footprint);
					break;
				case 16: /* memory_footprint_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->memory_footprint/report->memory_footprint);
					break;
				case 17: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->req_count / 2));
					break;
				case 18: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(18, data->histogram_data, data->req_count);
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::report14_fetch_row(unsigned char *buf) /* {{{ */
{
	REPORT_FETCH_TOP_BLOCK(14);

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
				case 15: /* schema */
					(*field)->set_notnull();
					(*field)->store(data->schema, strlen(data->schema), &my_charset_bin);
					break;
				case 16: /* memory_footprint_total */
					(*field)->set_notnull();
					(*field)->store(data->memory_footprint);
					break;
				case 17: /* memory_footprint_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->memory_footprint/report->memory_footprint);
					break;
				case 18: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->req_count / 2));
					break;
				case 19: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(19, data->histogram_data, data->req_count);
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::report15_fetch_row(unsigned char *buf) /* {{{ */
{
	REPORT_FETCH_TOP_BLOCK(15);

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
				case 15: /* schema */
					(*field)->set_notnull();
					(*field)->store(data->schema, strlen(data->schema), &my_charset_bin);
					break;
				case 16: /* memory_footprint_total */
					(*field)->set_notnull();
					(*field)->store(data->memory_footprint);
					break;
				case 17: /* memory_footprint_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->memory_footprint/report->memory_footprint);
					break;
				case 18: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->req_count / 2));
					break;
				case 19: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(19, data->histogram_data, data->req_count);
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::report16_fetch_row(unsigned char *buf) /* {{{ */
{
	REPORT_FETCH_TOP_BLOCK(16);

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
				case 15: /* schema */
					(*field)->set_notnull();
					(*field)->store(data->schema, strlen(data->schema), &my_charset_bin);
					break;
				case 16: /* memory_footprint_total */
					(*field)->set_notnull();
					(*field)->store(data->memory_footprint);
					break;
				case 17: /* memory_footprint_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->memory_footprint/report->memory_footprint);
					break;
				case 18: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->req_count / 2));
					break;
				case 19: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(19, data->histogram_data, data->req_count);
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::report17_fetch_row(unsigned char *buf) /* {{{ */
{
	REPORT_FETCH_TOP_BLOCK(17);

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
				case 16: /* schema */
					(*field)->set_notnull();
					(*field)->store(data->schema, strlen(data->schema), &my_charset_bin);
					break;
				case 17: /* memory_footprint_total */
					(*field)->set_notnull();
					(*field)->store(data->memory_footprint);
					break;
				case 18: /* memory_footprint_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->memory_footprint/report->memory_footprint);
					break;
				case 19: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->req_count / 2));
					break;
				case 20: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(20, data->histogram_data, data->req_count);
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::report18_fetch_row(unsigned char *buf) /* {{{ */
{
	REPORT_FETCH_TOP_BLOCK(18);

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
				case 16: /* schema */
					(*field)->set_notnull();
					(*field)->store(data->schema, strlen(data->schema), &my_charset_bin);
					break;
				case 17: /* memory_footprint_total */
					(*field)->set_notnull();
					(*field)->store(data->memory_footprint);
					break;
				case 18: /* memory_footprint_percent */
					(*field)->set_notnull();
					(*field)->store(100.0 * (float)data->memory_footprint/report->memory_footprint);
					break;
				case 19: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->req_count / 2));
					break;
				case 20: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(20, data->histogram_data, data->req_count);
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

	report = pinba_get_report(share);

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
				case 6: /* memory_footprint */
					(*field)->set_notnull();
					(*field)->store(pinba_round(report->memory_footprint, 1000));
					break;
				case 7: /* req_time_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, report->std.histogram_data, report->results_cnt / 2));
					break;
				default:
					REPORT_PERCENTILE_FIELD(7, report->std.histogram_data, report->results_cnt)
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::status_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;

	DBUG_ENTER("ha_pinba::status_fetch_row");

	if (this_index[0].position != 0) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].position++;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* current_temp_pool_size */
					(*field)->set_notnull();
					pthread_rwlock_rdlock(&D->temp_lock);
					(*field)->store((long)D->temp_pool.size);
					pthread_rwlock_unlock(&D->temp_lock);
					break;
				case 1: /* current_timer_pool_size */
					(*field)->set_notnull();
					pthread_rwlock_rdlock(&D->timer_lock);
					(*field)->store((long)D->timer_pool.size);
					pthread_rwlock_unlock(&D->timer_lock);
					break;
				case 2: /* lost_tmp_records */
					(*field)->set_notnull();
					pthread_rwlock_rdlock(&D->stats_lock);
					(*field)->store((long)D->stats.lost_tmp_records);
					pthread_rwlock_unlock(&D->stats_lock);
					break;
				case 3: /* invalid_packets */
					(*field)->set_notnull();
					pthread_rwlock_rdlock(&D->stats_lock);
					(*field)->store((long)D->stats.invalid_packets);
					pthread_rwlock_unlock(&D->stats_lock);
					break;
				case 4: /* invalid_request_data */
					(*field)->set_notnull();
					pthread_rwlock_rdlock(&D->stats_lock);
					(*field)->store((long)D->stats.invalid_request_data);
					pthread_rwlock_unlock(&D->stats_lock);
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	DBUG_RETURN(0);
}
/* }}} */

#define TAG_INFO_FETCH_TOP_BLOCK(report_name)							\
	Field **field;														\
	my_bitmap_map *old_map;												\
	struct pinba_ ##report_name## _data *data;							\
	pinba_tag_report *report;											\
	PPvoid_t ppvalue;													\
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};							\
																		\
	DBUG_ENTER("ha_pinba:: ##report_name## _fetch_row");				\
																		\
	if (!share->params || share->params[0] == '\0') {					\
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);								\
	}																	\
																		\
	report = pinba_get_tag_report(share);								\
	if (!report) {														\
		DBUG_RETURN(HA_ERR_END_OF_FILE);								\
	}																	\
																		\
	pthread_rwlock_rdlock(&report->lock);								\
	if (this_index[0].position == 0) {									\
		ppvalue = JudySLFirst(report->results, index, NULL);			\
	} else {															\
		strcpy((char *)index, (char *)this_index[0].str.val);			\
		ppvalue = JudySLNext(report->results, index, NULL);				\
		free(this_index[0].str.val);									\
		this_index[0].str.val = NULL;									\
	}																	\
																		\
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {						\
		pthread_rwlock_unlock(&report->lock);							\
		DBUG_RETURN(HA_ERR_END_OF_FILE);								\
	}																	\
																		\
	this_index[0].str.val = (unsigned char *)strdup((char *)index);		\
	this_index[0].position++;											\
																		\
	data = (struct pinba_ ##report_name## _data *)*ppvalue;				\
																		\
	old_map = dbug_tmp_use_all_columns(table, table->write_set);

inline int ha_pinba::tag_info_fetch_row(unsigned char *buf) /* {{{ */
{
	TAG_INFO_FETCH_TOP_BLOCK(tag_info);

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
				case 6: /* timer_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->hit_count / 2));
					break;
				case 7: /* ru_utime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_utime_value), 1000));
					break;
				case 8: /* ru_stime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_stime_value), 1000));
					break;
				case 9: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(9, data->histogram_data, data->hit_count)
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
	TAG_INFO_FETCH_TOP_BLOCK(tag2_info);

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
				case 7: /* timer_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->hit_count / 2));
					break;
				case 8: /* ru_utime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_utime_value), 1000));
					break;
				case 9: /* ru_stime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_stime_value), 1000));
					break;
				case 10: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index, strlen((char *)index), &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(10, data->histogram_data, data->hit_count)
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

#define TAG_REPORT_TOP_BLOCK(report_name)											\
	Field **field;																	\
	my_bitmap_map *old_map;															\
	struct pinba_ ##report_name## _data *data;										\
	pinba_tag_report *report;														\
	PPvoid_t ppvalue, ppvalue_script;												\
	uint8_t index_script[PINBA_SCRIPT_NAME_SIZE + 1] = {0};							\
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};										\
	uint8_t index_value[PINBA_MAX_LINE_LEN] = {0};									\
	int index_value_len;															\
																					\
	DBUG_ENTER("ha_pinba:: ##report_name## _fetch_row");							\
																					\
	if (!share->params || share->params[0] == '\0') {								\
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);											\
	}																				\
																					\
	report = pinba_get_tag_report(share);											\
	if (!report) {																	\
		DBUG_RETURN(HA_ERR_END_OF_FILE);											\
	}																				\
																					\
	pthread_rwlock_rdlock(&report->lock);											\
	if (this_index[0].str.val == NULL) {											\
		ppvalue_script = JudySLFirst(report->results, index_script, NULL);			\
																					\
		if (!ppvalue_script) {														\
			pthread_rwlock_unlock(&report->lock);									\
			DBUG_RETURN(HA_ERR_END_OF_FILE);										\
		}																			\
																					\
		ppvalue = JudySLFirst(*ppvalue_script, index, NULL);						\
		if (!ppvalue) {																\
			pthread_rwlock_unlock(&report->lock);									\
			DBUG_RETURN(HA_ERR_END_OF_FILE);										\
		}																			\
		this_index[0].str.val = (unsigned char *)strdup((char *)index_script);		\
	} else {																		\
		strcpy((char *)index_script, (char *)this_index[0].str.val);				\
																					\
		ppvalue_script = JudySLGet(report->results, index_script, NULL);			\
		if (!ppvalue_script) {														\
			pthread_rwlock_unlock(&report->lock);									\
			DBUG_RETURN(HA_ERR_END_OF_FILE);										\
		}																			\
																					\
repeat_with_next_script:															\
		if (this_index[0].subindex.val == NULL) {									\
			index[0] = '\0';														\
			ppvalue = JudySLFirst(*ppvalue_script, index, NULL);					\
		} else {																	\
			strcpy((char *)index, (char *)this_index[0].subindex.val);				\
			ppvalue = JudySLNext(*ppvalue_script, index, NULL);						\
			free(this_index[0].subindex.val);										\
			this_index[0].subindex.val = NULL;										\
		}																			\
																					\
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {								\
			ppvalue_script = JudySLNext(report->results, index_script, NULL);		\
			free(this_index[0].str.val);											\
			this_index[0].str.val = NULL;											\
																					\
			if (ppvalue_script) {													\
				this_index[0].str.val = (unsigned char *)strdup((char *)index_script); \
				free(this_index[0].subindex.val);									\
				this_index[0].subindex.val = NULL;									\
				goto repeat_with_next_script;										\
			} else {																\
				pthread_rwlock_unlock(&report->lock);								\
				DBUG_RETURN(HA_ERR_END_OF_FILE);									\
			}																		\
		}																			\
	}																				\
																					\
	index_value_len = snprintf((char *)index_value, sizeof(index_value) - 1, "%s|%s", (char *)index_script, (char *)index); \
																					\
	this_index[0].subindex.val = (unsigned char *)strdup((char *)index);			\
	data = (struct pinba_ ##report_name## _data *)*ppvalue;								\
																					\
	old_map = dbug_tmp_use_all_columns(table, table->write_set);

inline int ha_pinba::tag_report_fetch_row(unsigned char *buf) /* {{{ */
{
	TAG_REPORT_TOP_BLOCK(tag_report);

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
				case 7: /* timer_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->hit_count / 2));
					break;
				case 8: /* ru_utime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_utime_value), 1000));
					break;
				case 9: /* ru_stime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_stime_value), 1000));
					break;
				case 10: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index_value, index_value_len, &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(10, data->histogram_data, data->hit_count)
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::tag2_report_fetch_row(unsigned char *buf) /* {{{ */
{
	TAG_REPORT_TOP_BLOCK(tag2_report);

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
				case 8: /* timer_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->hit_count / 2));
					break;
				case 9: /* ru_utime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_utime_value), 1000));
					break;
				case 10: /* ru_stime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_stime_value), 1000));
					break;
				case 11: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index_value, index_value_len, &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(11, data->histogram_data, data->hit_count)
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
	TAG_REPORT_TOP_BLOCK(tag_report2);

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
				case 9: /* timer_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->hit_count / 2));
					break;
				case 10: /* ru_utime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_utime_value), 1000));
					break;
				case 11: /* ru_stime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_stime_value), 1000));
					break;
				case 12: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index_value, index_value_len, &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(12, data->histogram_data, data->hit_count)
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::tag2_report2_fetch_row(unsigned char *buf) /* {{{ */
{
	TAG_REPORT_TOP_BLOCK(tag2_report2);

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
				case 10: /* timer_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->hit_count / 2));
					break;
				case 11: /* ru_utime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_utime_value), 1000));
					break;
				case 12: /* ru_stime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_stime_value), 1000));
					break;
				case 13: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index_value, index_value_len, &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(13, data->histogram_data, data->hit_count)
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

#define TAG_REPORT_TOP_BLOCK_BY_SCRIPT(report_name)									\
	Field **field;																	\
	my_bitmap_map *old_map;															\
	struct pinba_ ##report_name## _data *data;										\
	pinba_tag_report *report;														\
	PPvoid_t ppvalue, ppvalue_script;												\
	uint8_t index_script[PINBA_SCRIPT_NAME_SIZE + 1] = {0};							\
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};										\
	uint8_t index_value[PINBA_MAX_LINE_LEN] = {0};									\
	int index_value_len;															\
																					\
	DBUG_ENTER("ha_pinba:: ##report_name## _fetch_row_by_script");					\
																					\
	if (!share->params || share->params[0] == '\0') {								\
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);											\
	}																				\
																					\
	report = pinba_get_tag_report(share);											\
	if (!report) {																	\
		DBUG_RETURN(HA_ERR_END_OF_FILE);											\
	}																				\
																					\
	pthread_rwlock_rdlock(&report->lock);											\
	if (!this_index[0].str.val) {													\
		ppvalue_script = JudySLFirst(report->results, index_script, NULL);			\
		if (LIKELY(ppvalue_script != NULL)) {										\
			this_index[0].str.val = (unsigned char *)strdup((char *)index_script);	\
		}																			\
	} else {																		\
		ppvalue_script = JudySLGet(report->results, this_index[0].str.val, NULL);	\
	}																				\
																					\
	if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {					\
		pthread_rwlock_unlock(&report->lock);										\
		DBUG_RETURN(HA_ERR_END_OF_FILE);											\
	}																				\
																					\
	if (this_index[0].subindex.val == NULL) {										\
		ppvalue = JudySLFirst(*ppvalue_script, index, NULL);						\
	} else {																		\
		strcpy((char *)index, (char *)this_index[0].subindex.val);					\
		ppvalue = JudySLNext(*ppvalue_script, index, NULL);							\
		free(this_index[0].subindex.val);											\
		this_index[0].subindex.val = NULL;											\
	}																				\
																					\
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {									\
		pthread_rwlock_unlock(&report->lock);										\
		DBUG_RETURN(HA_ERR_END_OF_FILE);											\
	}																				\
																					\
	this_index[0].subindex.val = (unsigned char *)strdup((char *)index);			\
	index_value_len = snprintf((char *)index_value, sizeof(index_value) - 1, "%s|%s", (char *)index_script, (char *)index); \
	data = (struct pinba_ ##report_name## _data *)*ppvalue;							\
																					\
	old_map = dbug_tmp_use_all_columns(table, table->write_set);

inline int ha_pinba::tag_report_fetch_row_by_script(unsigned char *buf, const unsigned char *name, uint name_len) /* {{{ */
{
	TAG_REPORT_TOP_BLOCK_BY_SCRIPT(tag_report);

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
				case 7: /* timer_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->hit_count / 2));
					break;
				case 8: /* ru_utime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_utime_value), 1000));
					break;
				case 9: /* ru_stime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_stime_value), 1000));
					break;
				case 10: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index_value, index_value_len, &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(10, data->histogram_data, data->hit_count)
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
	TAG_REPORT_TOP_BLOCK_BY_SCRIPT(tag2_report);

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
				case 8: /* timer_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->hit_count / 2));
					break;
				case 9: /* ru_utime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_utime_value), 1000));
					break;
				case 10: /* ru_stime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_stime_value), 1000));
					break;
				case 11: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index_value, index_value_len, &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(11, data->histogram_data, data->hit_count)
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::tag_report2_fetch_row_by_script(unsigned char *buf, const unsigned char *name, uint name_len) /* {{{ */
{
	TAG_REPORT_TOP_BLOCK_BY_SCRIPT(tag_report2);

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
				case 9: /* timer_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->hit_count / 2));
					break;
				case 10: /* ru_utime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_utime_value), 1000));
					break;
				case 11: /* ru_stime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_stime_value), 1000));
					break;
				case 12: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index_value, index_value_len, &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(12, data->histogram_data, data->hit_count)
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::tag2_report2_fetch_row_by_script(unsigned char *buf, const unsigned char *name, uint name_len) /* {{{ */
{
	TAG_REPORT_TOP_BLOCK_BY_SCRIPT(tag2_report2);

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
				case 10: /* timer_median */
					(*field)->set_notnull();
					(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->hit_count / 2));
					break;
				case 11: /* ru_utime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_utime_value), 1000));
					break;
				case 12: /* ru_stime_value */
					(*field)->set_notnull();
					(*field)->store(pinba_round((float)timeval_to_float(data->ru_stime_value), 1000));
					break;
				case 13: /* index_value */
					(*field)->set_notnull();
					(*field)->store((const char *)index_value, index_value_len, &my_charset_bin);
					break;
				default:
					REPORT_PERCENTILE_FIELD(13, data->histogram_data, data->hit_count)
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::tagN_info_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	struct pinba_tagN_info_data *data;
	pinba_tag_report *report;
	PPvoid_t ppvalue;
	uint8_t *index;

	DBUG_ENTER("ha_pinba::tagN_info_fetch_row");

	if (!share->params || share->params[0] == '\0') {
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	report = pinba_get_tag_report(share);
	if (!report) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	index = (uint8_t *)calloc(report->tag_cnt, PINBA_TAG_VALUE_SIZE + 1);
	if (!index) {
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	pthread_rwlock_rdlock(&report->lock);
	if (this_index[0].position == 0) {
		ppvalue = JudySLFirst(report->results, index, NULL);
	} else {
		strcpy((char *)index, (char *)this_index[0].str.val);
		ppvalue = JudySLNext(report->results, index, NULL);
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
	}

	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
		free(index);
		pthread_rwlock_unlock(&report->lock);
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	this_index[0].str.val = (unsigned char *)strdup((char *)index);
	this_index[0].position++;

	data = (struct pinba_tagN_info_data *)*ppvalue;

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			if ((*field)->field_index < report->tag_cnt) { /* tagN_value */
				const char *tag_value = (const char *)data->tag_value + ((*field)->field_index) * PINBA_TAG_VALUE_SIZE;
				(*field)->set_notnull();
				(*field)->store(tag_value, strlen(tag_value), &my_charset_bin);
			} else if ((*field)->field_index == report->tag_cnt) { /* req_count */
				(*field)->set_notnull();
				(*field)->store((long)data->req_count);
			} else if ((*field)->field_index == report->tag_cnt + 1) { /* req_per_sec */
				(*field)->set_notnull();
				(*field)->store((float)data->req_count/(float)report->time_interval);
			} else if ((*field)->field_index == report->tag_cnt + 2) { /* hit_count */
				(*field)->set_notnull();
				(*field)->store((long)data->hit_count);
			} else if ((*field)->field_index == report->tag_cnt + 3) { /* hit_per_sec */
				(*field)->set_notnull();
				(*field)->store((float)data->hit_count/(float)report->time_interval);
			} else if ((*field)->field_index == report->tag_cnt + 4) { /* timer_value */
				(*field)->set_notnull();
				(*field)->store(timeval_to_float(data->timer_value));
			} else if ((*field)->field_index == report->tag_cnt + 5) { /* timer_median */
				(*field)->set_notnull();
				(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->hit_count / 2));
			} else if ((*field)->field_index == report->tag_cnt + 6) { /* ru_utime_value */
				(*field)->set_notnull();
				(*field)->store(pinba_round((float)timeval_to_float(data->ru_utime_value), 1000));
			} else if ((*field)->field_index == report->tag_cnt + 7) { /* ru_stime_value */
				(*field)->set_notnull();
				(*field)->store(pinba_round((float)timeval_to_float(data->ru_stime_value), 1000));
			} else if ((*field)->field_index == report->tag_cnt + 8) { /* index_value */
				(*field)->set_notnull();
				(*field)->store((const char *)index, strlen((const char *)index), &my_charset_bin);
			} else {
				REPORT_PERCENTILE_FIELD(report->tag_cnt + 8, data->histogram_data, data->hit_count)
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	free(index);
	DBUG_RETURN(0);
}
/* }}} */

#define TAGN_REPORT_TOP_BLOCK(report_name)													\
	Field **field;																			\
	my_bitmap_map *old_map;																	\
	struct pinba_ ##report_name## _data *data;												\
	pinba_tag_report *report;																\
	PPvoid_t ppvalue, ppvalue_script;														\
	uint8_t index_script[PINBA_SCRIPT_NAME_SIZE + 1] = {0};									\
	uint8_t *index;																			\
	int index_value_len;																	\
																							\
	DBUG_ENTER("ha_pinba:: ##report_name## _fetch_row");									\
																							\
	if (!share->params || share->params[0] == '\0') {										\
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);													\
	}																						\
																							\
	report = pinba_get_tag_report(share);													\
	if (!report) {																			\
		DBUG_RETURN(HA_ERR_END_OF_FILE);													\
	}																						\
																							\
	index = (uint8_t *)calloc(report->tag_cnt, PINBA_TAG_VALUE_SIZE + 1);					\
	if (!index) {																			\
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);													\
	}																						\
																							\
	pthread_rwlock_rdlock(&report->lock);													\
	if (this_index[0].str.val == NULL) {													\
		ppvalue_script = JudySLFirst(report->results, index_script, NULL);					\
		if (!ppvalue_script) {																\
			free(index);																	\
			pthread_rwlock_unlock(&report->lock);											\
			DBUG_RETURN(HA_ERR_END_OF_FILE);												\
		}																					\
																							\
		ppvalue = JudySLFirst(*ppvalue_script, index, NULL);								\
		if (!ppvalue) {																		\
			free(index);																	\
			pthread_rwlock_unlock(&report->lock);											\
			DBUG_RETURN(HA_ERR_END_OF_FILE);												\
		}																					\
		this_index[0].str.val = (unsigned char *)strdup((char *)index_script);				\
	} else {																				\
		strcpy((char *)index_script, (char *)this_index[0].str.val);						\
		ppvalue_script = JudySLGet(report->results, index_script, NULL);					\
		if (!ppvalue_script) {																\
			free(index);																	\
			pthread_rwlock_unlock(&report->lock);											\
			DBUG_RETURN(HA_ERR_END_OF_FILE);												\
		}																					\
																							\
repeat_with_next_script:																	\
		if (this_index[0].subindex.val == NULL) {											\
			index[0] = '\0';																\
			ppvalue = JudySLFirst(*ppvalue_script, index, NULL);							\
		} else {																			\
			strcpy((char *)index, (char *)this_index[0].subindex.val);						\
			ppvalue = JudySLNext(*ppvalue_script, index, NULL);								\
			free(this_index[0].subindex.val);												\
			this_index[0].subindex.val = NULL;												\
		}																					\
																							\
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {										\
			ppvalue_script = JudySLNext(report->results, index_script, NULL);				\
			free(this_index[0].str.val);													\
			this_index[0].str.val = NULL;													\
			if (ppvalue_script) {															\
				this_index[0].str.val = (unsigned char *)strdup((char *)index_script);		\
				free(this_index[0].subindex.val);											\
				this_index[0].subindex.val = NULL;											\
				goto repeat_with_next_script;												\
			} else {																		\
				free(index);																\
				pthread_rwlock_unlock(&report->lock);										\
				DBUG_RETURN(HA_ERR_END_OF_FILE);											\
			}																				\
		}																					\
	}																						\
																							\
	this_index[0].subindex.val = (unsigned char *)strdup((char *)index);					\
	data = (struct pinba_ ##report_name## _data *)*ppvalue;									\
																							\
	old_map = dbug_tmp_use_all_columns(table, table->write_set);

inline int ha_pinba::tagN_report_fetch_row(unsigned char *buf) /* {{{ */
{
	TAGN_REPORT_TOP_BLOCK(tagN_report);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			if ((*field)->field_index == 0) { /* script_name */
				(*field)->set_notnull();
				(*field)->store((const char *)data->script_name, strlen((const char *)data->script_name), &my_charset_bin);
			} else if ((*field)->field_index > 0 && (*field)->field_index <= report->tag_cnt) { /* tagN_value */
				const char *tag_value = (const char *)data->tag_value + ((*field)->field_index - 1) * PINBA_TAG_VALUE_SIZE;
				(*field)->set_notnull();
				(*field)->store(tag_value, strlen(tag_value), &my_charset_bin);
			} else if ((*field)->field_index == report->tag_cnt + 1) { /* req_count */
				(*field)->set_notnull();
				(*field)->store((long)data->req_count);
			} else if ((*field)->field_index == report->tag_cnt + 2) { /* req_per_sec */
				(*field)->set_notnull();
				(*field)->store((float)data->req_count/(float)report->time_interval);
			} else if ((*field)->field_index == report->tag_cnt + 3) { /* hit_count */
				(*field)->set_notnull();
				(*field)->store((long)data->hit_count);
			} else if ((*field)->field_index == report->tag_cnt + 4) { /* hit_per_sec */
				(*field)->set_notnull();
				(*field)->store((float)data->hit_count/(float)report->time_interval);
			} else if ((*field)->field_index == report->tag_cnt + 5) { /* timer_value */
				(*field)->set_notnull();
				(*field)->store(timeval_to_float(data->timer_value));
			} else if ((*field)->field_index == report->tag_cnt + 6) { /* timer_median */
				(*field)->set_notnull();
				(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->hit_count / 2));
			} else if ((*field)->field_index == report->tag_cnt + 7) { /* ru_utime_value */
				(*field)->set_notnull();
				(*field)->store(pinba_round((float)timeval_to_float(data->ru_utime_value), 1000));
			} else if ((*field)->field_index == report->tag_cnt + 8) { /* ru_stime_value */
				(*field)->set_notnull();
				(*field)->store(pinba_round((float)timeval_to_float(data->ru_stime_value), 1000));
			} else if ((*field)->field_index == report->tag_cnt + 9) { /* index_value */
				uint8_t *index_value;
				int index_value_alloc_len = PINBA_SCRIPT_NAME_SIZE + 1 + (PINBA_TAG_VALUE_SIZE + 1) * report->tag_cnt;

				index_value = (uint8_t *)malloc(index_value_alloc_len);
				if (!index_value) {
					(*field)->set_null();
				} else {
					index_value_len = snprintf((char *)index_value, index_value_alloc_len, "%s|%s", (char *)index_script, (char *)index);
					(*field)->set_notnull();
					(*field)->store((const char *)index_value, index_value_len, &my_charset_bin);
					free(index_value);
				}
			} else {
				REPORT_PERCENTILE_FIELD(report->tag_cnt + 9, data->histogram_data, data->hit_count)
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	free(index);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::tagN_report2_fetch_row(unsigned char *buf) /* {{{ */
{
	TAGN_REPORT_TOP_BLOCK(tagN_report2);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			if ((*field)->field_index == 0) { /* script_name */
				(*field)->set_notnull();
				(*field)->store((const char *)data->script_name, strlen((const char *)data->script_name), &my_charset_bin);
			} else if ((*field)->field_index > 0 && (*field)->field_index <= report->tag_cnt) { /* tagN_value */
				const char *tag_value = (const char *)data->tag_value + ((*field)->field_index - 1) * PINBA_TAG_VALUE_SIZE;
				(*field)->set_notnull();
				(*field)->store(tag_value, strlen(tag_value), &my_charset_bin);
			} else if ((*field)->field_index == report->tag_cnt + 1) { /* req_count */
				(*field)->set_notnull();
				(*field)->store((long)data->req_count);
			} else if ((*field)->field_index == report->tag_cnt + 2) { /* req_per_sec */
				(*field)->set_notnull();
				(*field)->store((float)data->req_count/(float)report->time_interval);
			} else if ((*field)->field_index == report->tag_cnt + 3) { /* hit_count */
				(*field)->set_notnull();
				(*field)->store((long)data->hit_count);
			} else if ((*field)->field_index == report->tag_cnt + 4) { /* hit_per_sec */
				(*field)->set_notnull();
				(*field)->store((float)data->hit_count/(float)report->time_interval);
			} else if ((*field)->field_index == report->tag_cnt + 5) { /* timer_value */
				(*field)->set_notnull();
				(*field)->store(timeval_to_float(data->timer_value));
			} else if ((*field)->field_index == report->tag_cnt + 6) { /* hostname */
				(*field)->set_notnull();
				(*field)->store((const char *)data->hostname, strlen((const char *)data->hostname), &my_charset_bin);
			} else if ((*field)->field_index == report->tag_cnt + 7) { /* server_name */
				(*field)->set_notnull();
				(*field)->store((const char *)data->server_name, strlen((const char *)data->server_name), &my_charset_bin);
			} else if ((*field)->field_index == report->tag_cnt + 8) { /* timer_median */
				(*field)->set_notnull();
				(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->hit_count / 2));
			} else if ((*field)->field_index == report->tag_cnt + 9) { /* ru_utime_value */
				(*field)->set_notnull();
				(*field)->store(pinba_round((float)timeval_to_float(data->ru_utime_value), 1000));
			} else if ((*field)->field_index == report->tag_cnt + 10) { /* ru_stime_value */
				(*field)->set_notnull();
				(*field)->store(pinba_round((float)timeval_to_float(data->ru_stime_value), 1000));
			} else if ((*field)->field_index == report->tag_cnt + 11) { /* index_value */
				uint8_t *index_value;
				int index_value_alloc_len = PINBA_SCRIPT_NAME_SIZE + 1 + (PINBA_TAG_VALUE_SIZE + 1) * report->tag_cnt;

				index_value = (uint8_t *)malloc(index_value_alloc_len);
				if (!index_value) {
					(*field)->set_null();
				} else {
					index_value_len = snprintf((char *)index_value, index_value_alloc_len, "%s|%s", (char *)index_script, (char *)index);
					(*field)->set_notnull();
					(*field)->store((const char *)index_value, index_value_len, &my_charset_bin);
					free(index_value);
				}
			} else {
				REPORT_PERCENTILE_FIELD(report->tag_cnt + 11, data->histogram_data, data->hit_count)
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	free(index);
	DBUG_RETURN(0);
}
/* }}} */

#define TAGN_REPORT_TOP_BLOCK_BY_SCRIPT(report_name)										\
	Field **field;																			\
	my_bitmap_map *old_map;																	\
	struct pinba_ ##report_name## _data *data;												\
	pinba_tag_report *report;																\
	PPvoid_t ppvalue, ppvalue_script;														\
	uint8_t *index;																			\
	int index_value_len;																	\
																							\
	DBUG_ENTER("ha_pinba:: ##report_name## _fetch_row_by_script");							\
																							\
	if (!share->params || share->params[0] == '\0') {										\
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);													\
	}																						\
																							\
	report = pinba_get_tag_report(share);													\
	if (!report) {																			\
		DBUG_RETURN(HA_ERR_END_OF_FILE);													\
	}																						\
																							\
	index = (uint8_t *)calloc(report->tag_cnt, PINBA_TAG_VALUE_SIZE + 1);					\
	if (!index) {																			\
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);													\
	}																						\
																							\
	pthread_rwlock_rdlock(&report->lock);													\
	if (!this_index[0].str.val) {															\
		ppvalue_script = JudySLFirst(report->results, index, NULL);							\
		if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {						\
			free(index);																	\
			pthread_rwlock_unlock(&report->lock);											\
			DBUG_RETURN(HA_ERR_END_OF_FILE);												\
		}																					\
		this_index[0].str.val = (unsigned char *)strdup((char *)index);						\
		index[0] = '\0';																	\
	} else {																				\
		ppvalue_script = JudySLGet(report->results, this_index[0].str.val, NULL);			\
		if (UNLIKELY(!ppvalue_script || ppvalue_script == PPJERR)) {						\
			free(index);																	\
			pthread_rwlock_unlock(&report->lock);											\
			DBUG_RETURN(HA_ERR_END_OF_FILE);												\
		}																					\
	}																						\
																							\
	if (this_index[0].subindex.val == NULL) {												\
		ppvalue = JudySLFirst(*ppvalue_script, index, NULL);								\
	} else {																				\
		strcpy((char *)index, (char *)this_index[0].subindex.val);							\
		ppvalue = JudySLNext(*ppvalue_script, index, NULL);									\
		free(this_index[0].subindex.val);													\
		this_index[0].subindex.val = NULL;													\
	}																						\
																							\
	if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {											\
		free(index);																		\
		pthread_rwlock_unlock(&report->lock);												\
		DBUG_RETURN(HA_ERR_END_OF_FILE);													\
	}																						\
																							\
	this_index[0].subindex.val = (unsigned char *)strdup((char *)index);					\
	data = (struct pinba_ ##report_name## _data *)*ppvalue;									\
	old_map = dbug_tmp_use_all_columns(table, table->write_set);

inline int ha_pinba::tagN_report_fetch_row_by_script(unsigned char *buf, const unsigned char *name, uint name_len) /* {{{ */
{
	TAGN_REPORT_TOP_BLOCK_BY_SCRIPT(tagN_report);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			if ((*field)->field_index == 0) { /* script_name */
				(*field)->set_notnull();
				(*field)->store((const char *)data->script_name, strlen((const char *)data->script_name), &my_charset_bin);
			} else if ((*field)->field_index > 0 && (*field)->field_index <= report->tag_cnt) { /* tagN_value */
				const char *tag_value = (const char *)data->tag_value + ((*field)->field_index - 1) * PINBA_TAG_VALUE_SIZE;
				(*field)->set_notnull();
				(*field)->store(tag_value, strlen(tag_value), &my_charset_bin);
			} else if ((*field)->field_index == report->tag_cnt + 1) { /* req_count */
				(*field)->set_notnull();
				(*field)->store((long)data->req_count);
			} else if ((*field)->field_index == report->tag_cnt + 2) { /* req_per_sec */
				(*field)->set_notnull();
				(*field)->store((float)data->req_count/(float)report->time_interval);
			} else if ((*field)->field_index == report->tag_cnt + 3) { /* hit_count */
				(*field)->set_notnull();
				(*field)->store((long)data->hit_count);
			} else if ((*field)->field_index == report->tag_cnt + 4) { /* hit_per_sec */
				(*field)->set_notnull();
				(*field)->store((float)data->hit_count/(float)report->time_interval);
			} else if ((*field)->field_index == report->tag_cnt + 5) { /* timer_value */
				(*field)->set_notnull();
				(*field)->store(timeval_to_float(data->timer_value));
			} else if ((*field)->field_index == report->tag_cnt + 6) { /* timer_median */
				(*field)->set_notnull();
				(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->hit_count / 2));
			} else if ((*field)->field_index == report->tag_cnt + 7) { /* ru_utime_value */
				(*field)->set_notnull();
				(*field)->store(pinba_round((float)timeval_to_float(data->ru_utime_value), 1000));
			} else if ((*field)->field_index == report->tag_cnt + 8) { /* ru_stime_value */
				(*field)->set_notnull();
				(*field)->store(pinba_round((float)timeval_to_float(data->ru_stime_value), 1000));
			} else if ((*field)->field_index == report->tag_cnt + 9) { /* index_value */
				uint8_t *index_value;
				int index_value_alloc_len = PINBA_SCRIPT_NAME_SIZE + 1 + (PINBA_TAG_VALUE_SIZE + 1) * report->tag_cnt;

				index_value = (uint8_t *)malloc(index_value_alloc_len);
				if (!index_value) {
					(*field)->set_null();
				} else {
					index_value_len = snprintf((char *)index_value, index_value_alloc_len, "%s|%s", (char *)this_index[0].str.val, (char *)index);
					(*field)->set_notnull();
					(*field)->store((const char *)index_value, index_value_len, &my_charset_bin);
					free(index_value);
				}
			} else {
				REPORT_PERCENTILE_FIELD(report->tag_cnt + 9, data->histogram_data, data->hit_count)
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	free(index);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::tagN_report2_fetch_row_by_script(unsigned char *buf, const unsigned char *name, uint name_len) /* {{{ */
{
	TAGN_REPORT_TOP_BLOCK_BY_SCRIPT(tagN_report2);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			if ((*field)->field_index == 0) { /* script_name */
				(*field)->set_notnull();
				(*field)->store((const char *)data->script_name, strlen((const char *)data->script_name), &my_charset_bin);
			} else if ((*field)->field_index > 0 && (*field)->field_index <= report->tag_cnt) { /* tagN_value */
				const char *tag_value = (const char *)data->tag_value + ((*field)->field_index - 1) * PINBA_TAG_VALUE_SIZE;
				(*field)->set_notnull();
				(*field)->store(tag_value, strlen(tag_value), &my_charset_bin);
			} else if ((*field)->field_index == report->tag_cnt + 1) { /* req_count */
				(*field)->set_notnull();
				(*field)->store((long)data->req_count);
			} else if ((*field)->field_index == report->tag_cnt + 2) { /* req_per_sec */
				(*field)->set_notnull();
				(*field)->store((float)data->req_count/(float)report->time_interval);
			} else if ((*field)->field_index == report->tag_cnt + 3) { /* hit_count */
				(*field)->set_notnull();
				(*field)->store((long)data->hit_count);
			} else if ((*field)->field_index == report->tag_cnt + 4) { /* hit_per_sec */
				(*field)->set_notnull();
				(*field)->store((float)data->hit_count/(float)report->time_interval);
			} else if ((*field)->field_index == report->tag_cnt + 5) { /* timer_value */
				(*field)->set_notnull();
				(*field)->store(timeval_to_float(data->timer_value));
			} else if ((*field)->field_index == report->tag_cnt + 6) { /* hostname */
				(*field)->set_notnull();
				(*field)->store((const char *)data->hostname, strlen((const char *)data->hostname), &my_charset_bin);
			} else if ((*field)->field_index == report->tag_cnt + 7) { /* server_name */
				(*field)->set_notnull();
				(*field)->store((const char *)data->server_name, strlen((const char *)data->server_name), &my_charset_bin);
			} else if ((*field)->field_index == report->tag_cnt + 8) { /* timer_median */
				(*field)->set_notnull();
				(*field)->store(pinba_histogram_value((pinba_std_report *)report, data->histogram_data, data->hit_count / 2));
			} else if ((*field)->field_index == report->tag_cnt + 9) { /* ru_utime_value */
				(*field)->set_notnull();
				(*field)->store(pinba_round((float)timeval_to_float(data->ru_utime_value), 1000));
			} else if ((*field)->field_index == report->tag_cnt + 10) { /* ru_stime_value */
				(*field)->set_notnull();
				(*field)->store(pinba_round((float)timeval_to_float(data->ru_stime_value), 1000));
			} else if ((*field)->field_index == report->tag_cnt + 11) { /* index_value */
				uint8_t *index_value;
				int index_value_alloc_len = PINBA_SCRIPT_NAME_SIZE + 1 + (PINBA_TAG_VALUE_SIZE + 1) * report->tag_cnt;

				index_value = (uint8_t *)malloc(index_value_alloc_len);
				if (!index_value) {
					(*field)->set_null();
				} else {
					index_value_len = snprintf((char *)index_value, index_value_alloc_len, "%s|%s", (char *)this_index[0].str.val, (char *)index);
					(*field)->set_notnull();
					(*field)->store((const char *)index_value, index_value_len, &my_charset_bin);
					free(index_value);
				}
			} else {
				REPORT_PERCENTILE_FIELD(report->tag_cnt + 11, data->histogram_data, data->hit_count)
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);
	pthread_rwlock_unlock(&report->lock);
	free(index);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::histogram_fetch_row(unsigned char *buf) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	pinba_report *report;
	int *histogram_data;
	int position;
	pinba_std_report *std;
	unsigned long results_cnt;

	DBUG_ENTER("ha_pinba::histogram_fetch_row");

	if (this_index[0].position >= PINBA_HISTOGRAM_SIZE) {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	position = this_index[0].position;

	if (!share->tag_report) {
		report = pinba_get_report(share);

		if (!report) {
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}

		pthread_rwlock_rdlock(&report->lock);

		std = (pinba_std_report *)report;

		if (share->hv_table_type == PINBA_TABLE_REPORT_INFO) {
			histogram_data = (int *)std->histogram_data;
			results_cnt = report->results_cnt;
		} else {
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}
	} else {
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* index value */
					/* NULL */
					break;
				case 1: /* histogram index */
					(*field)->set_notnull();
					(*field)->store((long)position);
					break;
				case 2: /* time_value */
					(*field)->set_notnull();
					(*field)->store((float)std->histogram_segment * position);
					break;
				case 3: /* count */
					(*field)->set_notnull();
					(*field)->store((long)histogram_data[position]);
					break;
				case 4: /* cnt_percent */
					(*field)->set_notnull();
					if (histogram_data[position] > 0) {
						(*field)->store(((float)histogram_data[position]/(float)results_cnt) * 100.0);
					} else {
						(*field)->store((float)0);
					}
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);

	this_index[0].position++;

	pthread_rwlock_unlock(&report->lock);
	DBUG_RETURN(0);
}
/* }}} */

inline int ha_pinba::histogram_fetch_row_by_key(unsigned char *buf, const unsigned char *name, uint name_len) /* {{{ */
{
	Field **field;
	my_bitmap_map *old_map;
	pinba_report *report;
	pinba_tag_report *tag_report;
	PPvoid_t ppvalue;
	int *histogram_data;
	int position;
	pinba_std_report *std;
	unsigned long results_cnt;

	DBUG_ENTER("ha_pinba::histogram_fetch_row_by_key");

	if (this_index[0].position >= PINBA_HISTOGRAM_SIZE) {
		free(this_index[0].str.val);
		this_index[0].str.val = NULL;
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	position = this_index[0].position;

	if (!share->tag_report) {
		struct pinba_report_data_header *header;

		report = pinba_get_report(share);
		if (!report) {
			free(this_index[0].str.val);
			this_index[0].str.val = NULL;
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}

		pthread_rwlock_rdlock(&report->lock);

		std = (pinba_std_report *)report;

		ppvalue = JudySLGet(report->results, this_index[0].str.val, NULL);
		if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
			free(this_index[0].str.val);
			this_index[0].str.val = NULL;
			pthread_rwlock_unlock(&report->lock);
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}

		header = (pinba_report_data_header *)*ppvalue;
		histogram_data = header->histogram_data;
		results_cnt = header->req_count;
	} else {
		struct pinba_tag_report_data_header *header;

		tag_report = pinba_get_tag_report(share);
		if (!tag_report) {
			DBUG_RETURN(HA_ERR_END_OF_FILE);
		}

		pthread_rwlock_rdlock(&tag_report->lock);

		std = (pinba_std_report *)tag_report;

		if (share->hv_table_type == PINBA_TABLE_TAG_REPORT || share->hv_table_type == PINBA_TABLE_TAG2_REPORT
			|| share->hv_table_type == PINBA_TABLE_TAG_REPORT2 || share->hv_table_type == PINBA_TABLE_TAG2_REPORT2) {
			/* only these tables atm have indexes by script, so we have to recreate and use them instead of a composite index */
			uint8_t index_script[PINBA_SCRIPT_NAME_SIZE + 1] = {0};
			uint8_t index_tag[PINBA_MAX_LINE_LEN] = {0};
			PPvoid_t ppvalue_script;
			char *p;
			int script_len, tag_len;

			if (!this_index[0].str.val) {
				pthread_rwlock_unlock(&tag_report->lock);
				DBUG_RETURN(HA_ERR_END_OF_FILE);
			}

			p = strchr((char *)this_index[0].str.val, '|');
			if (!p) {
				free(this_index[0].str.val);
				this_index[0].str.val = NULL;
				pthread_rwlock_unlock(&tag_report->lock);
				DBUG_RETURN(HA_ERR_END_OF_FILE);
			}

			script_len = snprintf((char *)index_script, PINBA_SCRIPT_NAME_SIZE, "%.*s", p - (char *)this_index[0].str.val, this_index[0].str.val);
			index_script[script_len] = '\0';
			tag_len = snprintf((char *)index_tag, PINBA_MAX_LINE_LEN, "%s", p + 1 /* skip the '|' */);
			index_tag[tag_len] = '\0';

			ppvalue_script = JudySLGet(tag_report->results, index_script, NULL);
			if (!ppvalue_script) {
				free(this_index[0].str.val);
				this_index[0].str.val = NULL;
				pthread_rwlock_unlock(&tag_report->lock);
				DBUG_RETURN(HA_ERR_END_OF_FILE);
			}

			ppvalue = JudySLGet(*ppvalue_script, index_tag, NULL);
			if (!ppvalue) {
				free(this_index[0].str.val);
				this_index[0].str.val = NULL;
				pthread_rwlock_unlock(&tag_report->lock);
				DBUG_RETURN(HA_ERR_END_OF_FILE);
			}

		} else {
			ppvalue = JudySLGet(tag_report->results, this_index[0].str.val, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				free(this_index[0].str.val);
				this_index[0].str.val = NULL;
				pthread_rwlock_unlock(&tag_report->lock);
				DBUG_RETURN(HA_ERR_END_OF_FILE);
			}
		}

		header = (pinba_tag_report_data_header *)*ppvalue;
		histogram_data = header->histogram_data;
		results_cnt = header->hit_count;
	}

	old_map = dbug_tmp_use_all_columns(table, table->write_set);

	for (field = table->field; *field; field++) {
		if (bitmap_is_set(table->read_set, (*field)->field_index)) {
			switch((*field)->field_index) {
				case 0: /* index value */
					(*field)->set_notnull();
					(*field)->store((const char *)this_index[0].str.val, this_index[0].str.len, &my_charset_bin);
					break;
				case 1: /* histogram index */
					(*field)->set_notnull();
					(*field)->store((long)position);
					break;
				case 2: /* time_value */
					(*field)->set_notnull();
					(*field)->store((float)std->histogram_segment * position);
					break;
				case 3: /* count */
					(*field)->set_notnull();
					(*field)->store((long)histogram_data[position]);
					break;
				case 4: /* cnt_percent */
					(*field)->set_notnull();
					if (histogram_data[position] > 0) {
						(*field)->store(((float)histogram_data[position]/(float)results_cnt) * 100.0);
					} else {
						(*field)->store((float)0);
					}
					break;
				default:
					(*field)->set_null();
					break;
			}
		}
	}
	dbug_tmp_restore_column_map(table->write_set, old_map);

	if (!share->tag_report) {
		pthread_rwlock_unlock(&report->lock);
	} else {
		pthread_rwlock_unlock(&tag_report->lock);
	}
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
	unsigned char type;
	DBUG_ENTER("ha_pinba::create");

	if (!table_arg->s) {
		DBUG_RETURN(HA_WRONG_CREATE_OPTION);
	}

	type = pinba_get_table_type(table_arg->s->comment.str, table_arg->s->comment.length, NULL);
	if (type == PINBA_TABLE_UNKNOWN) {
		DBUG_RETURN(HA_WRONG_CREATE_OPTION);
	}

	if (pinba_parse_params(table_arg, type, NULL) < 0) {
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
				pthread_rwlock_rdlock(&D->base_reports_lock);				\
				report = pinba_get_report(share);	\
																			\
				if (!report) {												\
					pthread_rwlock_unlock(&D->base_reports_lock);			\
					pthread_rwlock_rdlock(&D->collector_lock);				\
					pthread_rwlock_wrlock(&D->base_reports_lock);			\
					report = pinba_regenerate_report ## __id__(share);		\
					pthread_rwlock_unlock(&D->base_reports_lock);			\
					pthread_rwlock_unlock(&D->collector_lock);				\
					pthread_rwlock_rdlock(&D->base_reports_lock);			\
				}															\
																			\
				stats.records = 0;											\
				if (report) {												\
					pthread_rwlock_rdlock(&report->lock);					\
					stats.records = report->results_cnt;					\
					pthread_rwlock_unlock(&report->lock);					\
				}															\
				pthread_rwlock_unlock(&D->base_reports_lock);				\
			}

#define HANDLE_TAG_REPORT(__name__, __lc_name__)							\
			{																\
				pinba_tag_report *report;									\
				pthread_rwlock_rdlock(&D->tag_reports_lock);				\
				report = pinba_get_tag_report(share); \
																			\
				if (!report) {												\
					pthread_rwlock_unlock(&D->tag_reports_lock);			\
					pthread_rwlock_rdlock(&D->collector_lock);				\
					pthread_rwlock_wrlock(&D->tag_reports_lock);			\
					pthread_rwlock_rdlock(&D->timer_lock);					\
					report = pinba_regenerate_ ## __lc_name__(share);		\
					pthread_rwlock_unlock(&D->timer_lock);					\
					pthread_rwlock_unlock(&D->tag_reports_lock);			\
					pthread_rwlock_unlock(&D->collector_lock);				\
					pthread_rwlock_rdlock(&D->tag_reports_lock);			\
				}															\
																			\
				stats.records = 0;											\
				if (report) {												\
					pthread_rwlock_rdlock(&report->lock);					\
					stats.records = report->results_cnt;					\
					pthread_rwlock_unlock(&report->lock);					\
				}															\
				pthread_rwlock_unlock(&D->tag_reports_lock);				\
			}

int ha_pinba::info(uint flag) /* {{{ */
{
	pinba_pool *p = &D->request_pool;
	int type;

	DBUG_ENTER("ha_pinba::info");

	type = share->table_type;
	if (type == PINBA_TABLE_HISTOGRAM_VIEW) {
		/* substitute with the real type in order to regenerate the report if needed */
		type = share->hv_table_type;
	}

	switch(type) {
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
		case PINBA_TABLE_HISTOGRAM_VIEW:
			stats.records = PINBA_HISTOGRAM_SIZE;
			break;
		case PINBA_TABLE_REPORT_INFO:
			{
				pinba_report *report;

				pthread_rwlock_rdlock(&D->base_reports_lock);
				report = pinba_get_report(share);

				if (!report) {
					pthread_rwlock_unlock(&D->base_reports_lock);
					pthread_rwlock_rdlock(&D->collector_lock);
					pthread_rwlock_wrlock(&D->base_reports_lock);
					report = pinba_regenerate_report_info(share);
					pthread_rwlock_unlock(&D->base_reports_lock);
					pthread_rwlock_unlock(&D->collector_lock);
					pthread_rwlock_rdlock(&D->base_reports_lock);
				}

				stats.records = 0;
				if (report) {
					stats.records = 1;
				}
				pthread_rwlock_unlock(&D->base_reports_lock);
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
		case PINBA_TABLE_REPORT13:
			HANDLE_REPORT(13);
			break;
		case PINBA_TABLE_REPORT14:
			HANDLE_REPORT(14);
			break;
		case PINBA_TABLE_REPORT15:
			HANDLE_REPORT(15);
			break;
		case PINBA_TABLE_REPORT16:
			HANDLE_REPORT(16);
			break;
		case PINBA_TABLE_REPORT17:
			HANDLE_REPORT(17);
			break;
		case PINBA_TABLE_REPORT18:
			HANDLE_REPORT(18);
			break;
		case PINBA_TABLE_TAG_INFO:
			HANDLE_TAG_REPORT(TAG_INFO, tag_info);
			break;
		case PINBA_TABLE_TAG2_INFO:
			HANDLE_TAG_REPORT(TAG2_INFO, tag2_info);
			break;
		case PINBA_TABLE_TAG_REPORT:
			HANDLE_TAG_REPORT(TAG_REPORT, tag_report);
			break;
		case PINBA_TABLE_TAG2_REPORT:
			HANDLE_TAG_REPORT(TAG2_REPORT, tag2_report);
			break;
		case PINBA_TABLE_TAG_REPORT2:
			HANDLE_TAG_REPORT(TAG_REPORT2, tag_report2);
			break;
		case PINBA_TABLE_TAG2_REPORT2:
			HANDLE_TAG_REPORT(TAG2_REPORT2, tag2_report2);
			break;
		case PINBA_TABLE_TAGN_INFO:
			HANDLE_TAG_REPORT(TAGN_INFO, tagN_info);
			break;
		case PINBA_TABLE_TAGN_REPORT:
			HANDLE_TAG_REPORT(TAGN_REPORT, tagN_report);
			break;
		case PINBA_TABLE_TAGN_REPORT2:
			HANDLE_TAG_REPORT(TAGN_REPORT2, tagN_report2);
			break;
		default:
			stats.records = 2; /* dummy */
			break;
	}

	if (share->table_type == PINBA_TABLE_HISTOGRAM_VIEW) {
		stats.records = PINBA_HISTOGRAM_SIZE;
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

static MYSQL_SYSVAR_INT(data_pool_size,
  data_pool_size_var,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Raw socket data pool size",
  NULL,
  NULL,
  0,
  0,
  INT_MAX,
  0);

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

static MYSQL_SYSVAR_INT(temp_pool_size_limit,
  temp_pool_size_limit_var,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Temporary pool size limit",
  NULL,
  NULL,
  0,
  1000,
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

static MYSQL_SYSVAR_INT(histogram_max_time,
  histogram_max_time_var,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Set max time value for median computation",
  NULL,
  NULL,
  10,
  1,
  INT_MAX,
  0);

static struct st_mysql_sys_var* system_variables[]= {
	MYSQL_SYSVAR(port),
	MYSQL_SYSVAR(address),
	MYSQL_SYSVAR(data_pool_size),
	MYSQL_SYSVAR(temp_pool_size),
	MYSQL_SYSVAR(temp_pool_size_limit),
	MYSQL_SYSVAR(request_pool_size),
	MYSQL_SYSVAR(stats_history),
	MYSQL_SYSVAR(stats_gathering_period),
	MYSQL_SYSVAR(cpu_start),
	MYSQL_SYSVAR(histogram_max_time),
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
	0x0101, /* VERSION 1.1.0 */
	NULL,                       /* status variables                */
	system_variables,           /* system variables                */
	NULL                        /* config options                  */
}
mysql_declare_plugin_end;
/* }}} */

/*
 * vim600: sw=4 ts=4 fdm=marker
 */
