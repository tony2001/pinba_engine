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

#ifndef PINBA_TYPES_H
#define PINBA_TYPES_H

/* max index string length */
#define PINBA_MAX_LINE_LEN 8192

/* these must not be greater than 255! */
#define PINBA_HOSTNAME_SIZE 33
#define PINBA_SERVER_NAME_SIZE 33
#define PINBA_SCRIPT_NAME_SIZE 129
#define PINBA_STATUS_SIZE 33
#define PINBA_SCHEMA_SIZE 17

#define PINBA_TAG_NAME_SIZE 65
#define PINBA_TAG_VALUE_SIZE 65

#define PINBA_ERR_BUFFER 2048

#define PINBA_UDP_BUFFER_SIZE 65536

#define PINBA_DICTIONARY_GROW_SIZE 32
#define PINBA_TIMER_POOL_GROW_SIZE 2621440
#define PINBA_TIMER_POOL_SHRINK_SIZE PINBA_TIMER_POOL_GROW_SIZE*5

#define PINBA_THREAD_POOL_DEFAULT_SIZE 8
#define PINBA_THREAD_POOL_THRESHOLD_AMOUNT 16
#define PINBA_MIN_TAG_VALUES_CNT_MAGIC_NUMBER 8
#define PINBA_PER_THREAD_POOL_GROW_SIZE 1024
#define PINBA_TEMP_DICTIONARY_SIZE 1024

#define PINBA_HISTOGRAM_SIZE 512

enum {
	PINBA_TABLE_UNKNOWN,
	PINBA_TABLE_REQUEST,
	PINBA_TABLE_TIMER,
	PINBA_TABLE_TIMERTAG,
	PINBA_TABLE_TAG,
	PINBA_TABLE_REPORT_INFO,
	PINBA_TABLE_REPORT1, /* group by script_name */
	PINBA_TABLE_REPORT2, /* group by virtual host */
	PINBA_TABLE_REPORT3, /* group by hostname */
	PINBA_TABLE_REPORT4, /* group by virtual host, script_name */
	PINBA_TABLE_REPORT5, /* group by hostname, script_name */
	PINBA_TABLE_REPORT6, /* group by hostname, virtual_host */
	PINBA_TABLE_REPORT7, /* group by hostname, virtual_host and script_name */
	PINBA_TABLE_REPORT8, /* group by status */
	PINBA_TABLE_REPORT9, /* group by script_name and status */
	PINBA_TABLE_REPORT10, /* group by virtual_host and status */
	PINBA_TABLE_REPORT11, /* group by hostname and status */
	PINBA_TABLE_REPORT12, /* group by hostname, script_name and status */
	PINBA_TABLE_REPORT13, /* group by schema */
	PINBA_TABLE_REPORT14, /* group by schema and script_name */
	PINBA_TABLE_REPORT15, /* group by schema and server_name */
	PINBA_TABLE_REPORT16, /* group by schema and hostname */
	PINBA_TABLE_REPORT17, /* group by schema, hostname and script_name */
	PINBA_TABLE_REPORT18, /* group by schema, hostname and status */
	PINBA_TABLE_TAG_INFO, /* tag report grouped by custom tag */
	PINBA_TABLE_TAG2_INFO, /* tag report grouped by 2 custom tags */
	PINBA_TABLE_TAG_REPORT, /* tag report grouped by script_name and custom tag */
	PINBA_TABLE_TAG2_REPORT, /* tag report grouped by script_name and 2 custom tags */
	PINBA_TABLE_TAG_REPORT2, /* tag report grouped by script_name, host_name, server_name and custom tag */
	PINBA_TABLE_TAG2_REPORT2 /* tag report grouped by script_name, host_name, server_name and 2 custom tags */
};

#define PINBA_TABLE_REPORT_LAST PINBA_TABLE_REPORT18

enum {
	PINBA_REPORT_REGULAR = 1<<0,
	PINBA_REPORT_CONDITIONAL = 1<<1,
	PINBA_REPORT_TAGGED = 1<<2
};

typedef struct _pinba_socket { /* {{{ */
	int listen_sock;
	struct event *accept_event;
} pinba_socket;
/* }}} */

#if 0
typedef struct _struct timeval { /* {{{ */
	int tv_sec;
	int tv_usec;
} struct timeval;
/* }}} */
#endif

typedef struct _pinba_word { /* {{{ */
	char *str;
	unsigned char len;
} pinba_word;
/* }}} */

typedef struct _pinba_timer_record { /* {{{ */
	struct timeval value;
	int *tag_ids;
	pinba_word **tag_values;
	unsigned short tag_num;
	unsigned short tag_num_allocated;
	int hit_count;
	int index;
	int request_id;
	unsigned short num_in_request;
} pinba_timer_record;
/* }}} */

typedef struct _pinba_tmp_stats_record { /* {{{ */
	Pinba__Request *request;
	struct timeval time;
	int free:1;
} pinba_tmp_stats_record;
/* }}} */

typedef struct _pinba_stats_record { /* {{{ */
	struct {
		char script_name[PINBA_SCRIPT_NAME_SIZE];
		char server_name[PINBA_SERVER_NAME_SIZE];
		char hostname[PINBA_HOSTNAME_SIZE];
		struct timeval req_time;
		struct timeval ru_utime;
		struct timeval ru_stime;
		unsigned char script_name_len;
		unsigned char server_name_len;
		unsigned char hostname_len;
		unsigned int req_count;
		float doc_size;
		float mem_peak_usage;
		unsigned short status;
		float memory_footprint;
		char schema[PINBA_SCHEMA_SIZE];
		unsigned char schema_len;
		char **tag_names; //PINBA_TAG_NAME_SIZE applies here
		char **tag_values; //PINBA_TAG_VALUE_SIZE applies here
		unsigned int tags_cnt;
		unsigned int tags_alloc_cnt;
	} data;
	struct timeval time;
	unsigned int timers_start;
	unsigned short timers_cnt;
} pinba_stats_record;
/* }}} */

typedef void (*pool_dtor_func_t)(void *pool);

typedef struct _pinba_pool { /* {{{ */
	size_t size;
	size_t element_size;
	pool_dtor_func_t dtor;
	size_t in;
	size_t out;
	void **data;
} pinba_pool;
/* }}} */

typedef struct _pinba_tag { /* {{{ */
	size_t id;
	char name[PINBA_TAG_NAME_SIZE];
	unsigned char name_len;
} pinba_tag;
/* }}} */

typedef struct _pinba_conditions {
	double min_time;
	double max_time;
	unsigned int tags_cnt;
	char **tag_names;
	char **tag_values;
} pinba_conditions;

typedef struct _pinba_std_report {
	pinba_conditions cond;
	int flags;
	int type;
	int histogram_max_time;
	float histogram_segment;
	int histogram_data[PINBA_HISTOGRAM_SIZE];
} pinba_std_report;

typedef struct _pinba_report pinba_report;
typedef void (pinba_report_update_function)(pinba_report *report, const pinba_stats_record *record);

struct _pinba_report { /* {{{ */
	pinba_std_report std;
	time_t time_interval;
	size_t results_cnt;
	Pvoid_t results;
	struct timeval time_total;
	double kbytes_total;
	double memory_footprint;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	pthread_rwlock_t lock;
	pinba_report_update_function *add_func;
	pinba_report_update_function *delete_func;
};
/* }}} */

typedef struct _pinba_tag_report pinba_tag_report;
typedef void (pinba_tag_report_update_function)(int request_id, pinba_tag_report *report, const pinba_stats_record *record);

struct _pinba_tag_report { /* {{{ */
	pinba_std_report std;
	char tag1[PINBA_TAG_NAME_SIZE];
	char tag2[PINBA_TAG_NAME_SIZE];
	int tag1_id;
	int tag2_id;
	time_t time_interval;
	time_t last_requested;
	size_t results_cnt;
	Pvoid_t results;
	pthread_rwlock_t lock;
	pinba_tag_report_update_function *add_func;
	pinba_tag_report_update_function *delete_func;
};
/* }}} */

typedef struct _pinba_daemon_settings { /* {{{ */
	int port;
	int stats_history;
	int stats_gathering_period;
	int request_pool_size;
	int temp_pool_size;
	int tag_report_timeout;
	int show_protobuf_errors;
	char *address;
} pinba_daemon_settings;
/* }}} */

typedef struct _pinba_data_bucket { /* {{{ */
	char *buf;
	int len;
	int alloc_len;
} pinba_data_bucket;
/* }}} */

typedef struct _pinba_daemon { /* {{{ */
	pthread_rwlock_t collector_lock;
	pthread_rwlock_t temp_lock;
	pthread_rwlock_t data_lock;
	pthread_rwlock_t tag_reports_lock;
	pthread_rwlock_t base_reports_lock;
	pthread_rwlock_t timer_lock;
	pinba_socket *collector_socket;
	struct event_base *base;
	pinba_pool temp_pool;
	pinba_pool data_pool;
	pinba_pool request_pool;
	pinba_pool timer_pool;
	pinba_pool *per_thread_temp_pools;
	pinba_pool *per_thread_request_pools;
	struct {
		pinba_word **table;
		Pvoid_t word_index;
		size_t count;
		size_t size;
	} dict;
	size_t timertags_cnt;
	struct {
		Pvoid_t table; /* ID -> NAME */
		Pvoid_t name_index; /* NAME -> */
	} tag;
	pinba_daemon_settings settings;
	Pvoid_t base_reports;
	void **base_reports_arr;
	int base_reports_arr_size;
	Pvoid_t tag_reports;
	void **tag_reports_arr;
	int tag_reports_arr_size;
	thread_pool_t *thread_pool;
} pinba_daemon;
/* }}} */

struct pinba_report1_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	double memory_footprint;
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_report2_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	double memory_footprint;
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_report3_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	double memory_footprint;
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_report4_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	double memory_footprint;
	char server_name[PINBA_SERVER_NAME_SIZE];
	char script_name[PINBA_SCRIPT_NAME_SIZE];
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_report5_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	double memory_footprint;
	char hostname[PINBA_HOSTNAME_SIZE];
	char script_name[PINBA_SCRIPT_NAME_SIZE];
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_report6_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	double memory_footprint;
	char hostname[PINBA_HOSTNAME_SIZE];
	char server_name[PINBA_SERVER_NAME_SIZE];
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_report7_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	double memory_footprint;
	char hostname[PINBA_HOSTNAME_SIZE];
	char server_name[PINBA_SERVER_NAME_SIZE];
	char script_name[PINBA_SCRIPT_NAME_SIZE];
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_report8_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	double memory_footprint;
	int status;
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_report9_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	double memory_footprint;
	int status;
	char script_name[PINBA_SCRIPT_NAME_SIZE];
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_report10_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	double memory_footprint;
	int status;
	char server_name[PINBA_SERVER_NAME_SIZE];
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_report11_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	double memory_footprint;
	int status;
	char hostname[PINBA_HOSTNAME_SIZE];
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_report12_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	double memory_footprint;
	int status;
	char hostname[PINBA_HOSTNAME_SIZE];
	char script_name[PINBA_SCRIPT_NAME_SIZE];
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_report13_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	double memory_footprint;
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_report14_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	double memory_footprint;
	char schema[PINBA_SCHEMA_SIZE];
	char script_name[PINBA_SCRIPT_NAME_SIZE];
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_report15_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	double memory_footprint;
	char schema[PINBA_SCHEMA_SIZE];
	char server_name[PINBA_SERVER_NAME_SIZE];
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_report16_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	double memory_footprint;
	char schema[PINBA_SCHEMA_SIZE];
	char hostname[PINBA_HOSTNAME_SIZE];
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_report17_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	double memory_footprint;
	char schema[PINBA_SCHEMA_SIZE];
	char hostname[PINBA_HOSTNAME_SIZE];
	char script_name[PINBA_SCRIPT_NAME_SIZE];
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_report18_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	double memory_footprint;
	int status;
	char schema[PINBA_SCHEMA_SIZE];
	char hostname[PINBA_HOSTNAME_SIZE];
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_tag_info_data { /* {{{ */
	size_t req_count;
	size_t hit_count;
	struct timeval timer_value;
	int prev_add_request_id;
	int prev_del_request_id;
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_tag2_info_data { /* {{{ */
	size_t req_count;
	size_t hit_count;
	struct timeval timer_value;
	char tag1_value[PINBA_TAG_VALUE_SIZE];
	char tag2_value[PINBA_TAG_VALUE_SIZE];
	int prev_add_request_id;
	int prev_del_request_id;
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_tag_report_data { /* {{{ */
	size_t req_count;
	size_t hit_count;
	struct timeval timer_value;
	char script_name[PINBA_SCRIPT_NAME_SIZE];
	char tag_value[PINBA_TAG_VALUE_SIZE];
	int prev_add_request_id;
	int prev_del_request_id;
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_tag2_report_data { /* {{{ */
	size_t req_count;
	size_t hit_count;
	struct timeval timer_value;
	char script_name[PINBA_SCRIPT_NAME_SIZE];
	char tag1_value[PINBA_TAG_VALUE_SIZE];
	char tag2_value[PINBA_TAG_VALUE_SIZE];
	int prev_add_request_id;
	int prev_del_request_id;
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_tag_report2_data { /* {{{ */
	size_t req_count;
	size_t hit_count;
	struct timeval timer_value;
	char hostname[PINBA_HOSTNAME_SIZE];
	char server_name[PINBA_SERVER_NAME_SIZE];
	char script_name[PINBA_SCRIPT_NAME_SIZE];
	char tag_value[PINBA_TAG_VALUE_SIZE];
	int prev_add_request_id;
	int prev_del_request_id;
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */

struct pinba_tag2_report2_data { /* {{{ */
	size_t req_count;
	size_t hit_count;
	struct timeval timer_value;
	char hostname[PINBA_HOSTNAME_SIZE];
	char server_name[PINBA_SERVER_NAME_SIZE];
	char script_name[PINBA_SCRIPT_NAME_SIZE];
	char tag1_value[PINBA_TAG_VALUE_SIZE];
	char tag2_value[PINBA_TAG_VALUE_SIZE];
	int prev_add_request_id;
	int prev_del_request_id;
	int histogram_data[PINBA_HISTOGRAM_SIZE];
};
/* }}} */



#endif /* PINBA_TYPES_H */

/*
 * vim600: sw=4 ts=4 fdm=marker
 */
