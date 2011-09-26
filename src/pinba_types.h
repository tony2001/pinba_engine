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

enum {
	PINBA_BASE_REPORT_INFO,
	PINBA_BASE_REPORT1,
	PINBA_BASE_REPORT2,
	PINBA_BASE_REPORT3,
	PINBA_BASE_REPORT4,
	PINBA_BASE_REPORT5,
	PINBA_BASE_REPORT6,
	PINBA_BASE_REPORT7,
	PINBA_BASE_REPORT_LAST
};

enum {
	PINBA_TAG_REPORT_INFO,
	PINBA_TAG2_REPORT_INFO,
	PINBA_TAG_REPORT,
	PINBA_TAG2_REPORT,
	PINBA_TAG_REPORT2,
	PINBA_TAG2_REPORT2,
	PINBA_TAG_REPORT_LAST
};

enum {
	PINBA_REPORT_REGULAR = 1<<0,
	PINBA_REPORT_CONDITIONAL = 1<<1
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
	Pinba::Request request;
	struct timeval time;
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

typedef struct _pinba_report { /* {{{ */
	time_t time_interval;
	size_t results_cnt;
	Pvoid_t results;
	struct timeval time_total;
	double kbytes_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	pthread_rwlock_t lock;
	int flags;
/*	struct {
		double min_time;
		double max_time;
	} cond; */
} pinba_report;
/* }}} */

typedef struct _pinba_tag_report { /* {{{ */
	char tag1[PINBA_TAG_NAME_SIZE];
	char tag2[PINBA_TAG_NAME_SIZE];
	int tag1_id;
	int tag2_id;
	time_t time_interval;
	time_t last_requested;
	size_t results_cnt;
	Pvoid_t results;
	int type;
	pthread_rwlock_t lock;
	int flags;
	struct {
		double min_time;
		double max_time;
	} cond;
} pinba_tag_report;
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
	pthread_rwlock_t timer_lock;
	pinba_socket *collector_socket;
	struct event_base *base;
	pinba_pool temp_pool;
	pinba_pool data_pool;
	pinba_pool request_pool;
	pinba_pool timer_pool;
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
	pinba_report base_reports[PINBA_BASE_REPORT_LAST];
	Pvoid_t tag_reports;
	thread_pool_t *thread_pool;
} pinba_daemon;
/* }}} */

struct pinba_report1_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
};
/* }}} */

struct pinba_report2_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
};
/* }}} */

struct pinba_report3_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
};
/* }}} */

struct pinba_report4_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	char server_name[PINBA_SERVER_NAME_SIZE];
	char script_name[PINBA_SCRIPT_NAME_SIZE];
};
/* }}} */

struct pinba_report5_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	char hostname[PINBA_HOSTNAME_SIZE];
	char script_name[PINBA_SCRIPT_NAME_SIZE];
};
/* }}} */

struct pinba_report6_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	char hostname[PINBA_HOSTNAME_SIZE];
	char server_name[PINBA_SERVER_NAME_SIZE];
};
/* }}} */

struct pinba_report7_data { /* {{{ */
	size_t req_count;
	struct timeval req_time_total;
	struct timeval ru_utime_total;
	struct timeval ru_stime_total;
	double kbytes_total;
	char hostname[PINBA_HOSTNAME_SIZE];
	char server_name[PINBA_SERVER_NAME_SIZE];
	char script_name[PINBA_SCRIPT_NAME_SIZE];
};
/* }}} */

struct pinba_tag_info_data { /* {{{ */
	size_t req_count;
	size_t hit_count;
	struct timeval timer_value;
	int prev_add_request_id;
	int prev_del_request_id;
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
};
/* }}} */



#endif /* PINBA_TYPES_H */

/* 
 * vim600: sw=4 ts=4 fdm=marker
 */
