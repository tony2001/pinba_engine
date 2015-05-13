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

#ifndef PINBA_H
#define PINBA_H

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

extern "C" {
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <event.h>
#include <math.h>
#include <sys/time.h>
#include <pthread.h>
#include <Judy.h>
#include <event.h>
}

#include "xxhash.h"
#include "pinba.pb-c.h"
#include "pinba_config.h"
#include "threadpool.h"
#include "pinba_types.h"

#undef P_SUCCESS
#undef P_FAILURE
#define P_SUCCESS 0
#define P_FAILURE -1


#define P_ERROR        (1<<0L)
#define P_WARNING      (1<<1L)
#define P_NOTICE       (1<<2L)
#define P_DEBUG        (1<<3L)
#define P_DEBUG_DUMP   (1<<4L)

char *pinba_error_ex(int return_error, int type, const char *file, int line, const char *format, ...);

#ifdef PINBA_DEBUG
#define pinba_debug(...) pinba_error_ex(0, P_DEBUG, __FILE__, __LINE__, __VA_ARGS__)
#else
#define pinba_debug(...)
#endif

#define pinba_warning(...) pinba_error_ex(0, P_WARNING, __FILE__, __LINE__, __VA_ARGS__)
#define pinba_error(type, ...) pinba_error_ex(0, type, __FILE__, __LINE__, __VA_ARGS__)
#define pinba_error_get(type, ...) pinba_error_ex(1, type, __FILE__, __LINE__, __VA_ARGS__)
extern pinba_daemon *D;

#ifdef __GNUC__
#define LIKELY(x)       __builtin_expect((x),1)
#define UNLIKELY(x)     __builtin_expect((x),0)
#else
#define LIKELY(x)       x
#define UNLIKELY(x)     x
#endif

void *pinba_data_main(void *arg);
void *pinba_collector_main(void *arg);
void *pinba_stats_main(void *arg);
int pinba_collector_init(pinba_daemon_settings settings);
void pinba_collector_shutdown();
int pinba_get_processors_number();

int pinba_get_time_interval(pinba_std_report *report);
int pinba_process_stats_packet(const unsigned char *buffer, int buffer_len);

void pinba_udp_read_callback_fn(int sock, short event, void *arg);
void pinba_socket_free(pinba_socket *socket);
pinba_socket *pinba_socket_open(char *ip, int listen_port);

void pinba_tag_dtor(pinba_tag *tag);
int pinba_tag_put(const unsigned char *name);
pinba_tag *pinba_tag_get_by_hash(size_t hash);
pinba_tag *pinba_tag_get_by_hash_next(size_t hash);
pinba_tag *pinba_tag_get_by_id(size_t id);

#include "pinba_update_report_proto.h"

void pinba_update_add(pinba_array_t *array, size_t request_id, const pinba_stats_record *record);
void pinba_update_delete(pinba_array_t *array, size_t request_id, const pinba_stats_record *record);
void pinba_reports_destroy(void);
void pinba_tag_reports_destroy(void);
void pinba_rtag_reports_destroy(void);
void pinba_std_report_dtor(void *rprt);
void pinba_report_dtor(pinba_report *report, int lock_reports);
void pinba_tag_report_dtor(pinba_tag_report *report, int lock_tag_reports);
void pinba_rtag_report_dtor(pinba_rtag_report *report, int lock);

void pinba_update_tag_info_add(size_t request_id, void *report, const pinba_stats_record *record);
void pinba_update_tag_info_delete(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_tag2_info_add(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_tag2_info_delete(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_tag_report_add(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_tag_report_delete(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_tag2_report_add(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_tag2_report_delete(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_tag_report2_add(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_tag_report2_delete(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_tag2_report2_add(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_tag2_report2_delete(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_tagN_info_add(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_tagN_info_delete(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_tagN_report_add(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_tagN_report_delete(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_tagN_report2_add(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_tagN_report2_delete(size_t request_id, void *rep, const pinba_stats_record *record);

void pinba_update_rtag_info_add(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_rtag_info_delete(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_rtag2_info_add(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_rtag2_info_delete(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_rtagN_info_add(size_t request_id, void *rep, const pinba_stats_record *record);
void pinba_update_rtagN_info_delete(size_t request_id, void *rep, const pinba_stats_record *record);

int pinba_array_add(pinba_array_t *array, void *tag_report);
int pinba_array_delete(pinba_array_t *array, void *tag_report);

/* go over all new records in the pool */
#define pool_traverse_forward(i, pool) \
		for (i = (pool)->out; i != (pool)->in; i = (i == (pool)->size - 1) ? 0 : i + 1)

/* go over all records in the pool */
#define pool_traverse_backward(i, pool) \
			for (i = ((pool)->in > 0) ? (pool)->in - 1 : 0; \
                 i != ((pool)->out ? (pool)->out : ((pool)->in ? ((pool)->size - 1) : 0)); \
                 i = (i == 0) ? ((pool)->size - 1) : i - 1)

#define TMP_POOL(pool) ((pinba_tmp_stats_record *)((pool)->data))
#define DATA_POOL(pool) ((pinba_data_bucket *)((pool)->data))
#define REQ_POOL(pool) ((pinba_stats_record *)((pool)->data))
#define REQ_POOL_EX(pool) ((pinba_stats_record_ex *)((pool)->data))
#define TIMER_POOL(pool) ((pinba_timer_record *)((pool)->data))
#define POOL_DATA(pool) ((void **)((pool)->data))

#define memcpy_static(buf, str, str_len, result_len)	\
do {										\
	if (sizeof(buf) <= (unsigned int)str_len) { 	\
		/* truncate the string */			\
		memcpy(buf, str, sizeof(buf) - 1);	\
		buf[sizeof(buf) - 1] = '\0';		\
		result_len = sizeof(buf) - 1;       \
	} else {								\
		memcpy(buf, str, str_len);			\
		buf[str_len] = '\0';				\
		result_len = str_len;               \
	}										\
} while(0)

#define memcat_static(buf, plus, str, str_len, result_len)	\
do {										\
	register unsigned int __n = sizeof(buf);			\
											\
	if ((unsigned int)(plus) >= __n) {		\
		break;								\
	}										\
											\
	if ((__n - (plus) - 1) < str_len) {		\
		/* truncate the string */			\
		memcpy(buf + (plus), str, __n - (plus)); \
		buf[__n - 1] = '\0';				\
		result_len = __n - 1;				\
	} else {								\
		memcpy(buf + (plus), str, str_len);	\
		buf[(plus) + str_len] = '\0';		\
		result_len = (plus) + str_len;		\
	}										\
} while(0)

size_t pinba_pool_num_records(pinba_pool *p);
int pinba_pool_init(pinba_pool *p, size_t size, size_t element_size, pool_dtor_func_t dtor);
int pinba_pool_grow(pinba_pool *p, size_t more);
void pinba_pool_destroy(pinba_pool *p);

/* utility macros */

#define timeval_to_float(tv) ((float)(tv).tv_sec + ((float)(tv).tv_usec / 1000000.0))

static inline struct timeval float_to_timeval(double f) /* {{{ */
{
	struct timeval t;
	double fraction, integral;

	fraction = modf(f, &integral);
	t.tv_sec = (int)integral;
	t.tv_usec = (int)(fraction*1000000);
	return t;
}
/* }}} */

#define pinba_pool_is_full(pool) ((pool->in < pool->out) ? pool->size - (pool->out - pool->in) : (pool->in - pool->out)) == (pool->size - 1)

#define record_get_timer(pool, record, i) (((record->timers_start + i) >= (pool)->size) ? (TIMER_POOL((pool)) + (record->timers_start + i - (pool)->size)) : (TIMER_POOL((pool)) + (record->timers_start + i)))
#define record_get_timer_id(pool, record, i) ((record->timers_start + i) >= (pool)->size) ? ((record->timers_start + i) - (pool)->size) : ((record->timers_start + i))

#define CHECK_REPORT_CONDITIONS_CONTINUE(report, record)																\
	if (report->flags & PINBA_REPORT_CONDITIONAL) {																		\
		if (report->cond.min_time > 0.0 && timeval_to_float(record->data.req_time) < report->cond.min_time) {			\
			continue;																									\
		}																												\
		if (report->cond.max_time > 0.0 && timeval_to_float(record->data.req_time) > report->cond.max_time) {			\
			continue;																									\
		}																												\
	}																													\
	if (report->flags & PINBA_REPORT_TAGGED) {																			\
		unsigned int t1, t2;																							\
		unsigned int found_tags = 0;																					\
																														\
		if (!record->data.tags_cnt) {																					\
			continue;																									\
		}																												\
																														\
		for (t1 = 0; t1 < report->cond.tags_cnt; t1++) {																\
			for (t2 = 0; t2 < record->data.tags_cnt; t2++) {															\
				if (strcmp(report->cond.tag_names[t1], record->data.tag_names[t2]->str) == 0) {							\
					if (strcmp(report->cond.tag_values[t1], record->data.tag_values[t2]->str) == 0) {					\
						found_tags++;																					\
					} else {																							\
						/* found wrong value for the tag, so there's no point to continue searching */					\
						goto skip;																						\
					}																									\
				}																										\
			}																											\
		}																												\
																														\
		skip:																											\
																														\
		if (found_tags != report->cond.tags_cnt) {																		\
			continue;																									\
		}																												\
	}

int pinba_timer_mutex_lock();
int pinba_timer_mutex_unlock();

void pinba_per_thread_request_pool_dtor(void *pool);
void pinba_data_pool_dtor(void *pool);
void pinba_temp_pool_dtor(void *pool);
void pinba_request_pool_dtor(void *pool);
void pinba_timer_pool_dtor(void *pool);

int timer_pool_add(int timers_cnt);

void update_reports_func(void *job_data);
void update_tag_reports_update_func(void *job_data);

static inline void pinba_update_histogram(pinba_std_report *report, int *histogram_data, const struct timeval *time, const int add) /* {{{ */
{
	unsigned int slot_num;
	float time_value = timeval_to_float(*time);

	if (add > 1) {
		time_value = time_value / add;
	} else if (add < -1) {
		time_value = time_value / -(add);
	}

	if (time_value > report->histogram_max_time) {
		slot_num = PINBA_HISTOGRAM_SIZE-1;
	} else {
		slot_num = time_value / report->histogram_segment;
		if (slot_num > PINBA_HISTOGRAM_SIZE-1) {
			slot_num = 0;
		}
	}

	histogram_data[slot_num] += add;
}
/* }}} */

#define PINBA_UPDATE_HISTOGRAM_ADD(report, data, value) pinba_update_histogram((pinba_std_report *)(report), (data), &(value), 1);
#define PINBA_UPDATE_HISTOGRAM_DEL(report, data, value) pinba_update_histogram((pinba_std_report *)(report), (data), &(value), -1);
#define PINBA_UPDATE_HISTOGRAM_ADD_EX(report, data, value, cnt) pinba_update_histogram((pinba_std_report *)(report), (data), &(value), (cnt));
#define PINBA_UPDATE_HISTOGRAM_DEL_EX(report, data, value, cnt) pinba_update_histogram((pinba_std_report *)(report), (data), &(value), -(cnt));

#define PINBA_REPORT_DELETE_CHECK(report, record) if (timercmp(&(report)->std.start, &(record)->time, >)) { return; }

#ifndef PINBA_ENGINE_HAVE_STRNDUP
char *pinba_strndup(const char *s, unsigned int length);
#define strndup pinba_strndup
#endif

#endif /* PINBA_H */

/*
 * vim600: sw=4 ts=4 fdm=marker
 */
