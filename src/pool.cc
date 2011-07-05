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

/* generic pool functions */

size_t pinba_pool_num_records(pinba_pool *p) /* {{{ */
{
	size_t result;
	
	if (p->in == p->out) {
		return 0;
	} else if (p->in > p->out) {
		result = p->in - p->out;
	} else {
		result = p->size - (p->out - p->in);
	}
	
	return result;
}
/* }}} */

static inline int pinba_pool_grow(pinba_pool *p, size_t more) /* {{{ */
{
	size_t old_size = p->size;

	p->size += more; /* +more elements*/

	if (p->size <= 0) {
		return P_FAILURE;
	}

	p->data = (void **)realloc(p->data, p->size * p->element_size);

	if (!p->data) {
		return P_FAILURE;
	}

	if (p->out > p->in) {
		memmove((char *)p->data + p->out*p->element_size + more*p->element_size, (char *)p->data + p->out*p->element_size, (old_size - p->out) * p->element_size);
		p->out += more;
	}
	/* initialize memory */
	memset((char *)p->data + p->in*p->element_size, 0, more * p->element_size);

	return P_SUCCESS;
}
/* }}} */

static inline int pinba_pool_shrink(pinba_pool *p, size_t less) /* {{{ */
{
	size_t old_size = p->size;

	if (old_size <= less) {
		return P_FAILURE;
	}

	pinba_debug("shrinking pool (in: %ld, out: %ld, taken: %ld, empty: %ld) from %ld to %ld", p->in, p->out, pinba_pool_num_records(p), p->size - p->in, old_size, p->size - less);

	p->size -= less; /* -less elements*/
	p->data = (void **)realloc(p->data, p->size * p->element_size);

	if (!p->data) {
		return P_FAILURE;
	}
	return P_SUCCESS;
}
/* }}} */

int pinba_pool_init(pinba_pool *p, size_t size, size_t element_size, pool_dtor_func_t dtor) /* {{{ */
{
	memset(p, 0, sizeof(pinba_pool));
	p->element_size = element_size;
	p->dtor = dtor;
	return pinba_pool_grow(p, size);
}
/* }}} */

void pinba_pool_destroy(pinba_pool *p) /* {{{ */
{
	if (p->data) {
		if (p->dtor) {
			p->dtor(p);
		}

		free(p->data);
		p->data = NULL;
	}
}
/* }}} */

/* stats pool functions */

static inline void pinba_stats_record_dtor(int request_id, pinba_stats_record *record) /* {{{ */
{
	int i;
	pinba_pool *timer_pool = &D->timer_pool;

	if (!record->time.tv_sec) {
		return;
	}

	pinba_update_reports_delete(record);

	pthread_rwlock_rdlock(&D->tag_reports_lock);
	pthread_rwlock_wrlock(&D->timer_lock);
	if (record->timers_cnt > 0) {
		pinba_timer_record *timer;
		int tag_sum = 0;

		pinba_update_tag_reports_delete(request_id, record);

		for (i = 0; i < record->timers_cnt; i++) {
			timer = record_get_timer(&D->timer_pool, record, i);
			if (UNLIKELY(timer_pool->out == (timer_pool->size - 1))) {
				timer_pool->out = 0;
			} else {
				timer_pool->out++;
			}

			tag_sum += timer->tag_num;
			D->timertags_cnt -= timer->tag_num;
		}

		record->timers_cnt = 0;
	}
	record->time.tv_sec = 0;
	pthread_rwlock_unlock(&D->timer_lock);
	pthread_rwlock_unlock(&D->tag_reports_lock);
}
/* }}} */

struct delete_job_data {
	int start;
	int end;
	int tags_cnt;
};

void pinba_temp_pool_dtor(void *pool) /* {{{ */
{
	pinba_pool *p = (pinba_pool *)pool; 
	unsigned int i;
	pinba_tmp_stats_record *tmp_record;

	for (i = 0; i < p->size; i++) {
		tmp_record = TMP_POOL(p) + i;
		tmp_record->time.tv_sec = 0;
		tmp_record->request.~Request();
	}
}
/* }}} */

void pinba_data_pool_dtor(void *pool) /* {{{ */
{
	pinba_pool *p = (pinba_pool *)pool; 
	unsigned int i;
	pinba_data_bucket *bucket;

	for (i = 0; i < p->size; i++) {
		bucket = DATA_POOL(p) + i;
		if (bucket->buf) {
			free(bucket->buf);
			bucket->buf = NULL;
			bucket->len = 0;
		}
	}
}
/* }}} */

void pinba_request_pool_dtor(void *pool) /* {{{ */
{
	pinba_pool *p = (pinba_pool *)pool;
	unsigned int i;
	pinba_stats_record *record;

	if (pinba_pool_num_records(p) > 0) {
		pool_traverse_forward(i, p) {
			record = REQ_POOL(p) + i;
			pinba_stats_record_dtor(i, record);
		}
	}
}
/* }}} */

void pinba_timer_pool_dtor(void *pool) /* {{{ */
{
	pinba_pool *p = (pinba_pool *)pool;
	unsigned int i;
	pinba_timer_record *timer;

	for (i = 0; i < p->size; i++) {
		timer = TIMER_POOL(p) + i;
		if (timer->tag_ids) {
			free(timer->tag_ids);
		}
		if (timer->tag_values) {
			free(timer->tag_values);
		}
	}
}
/* }}} */

int timer_pool_add(int timers_cnt) /* {{{ */
{
	int id;
	pinba_pool *timer_pool = &D->timer_pool;

	if ((pinba_pool_num_records(timer_pool) + timers_cnt) >= (timer_pool->size - 1)) {
		pinba_debug("growing from %ld to %ld; in: %ld, out: %ld", timer_pool->size, timer_pool->size + PINBA_TIMER_POOL_GROW_SIZE, timer_pool->in, timer_pool->out);
		pinba_pool_grow(timer_pool, PINBA_TIMER_POOL_GROW_SIZE);

		if (timer_pool->out > timer_pool->in) {
			int i, prev_request_id = -1;
			pinba_stats_record *record;
			pinba_timer_record *timer;
			pinba_pool *request_pool = &D->request_pool;

			for (i = timer_pool->out; i < timer_pool->size; i++) {
				timer = TIMER_POOL(timer_pool) + i;

				if (timer->tag_num == 0) { 
					continue;
				}
				
				if (timer->request_id == prev_request_id) {
					continue;
				}

				record = REQ_POOL(request_pool) + timer->request_id;
				record->timers_start += PINBA_TIMER_POOL_GROW_SIZE;

				prev_request_id = timer->request_id;
			}
		}
	}

	id = timer_pool->in;

	if (UNLIKELY((timer_pool->in + timers_cnt) >= (timer_pool->size - 1))) {
		timer_pool->in = (timer_pool->in + timers_cnt) - (timer_pool->size - 1);
	} else {
		timer_pool->in += timers_cnt;
	}

	return id;
}
/* }}} */

struct timers_job_data {
	pinba_stats_record *record;
	Pinba::Request *request;
	int request_id;
	int dict_size;
};

#define PINBA_MIN_TAG_VALUES_CNT_MAGIC_NUMBER 8

pthread_mutex_t timertag_mutex = PTHREAD_MUTEX_INITIALIZER;
int g_timer_cnt;
int g_timertag_cnt;

void add_timers_func(void *job_data) /* {{{ */
{
	pinba_pool *timer_pool = &D->timer_pool;
	struct timers_job_data *d = (struct timers_job_data *)job_data;
	pinba_stats_record *record = d->record;
	Pinba::Request *request = d->request;
	pinba_timer_record *timer;
	unsigned int timers_cnt = record->timers_cnt;
	float timer_value;
	unsigned int i, j, timer_tag_cnt, timer_hit_cnt, tag_value, tag_name;
	unsigned int ti = 0, tt = 0;
	PPvoid_t ppvalue;
	Word_t word_id, tag_id;
	pinba_word *word_ptr;
	string *str;
	pinba_tag *tag;
	int res;

	pthread_rwlock_rdlock(&D->timer_lock);
	record->timers_cnt = 0;

	/* add timers to the timers hash */
	for (i = 0; i < timers_cnt; i++, ti++) {
		timer_value = request->timer_value(ti);
		timer_tag_cnt = request->timer_tag_count(ti);
		timer_hit_cnt = request->timer_hit_count(ti);

		if (timer_value < 0) {
			pinba_debug("internal error: timer.value is negative");
			continue;
		}

		if (timer_hit_cnt < 0) {
			pinba_debug("internal error: timer.hit_count is negative");
			continue;
		}

		if (timer_tag_cnt <= 0) {
			pinba_debug("internal error: timer.tag_count is invalid");
			continue;
		}

		timer = record_get_timer(timer_pool, record, i);
		timer->index = record_get_timer_id(timer_pool, record, i);

		if (timer->tag_num_allocated < timer_tag_cnt) {
			int allocate_num = timer_tag_cnt;
			if (timer_tag_cnt < PINBA_MIN_TAG_VALUES_CNT_MAGIC_NUMBER) {
				allocate_num = PINBA_MIN_TAG_VALUES_CNT_MAGIC_NUMBER;
			}
			timer->tag_ids = (int *)realloc(timer->tag_ids, sizeof(int) * allocate_num);
			timer->tag_values = (pinba_word **)realloc(timer->tag_values, sizeof(pinba_word *) * allocate_num);
			timer->tag_num_allocated = allocate_num;
		}

		timer->value = float_to_timeval(timer_value);
		timer->hit_count = timer_hit_cnt;
		timer->num_in_request = record->timers_cnt;
		timer->request_id = d->request_id;

		if (!timer->tag_ids || !timer->tag_values) {
			timer->tag_num_allocated = 0;
			pinba_debug("out of memory when allocating tag attributes");
			continue;
		}

		record->timers_cnt++;
		timer->tag_num = 0;

		for (j = 0; j < timer_tag_cnt; j++, tt++) {

			tag_value = request->timer_tag_value(tt);
			tag_name = request->timer_tag_name(tt);

			timer->tag_values[j] = NULL;

			if (LIKELY(tag_value < d->dict_size && tag_name < d->dict_size && tag_value >= 0 && tag_name >= 0)) {
				str = request->mutable_dictionary(tag_value);
			} else {
				pinba_debug("tag_value >= dict_size || tag_name >= dict_size");
				continue;
			}

			pthread_mutex_lock(&timertag_mutex);
			ppvalue = JudySLGet(D->dict.word_index, (uint8_t *)str->c_str(), NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				pinba_word *word;
				int len;

				word = (pinba_word *)malloc(sizeof(*word));

				/* insert */
				word_id = D->dict.count;
				if (word_id == D->dict.size) {
					D->dict.table = (pinba_word **)realloc(D->dict.table, sizeof(pinba_word *) * (D->dict.size + PINBA_DICTIONARY_GROW_SIZE));
					D->dict.size += PINBA_DICTIONARY_GROW_SIZE;
				}

				D->dict.table[word_id] = word;
				len = str->size();
				word->len = (len >= PINBA_TAG_VALUE_SIZE) ? PINBA_TAG_VALUE_SIZE - 1 : len;
				word->str = strndup(str->c_str(), word->len);
				word_ptr = word;

				ppvalue = JudySLIns(&D->dict.word_index, (uint8_t *)str->c_str(), NULL);
				if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
					/* well.. too bad.. */
					free(D->dict.table[word_id]);
					pinba_debug("failed to insert new value into word_index");
					pthread_mutex_unlock(&timertag_mutex);
					continue;
				}
				*ppvalue = (void *)word_id;
				D->dict.count++;
			} else {
				word_id = (Word_t)*ppvalue;
				if (LIKELY(word_id >= 0 && word_id < D->dict.count)) {
					word_ptr = D->dict.table[word_id];
				} else {
					pinba_debug("invalid word_id");
					pthread_mutex_unlock(&timertag_mutex);
					continue;
				}
			}

			timer->tag_values[j] = word_ptr;

			str = request->mutable_dictionary(tag_name);

			ppvalue = JudySLGet(D->tag.name_index, (uint8_t *)str->c_str(), NULL);

			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				/* doesn't exist, create */
				int dummy;

				tag_id = 0;

				/* get the first empty ID */
				res = JudyLFirstEmpty(D->tag.table, &tag_id, NULL);
				if (res < 0) {
					pinba_debug("no empty indexes in tag.table");
					pthread_mutex_unlock(&timertag_mutex);
					continue;
				}

				tag = (pinba_tag *)malloc(sizeof(pinba_tag));
				if (!tag) {
					pinba_debug("failed to allocate tag");
					pthread_mutex_unlock(&timertag_mutex);
					continue;
				}

				tag->id = tag_id;
				memcpy_static(tag->name, str->c_str(), str->size(), dummy);
				tag->name_len = str->size();

				/* add the tag to the table */
				ppvalue = JudyLIns(&D->tag.table, tag_id, NULL);
				if (!ppvalue || ppvalue == PJERR) {
					free(tag);
					pinba_debug("failed to insert tag into tag.table");
					pthread_mutex_unlock(&timertag_mutex);
					continue;
				}
				*ppvalue = tag;

				/* add the tag to the index */
				ppvalue = JudySLIns(&D->tag.name_index, (uint8_t *)str->c_str(), NULL);
				if (UNLIKELY(ppvalue == PJERR)) {
					JudyLDel(&D->tag.table, tag_id, NULL);
					free(tag);
					pinba_debug("failed to insert tag into tag.name_index");
					pthread_mutex_unlock(&timertag_mutex);
					continue;
				} else {
					*ppvalue = tag;
				}
			} else {
				tag = (pinba_tag *)*ppvalue;
				tag_id = tag->id;
			}

			timer->tag_ids[j] = tag_id;
			timer->tag_num++;
			D->timertags_cnt++;
			pthread_mutex_unlock(&timertag_mutex);
		}
	}
	pinba_update_tag_reports_add(d->request_id, record);
	pthread_rwlock_unlock(&D->timer_lock);
	free(d);
}
/* }}} */

void delete_timers_func(void *job_data) /* {{{ */
{
	pinba_pool *timer_pool = &D->timer_pool;
	struct timers_job_data *d = (struct timers_job_data *)job_data;
	pinba_stats_record *record = d->record;
	pinba_timer_record *timer;
	int j, tags_cnt = 0;

	pinba_update_tag_reports_delete(d->request_id, record);

	for (j = 0; j < record->timers_cnt; j++) {
		timer = record_get_timer(timer_pool, record, j);
		tags_cnt += timer->tag_num;
		timer->tag_num = 0;
		timer->value.tv_sec = 0;
		timer->value.tv_usec = 0;
		timer->hit_count = 0;
	}

	pthread_mutex_lock(&timertag_mutex);
	g_timer_cnt += record->timers_cnt;
	g_timertag_cnt += tags_cnt;
	record->timers_cnt = 0;
	pthread_mutex_unlock(&timertag_mutex);
	
	free(d);
}
/* }}} */

#define PINBA_MAGIC_THREAD_NUMBER 20

inline void pinba_request_pool_delete_old(struct timeval from) /* {{{ */
{
	pinba_pool *p = &D->request_pool;
	pinba_pool *timer_pool = &D->timer_pool;
	pinba_stats_record *record;
	int i, job_count = 0;
	thread_pool_barrier_t barrier;

	pthread_rwlock_rdlock(&D->tag_reports_lock);
	pthread_rwlock_wrlock(&D->timer_lock);

	th_pool_barrier_init(&barrier);
	th_pool_barrier_start(&barrier);

	g_timer_cnt = 0;
	g_timertag_cnt = 0;

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (timercmp(&record->time, &from, <)) {
			if (record->timers_cnt > 0) {
				struct timers_job_data *job_data;

				job_data = (struct timers_job_data *)malloc(sizeof(struct timers_job_data));
				job_data->record = record;
				job_data->request_id = i;
				if (record->timers_cnt > PINBA_MAGIC_THREAD_NUMBER) {
					th_pool_dispatch(D->thread_pool, &barrier, delete_timers_func, job_data);
					//job_count++;
				} else {
					delete_timers_func(job_data);
				}
			}

			pinba_update_reports_delete(record);

			record->time.tv_sec = 0;

			if (p->out == (p->size - 1)) {
				p->out = 0;
			} else {
				p->out++;
			}
		} else {
			break;
		}
	}

//	pinba_debug("delete job_count: %d", job_count);
	th_pool_barrier_end(&barrier);
	
	if ((timer_pool->out + g_timer_cnt) >= (timer_pool->size - 1)) {
		timer_pool->out = (timer_pool->out + g_timer_cnt) - (timer_pool->size - 1);
	} else {
		timer_pool->out += g_timer_cnt;
	}
	D->timertags_cnt -= g_timertag_cnt;
	pthread_rwlock_unlock(&D->timer_lock);
	pthread_rwlock_unlock(&D->tag_reports_lock);
}
/* }}} */

void update_timer_reports_func(void *job_data) /* {{{ */
{
	struct timers_job_data *d = (struct timers_job_data *)job_data;
	pinba_stats_record *record = d->record;
	
	if (!record->timers_cnt) {
		return;
	}

	pinba_update_tag_reports_add(d->request_id, record);
}
/* }}} */

inline void pinba_merge_pools(void) /* {{{ */
{
	unsigned int k;
	pinba_pool *temp_pool = &D->temp_pool;
	pinba_pool *request_pool = &D->request_pool;
	Pinba::Request *request;
	pinba_tmp_stats_record *tmp_record;
	pinba_stats_record *record;
	unsigned int timers_cnt, dict_size;
	double req_time, ru_utime, ru_stime, doc_size;
	struct timers_job_data *job_data;
	int job_count = 0;
	thread_pool_barrier_t barrier;

	pthread_rwlock_rdlock(&D->tag_reports_lock);

	th_pool_barrier_init(&barrier);
	th_pool_barrier_start(&barrier);

	/* we start with the last record, which should be already empty at the moment */
	pool_traverse_forward(k, temp_pool) {
		record = REQ_POOL(request_pool) + request_pool->in;
		tmp_record = TMP_POOL(temp_pool) + k;

		memset(record, 0, sizeof(*record));
		record->time.tv_sec = tmp_record->time.tv_sec;
		record->time.tv_usec = tmp_record->time.tv_usec;
		request = &tmp_record->request;

		timers_cnt = request->timer_hit_count_size();
		if (timers_cnt != (unsigned int)request->timer_value_size() || timers_cnt != (unsigned int)request->timer_tag_count_size()) {
			pinba_debug("internal error: timer_hit_count_size != timer_value_size || timer_hit_count_size != timer_tag_count_size");
			continue;
		}

		dict_size = request->dictionary_size(); 
		if (dict_size == 0 && timers_cnt > 0) {
			pinba_debug("internal error: dict_size == 0, but timers_cnt > 0");
			continue;
		}

		memcpy_static(record->data.script_name, request->script_name().c_str(), request->script_name().size(), record->data.script_name_len);
		memcpy_static(record->data.server_name, request->server_name().c_str(), request->server_name().size(), record->data.server_name_len);
		memcpy_static(record->data.hostname, request->hostname().c_str(), request->hostname().size(), record->data.hostname_len);
		req_time = (double)request->request_time();
		ru_utime = (double)request->ru_utime();
		ru_stime = (double)request->ru_stime();
		doc_size = (double)request->document_size() / 1024;

		if (req_time < 0 || ru_utime < 0 || ru_stime < 0 || doc_size < 0) {
			pinba_error(P_WARNING, "invalid packet data: req_time=%f, ru_utime=%f, ru_stime=%f, doc_size=%f", req_time, ru_utime, ru_stime, doc_size);
			req_time = 0;
			ru_utime = 0;
			ru_stime = 0;
			doc_size = 0;
		}

		record->data.req_time = float_to_timeval(req_time);
		record->data.ru_utime = float_to_timeval(ru_utime);
		record->data.ru_stime = float_to_timeval(ru_stime);
		record->data.req_count = request->request_count();
		record->data.doc_size = (float)doc_size; /* Kbytes*/
		record->data.mem_peak_usage = (float)request->memory_peak() / 1024; /* Kbytes */

		record->data.status = request->has_status() ? request->status() : 0;

		if (timers_cnt > 0) {
			record->timers_cnt = timers_cnt;

			job_data = (struct timers_job_data *)malloc(sizeof(struct timers_job_data));
			job_data->record = record;
			job_data->request = request;
			job_data->request_id = request_pool->in;
			job_data->dict_size = dict_size;

			pthread_rwlock_wrlock(&D->timer_lock);
			record->timers_start = timer_pool_add(timers_cnt);
			pthread_rwlock_unlock(&D->timer_lock);

			if (timers_cnt > PINBA_MAGIC_THREAD_NUMBER) {
				th_pool_dispatch(D->thread_pool, &barrier, add_timers_func, job_data);
				//job_count++;
			} else {
				add_timers_func(job_data);
			}
		}
		
		pinba_update_reports_add(record);
		
		if (UNLIKELY(request_pool->in == (request_pool->size - 1))) {
			request_pool->in = 0;
		} else {
			request_pool->in++;
		}

		/* reached the end of the pool, start throwing out old entries */
		if (request_pool->in == request_pool->out) {
			pinba_stats_record *tmp_record = REQ_POOL(request_pool) + request_pool->in;

			pinba_stats_record_dtor(request_pool->in, tmp_record);

			if (UNLIKELY(request_pool->out == (request_pool->size - 1))) {
				request_pool->out = 0;
			} else {
				request_pool->out++;
			}
		}
	}
	temp_pool->out = temp_pool->in;
//	pinba_debug("add job_count: %d", job_count);
	th_pool_barrier_end(&barrier);
	pthread_rwlock_unlock(&D->tag_reports_lock);
}
/* }}} */

void *pinba_stats_main(void *arg) /* {{{ */
{
	struct timeval launch;

	pinba_debug("starting up stats thread");
	
	gettimeofday(&launch, 0);

	for (;;) {
		struct timeval tv1, from;
	
		pthread_rwlock_wrlock(&D->collector_lock);
		/* make sure we don't store any OLD data */
		from.tv_sec = launch.tv_sec - D->settings.stats_history;
		from.tv_usec = launch.tv_usec;
		pinba_request_pool_delete_old(from);

		pthread_rwlock_wrlock(&D->temp_lock);
		if (LIKELY(pinba_pool_num_records(&D->temp_pool) > 0)) {
			pinba_merge_pools();
			if (D->settings.tag_report_timeout != -1) {
				pinba_tag_reports_destroy(0);
			}
		}
		pthread_rwlock_unlock(&D->temp_lock);
		pthread_rwlock_unlock(&D->collector_lock);

		launch.tv_sec += D->settings.stats_gathering_period / 1000000;
		launch.tv_usec += D->settings.stats_gathering_period % 1000000;

		if (launch.tv_usec > 1000000) { 
			launch.tv_usec -= 1000000;
			launch.tv_sec++;
		}

		gettimeofday(&tv1, 0);
		timersub(&launch, &tv1, &tv1);

		if (LIKELY(tv1.tv_sec >= 0 && tv1.tv_usec >= 0)) {
			usleep(tv1.tv_sec * 1000000 + tv1.tv_usec);
		} else { /* we were locked too long: run right now, but re-schedule next launch */
			gettimeofday(&launch, 0);
			tv1.tv_sec = D->settings.stats_gathering_period / 1000000;
			tv1.tv_usec = D->settings.stats_gathering_period % 1000000;
			timeradd(&launch, &tv1, &launch);
		}
	}
	/* not reachable */
	return NULL;
}
/* }}} */

/*
 * vim600: sw=4 ts=4 fdm=marker
 */
