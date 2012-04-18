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

int pinba_pool_grow(pinba_pool *p, size_t more) /* {{{ */
{
	size_t old_size = p->size;

	p->size += more; /* +more elements*/

	if (p->size <= 0) {
		return P_FAILURE;
	}

	p->data = (void **)realloc(p->data, p->size * p->element_size);

	if (!p->data) {
		p->size = 0;
		p->out = 1;
		p->in = 0;
		return P_FAILURE;
	}

	if (p->size == more) {
		memset(p->data, 0, p->size * p->element_size);
	} else if (p->out > p->in) {
		memmove((char *)p->data + (p->out + more)*p->element_size, (char *)p->data + p->out*p->element_size, (old_size - p->out) * p->element_size);
		memset((char *)p->data + p->out*p->element_size, 0, more * p->element_size);
		p->out += more;
	} else {
		memset((char *)p->data + old_size * p->element_size, 0, more * p->element_size);
	}

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
			timer_pool->out++;
			if (UNLIKELY(timer_pool->out == timer_pool->size)) {
				timer_pool->out = 0;
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

void pinba_temp_pool_dtor(void *pool) /* {{{ */
{
	pinba_pool *p = (pinba_pool *)pool;
	unsigned int i;
	pinba_tmp_stats_record *tmp_record;

	for (i = 0; i < p->size; i++) {
		tmp_record = TMP_POOL(p) + i;
		tmp_record->time.tv_sec = 0;
		if (tmp_record->request && tmp_record->free) {
			pinba__request__free_unpacked(tmp_record->request, NULL);
		}
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

	if ((pinba_pool_num_records(timer_pool) + timers_cnt) >= timer_pool->size) {
		int more = PINBA_TIMER_POOL_GROW_SIZE;

		if (more < timers_cnt) {
			more += timers_cnt;
		}

		pinba_error(P_WARNING, "growing timer_pool to %d", timer_pool->size + more);

		pinba_pool_grow(timer_pool, more);

		if (timer_pool->out >= timer_pool->in) {
			unsigned int i;
			int  prev_request_id = -1;
			pinba_stats_record *record;
			pinba_timer_record *timer;
			pinba_pool *request_pool = &D->request_pool;
			int min_id = -1, cnt = 0, rec_cnt = 0;

			for (i = timer_pool->out; i < timer_pool->size; i++) {
				timer = TIMER_POOL(timer_pool) + i;

				cnt++;

				if (timer->tag_num == 0) {
					continue;
				}

				timer->index = i;

				if (timer->request_id == prev_request_id) {
					continue;
				}

				if (min_id == -1) {
					min_id = timer->request_id;
				}

				record = REQ_POOL(request_pool) + timer->request_id;
				record->timers_start += more;

				rec_cnt ++;

				prev_request_id = timer->request_id;
			}
			pinba_error(P_WARNING, "moved timers_start for %d timers to timers_start + %d for %d records from %d to %d", cnt, more, rec_cnt, min_id, prev_request_id);
		}
	}

	id = timer_pool->in;

	if (UNLIKELY((timer_pool->in + timers_cnt) >= timer_pool->size)) {
		timer_pool->in = (timer_pool->in + timers_cnt) - timer_pool->size;
	} else {
		timer_pool->in += timers_cnt;
	}

	return id;
}
/* }}} */

pthread_rwlock_t timertag_lock = PTHREAD_RWLOCK_INITIALIZER;
int g_timertag_cnt;

inline static int _add_timers(pinba_stats_record *record, Pinba__Request *request, int *timertag_cnt) /* {{{ */
{
	pinba_pool *timer_pool = &D->timer_pool;
	pinba_timer_record *timer;
	unsigned int timers_cnt = record->timers_cnt;
	float timer_value;
	unsigned int i, j, timer_tag_cnt, timer_hit_cnt;
	int tag_value, tag_name;
	unsigned int ti = 0, tt = 0;
	PPvoid_t ppvalue;
	Word_t word_id, tag_id;
	pinba_word *word_ptr;
	char *str;
	pinba_tag *tag;
	int res, dict_size;

	record->timers_cnt = 0;

	dict_size = request->n_dictionary;

	/* add timers to the timers hash */
	for (i = 0; i < timers_cnt; i++, ti++) {
		timer_value = request->timer_value[ti];
		timer_tag_cnt = request->timer_tag_count[ti];
		timer_hit_cnt = request->timer_hit_count[ti];

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
		timer->request_id = 0; /* will be set later */

		if (!timer->tag_ids || !timer->tag_values) {
			timer->tag_num_allocated = 0;
			pinba_debug("out of memory when allocating tag attributes");
			continue;
		}

		record->timers_cnt++;
		timer->tag_num = 0;

		for (j = 0; j < timer_tag_cnt; j++, tt++) {

			tag_value = request->timer_tag_value[tt];
			tag_name = request->timer_tag_name[tt];

			timer->tag_values[j] = NULL;

			if (LIKELY(tag_value < dict_size && tag_name < dict_size && tag_value >= 0 && tag_name >= 0)) {
				str = request->dictionary[tag_value];
			} else {
				pinba_debug("tag_value >= dict_size || tag_name >= dict_size");
				continue;
			}

			pthread_rwlock_rdlock(&timertag_lock);
			ppvalue = JudySLGet(D->dict.word_index, (uint8_t *)str, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				pinba_word *word;
				int len;

				pthread_rwlock_unlock(&timertag_lock);
				pthread_rwlock_wrlock(&timertag_lock);

				word = (pinba_word *)malloc(sizeof(*word));

				/* insert */
				word_id = D->dict.count;
				if (word_id == D->dict.size) {
					D->dict.table = (pinba_word **)realloc(D->dict.table, sizeof(pinba_word *) * (D->dict.size + PINBA_DICTIONARY_GROW_SIZE));
					D->dict.size += PINBA_DICTIONARY_GROW_SIZE;
				}

				D->dict.table[word_id] = word;
				len = strlen(str);
				word->len = (len >= PINBA_TAG_VALUE_SIZE) ? PINBA_TAG_VALUE_SIZE - 1 : len;
				word->str = strndup(str, word->len);
				word_ptr = word;

				ppvalue = JudySLIns(&D->dict.word_index, (uint8_t *)str, NULL);
				if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
					/* well.. too bad.. */
					free(D->dict.table[word_id]);
					pinba_debug("failed to insert new value into word_index");
					pthread_rwlock_unlock(&timertag_lock);
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
					pthread_rwlock_unlock(&timertag_lock);
					continue;
				}
			}

			timer->tag_values[j] = word_ptr;

			str = request->dictionary[tag_name];

			ppvalue = JudySLGet(D->tag.name_index, (uint8_t *)str, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				/* doesn't exist, create */
				int dummy;

				pthread_rwlock_unlock(&timertag_lock);
				pthread_rwlock_wrlock(&timertag_lock);
				tag_id = 0;

				/* get the first empty ID */
				res = JudyLFirstEmpty(D->tag.table, &tag_id, NULL);
				if (res < 0) {
					pinba_debug("no empty indexes in tag.table");
					pthread_rwlock_unlock(&timertag_lock);
					continue;
				}

				tag = (pinba_tag *)malloc(sizeof(pinba_tag));
				if (!tag) {
					pinba_debug("failed to allocate tag");
					pthread_rwlock_unlock(&timertag_lock);
					continue;
				}

				tag->id = tag_id;
				tag->name_len = strlen(str);
				memcpy_static(tag->name, str, tag->name_len, dummy);

				/* add the tag to the table */
				ppvalue = JudyLIns(&D->tag.table, tag_id, NULL);
				if (!ppvalue || ppvalue == PJERR) {
					free(tag);
					pinba_debug("failed to insert tag into tag.table");
					pthread_rwlock_unlock(&timertag_lock);
					continue;
				}
				*ppvalue = tag;

				/* add the tag to the index */
				ppvalue = JudySLIns(&D->tag.name_index, (uint8_t *)str, NULL);
				if (UNLIKELY(ppvalue == PJERR)) {
					JudyLDel(&D->tag.table, tag_id, NULL);
					free(tag);
					pinba_debug("failed to insert tag into tag.name_index");
					pthread_rwlock_unlock(&timertag_lock);
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
			(*timertag_cnt)++;
			pthread_rwlock_unlock(&timertag_lock);
		}
	}
	return record->timers_cnt;
}
/* }}} */

struct tag_reports_job_data {
	int prefix;
	int count;
};

struct packets_job_data {
	int prefix;
	int count;
	int timers_cnt;
	int thread_num;
	int timers_prefix;
	int temp_records_processed;
	int timertag_cnt;
};

struct reports_job_data {
	int prefix;
	int count;
	pinba_report *report;
	int add;
};

void update_reports_func(void *job_data) /* {{{ */
{
	struct reports_job_data *d = (struct reports_job_data *)job_data;
	int i, tmp_id;
	pinba_pool *request_pool = &D->request_pool;
	pinba_stats_record *record;
	void (*func)(pinba_report *report, const pinba_stats_record *record);

	tmp_id = d->prefix;
	if (tmp_id >= request_pool->size) {
		tmp_id = tmp_id - request_pool->size;
	}

	switch (d->report->std.type) {
		case PINBA_TABLE_REPORT_INFO:
			func = d->add ? pinba_update_report_info_add : pinba_update_report_info_delete;
			break;
		case PINBA_TABLE_REPORT1:
			func = d->add ? pinba_update_report1_add : pinba_update_report1_delete;
			break;
		case PINBA_TABLE_REPORT2:
			func = d->add ? pinba_update_report2_add : pinba_update_report2_delete;
			break;
		case PINBA_TABLE_REPORT3:
			func = d->add ? pinba_update_report3_add : pinba_update_report3_delete;
			break;
		case PINBA_TABLE_REPORT4:
			func = d->add ? pinba_update_report4_add : pinba_update_report4_delete;
			break;
		case PINBA_TABLE_REPORT5:
			func = d->add ? pinba_update_report5_add : pinba_update_report5_delete;
			break;
		case PINBA_TABLE_REPORT6:
			func = d->add ? pinba_update_report6_add : pinba_update_report6_delete;
			break;
		case PINBA_TABLE_REPORT7:
			func = d->add ? pinba_update_report7_add : pinba_update_report7_delete;
			break;
		case PINBA_TABLE_REPORT8:
			func = d->add ? pinba_update_report8_add : pinba_update_report8_delete;
			break;
		case PINBA_TABLE_REPORT9:
			func = d->add ? pinba_update_report9_add : pinba_update_report9_delete;
			break;
		case PINBA_TABLE_REPORT10:
			func = d->add ? pinba_update_report10_add : pinba_update_report10_delete;
			break;
		case PINBA_TABLE_REPORT11:
			func = d->add ? pinba_update_report11_add : pinba_update_report11_delete;
			break;
		case PINBA_TABLE_REPORT12:
			func = d->add ? pinba_update_report12_add : pinba_update_report12_delete;
			break;
	}

	pthread_rwlock_wrlock(&d->report->lock);
	for (i = 0; i < d->count; i++, tmp_id = (tmp_id == request_pool->size - 1) ? 0 : tmp_id + 1) {
		record = REQ_POOL(request_pool) + tmp_id;
		func(d->report, record);
	}

	d->report->time_interval = pinba_get_time_interval();
	pthread_rwlock_unlock(&d->report->lock);
}
/* }}} */

void update_tag_reports_add_func(void *job_data) /* {{{ */
{
	struct tag_reports_job_data *d = (struct tag_reports_job_data *)job_data;
	int i, tmp_id;
	pinba_pool *request_pool = &D->request_pool;
	pinba_stats_record *record;

	tmp_id = d->prefix;
	if (tmp_id >= request_pool->size) {
		tmp_id = tmp_id - request_pool->size;
	}

	pthread_rwlock_rdlock(&D->tag_reports_lock);
	for (i = 0; i < d->count; i++, tmp_id = (tmp_id == request_pool->size - 1) ? 0 : tmp_id + 1) {
		record = REQ_POOL(request_pool) + tmp_id;
		if (record->timers_cnt > 0) {
			pinba_update_tag_reports_add(tmp_id, record);
		}
	}
	pthread_rwlock_unlock(&D->tag_reports_lock);
}
/* }}} */

void update_tag_reports_delete_func(void *job_data) /* {{{ */
{
	struct tag_reports_job_data *d = (struct tag_reports_job_data *)job_data;
	int i, tmp_id, j, timertag_cnt = 0;
	pinba_pool *request_pool = &D->request_pool;
	pinba_pool *timer_pool = &D->timer_pool;
	pinba_stats_record *record;
	pinba_timer_record *timer;

	tmp_id = d->prefix;
	if (tmp_id >= request_pool->size) {
		tmp_id = tmp_id - request_pool->size;
	}

	pthread_rwlock_rdlock(&D->tag_reports_lock);
	for (i = 0; i < d->count; i++, tmp_id = (tmp_id == request_pool->size - 1) ? 0 : tmp_id + 1) {
		record = REQ_POOL(request_pool) + tmp_id;

		if (record->timers_cnt > 0) {
			pinba_update_tag_reports_delete(tmp_id, record);

			for (j = 0; j < record->timers_cnt; j++) {
				timer = record_get_timer(timer_pool, record, j);
				if (timer->hit_count == 0) {
					pinba_error(P_WARNING, "clearing already cleared timer!");
					pinba_error(P_WARNING, "tmp_id: %d, timers_cnt: %d, timers_start: %d, timer_pool->size: %d", tmp_id, record->timers_cnt, record->timers_start, timer_pool->size);
				}
				timertag_cnt += timer->tag_num;
				timer->tag_num = 0;
				timer->value.tv_sec = 0;
				timer->value.tv_usec = 0;
				timer->hit_count = 0;
			}
		}
		record->timers_cnt = 0;
	}
	pthread_rwlock_unlock(&D->tag_reports_lock);

	pthread_rwlock_wrlock(&timertag_lock);
	g_timertag_cnt += timertag_cnt;
	pthread_rwlock_unlock(&timertag_lock);
}
/* }}} */

inline void pinba_request_pool_delete_old(struct timeval from, int *deleted_timer_cnt) /* {{{ */
{
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	unsigned int i;

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (timercmp(&record->time, &from, <)) {

			/* pinba_update_reports_delete(record); done by thread pool */

			record->time.tv_sec = 0;
			(*deleted_timer_cnt) += record->timers_cnt;

			p->out++;
			if (p->out == p->size) {
				p->out = 0;
			}
		} else {
			break;
		}
	}
}
/* }}} */
#if 0
inline void pinba_merge_pools(int *added_timer_cnt) /* {{{ */
{
	unsigned int k;
	pinba_pool *temp_pool = &D->temp_pool;
	pinba_pool *request_pool = &D->request_pool;
	Pinba::Request *request;
	pinba_tmp_stats_record *tmp_record;
	pinba_stats_record *record;
	unsigned int timers_cnt, dict_size;
	double req_time, ru_utime, ru_stime, doc_size;

	/* we start with the last record, which should be already empty at the moment */
	pool_traverse_forward(k, temp_pool) {
		record = REQ_POOL(request_pool) + request_pool->in;
		tmp_record = TMP_POOL(temp_pool) + k;

		memset(record, 0, sizeof(*record));
		record->time.tv_sec = tmp_record->time.tv_sec;
		record->time.tv_usec = tmp_record->time.tv_usec;
		request = tmp_record->request;

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
			struct timers_job_data job_data;

			record->timers_cnt = timers_cnt;

			job_data.record = record;
			job_data.request = request;
			job_data.request_id = request_pool->in;
			job_data.dict_size = dict_size;

			record->timers_start = timer_pool_add(timers_cnt);
			(*added_timer_cnt) += timers_cnt;

			add_timers_func(&job_data);
		}

		/* pinba_update_reports_add(record); done by thread pool */

		request_pool->in++;
		if (UNLIKELY(request_pool->in == request_pool->size)) {
			request_pool->in = 0;
		}

		/* reached the end of the pool, start throwing out old entries */
		if (request_pool->in == request_pool->out) {
			pinba_stats_record *tmp_record = REQ_POOL(request_pool) + request_pool->in;

			pinba_stats_record_dtor(request_pool->in, tmp_record);

			request_pool->out++;
			if (UNLIKELY(request_pool->out == request_pool->size)) {
				request_pool->out = 0;
			}
		}
	}
	temp_pool->out = temp_pool->in;
}
/* }}} */
#endif

void merge_pools_func(void *job_data) /* {{{ */
{
	struct packets_job_data *d = (struct packets_job_data *)job_data;
	unsigned int k;
	pinba_pool *temp_pool = &D->temp_pool;
	pinba_pool *request_pool = &D->per_thread_request_pools[d->thread_num];
	Pinba__Request *request;
	pinba_tmp_stats_record *tmp_record;
	pinba_stats_record *record;
	unsigned int timers_cnt, dict_size;
	double req_time, ru_utime, ru_stime, doc_size;
	int tmp_id, request_id;
	int prev_time = 0;

	tmp_id = d->prefix;
	if (tmp_id >= temp_pool->size) {
		tmp_id = tmp_id - temp_pool->size;
	}

	request_pool->in = 0;

	/* we start with the last record, which should be already empty at the moment */
	for (k = 0; k < d->count; k++, tmp_id = (tmp_id == temp_pool->size - 1) ? 0 : tmp_id + 1) {
		record = REQ_POOL(request_pool) + request_pool->in;
		tmp_record = TMP_POOL(temp_pool) + tmp_id;

		memset(record, 0, sizeof(*record));
		record->time.tv_sec = tmp_record->time.tv_sec;
		record->time.tv_usec = tmp_record->time.tv_usec;
		request = tmp_record->request;

		tmp_record->time.tv_sec = 0;
		d->temp_records_processed++;

		timers_cnt = request->n_timer_hit_count;
		if (timers_cnt != (unsigned int)request->n_timer_value || timers_cnt != (unsigned int)request->n_timer_tag_count) {
			pinba_debug("internal error: timer_hit_count_size != timer_value_size || timer_hit_count_size != timer_tag_count_size");
			continue;
		}

		dict_size = request->n_dictionary;
		if (dict_size == 0 && timers_cnt > 0) {
			pinba_debug("internal error: dict_size == 0, but timers_cnt > 0");
			continue;
		}

		memcpy_static(record->data.script_name, request->script_name, strlen(request->script_name), record->data.script_name_len);
		memcpy_static(record->data.server_name, request->server_name, strlen(request->server_name), record->data.server_name_len);
		memcpy_static(record->data.hostname, request->hostname, strlen(request->hostname), record->data.hostname_len);
		req_time = (double)request->request_time;
		ru_utime = (double)request->ru_utime;
		ru_stime = (double)request->ru_stime;
		doc_size = (double)request->document_size / 1024;

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
		record->data.req_count = request->request_count;
		record->data.doc_size = (float)doc_size; /* Kbytes*/
		record->data.mem_peak_usage = (float)request->memory_peak / 1024; /* Kbytes */

		record->data.status = request->has_status ? request->status : 0;

		d->timers_cnt += timers_cnt;
		if (timers_cnt > 0) {
			/*
			struct timers_job_data job_data;

			record->timers_cnt = timers_cnt;

			job_data.record = record;
			job_data.request = request;
			job_data.request_id = request_pool->in;
			job_data.dict_size = dict_size;

			pthread_rwlock_wrlock(&D->timer_lock);
			record->timers_start = timer_pool_add(timers_cnt);
			(*added_timer_cnt) += timers_cnt;

			add_timers_func(&job_data);
			pthread_rwlock_unlock(&D->timer_lock);
			*/
		}

		/* pinba_update_reports_add(record); done by thread pool */

		if (prev_time > 0 && record->time.tv_sec < prev_time) {
			pinba_error(P_WARNING, "prev_time: %d, current record time: %d, tmp_id: %d", prev_time, record->time.tv_sec, tmp_id);
		}
		prev_time = record->time.tv_sec;

		request_pool->in++;

		if (UNLIKELY(request_pool->in == request_pool->size)) {
			/* enlarge the pool */
			if (pinba_pool_grow(request_pool, PINBA_PER_THREAD_POOL_GROW_SIZE) != P_SUCCESS) {
				return;
			}
		}
	}
}
/* }}} */

void merge_timers_func(void *job_data) /* {{{ */
{
	struct packets_job_data *d = (struct packets_job_data *)job_data;
	unsigned int k;
	pinba_pool *temp_pool = &D->temp_pool;
	pinba_pool *timer_pool = &D->timer_pool;
	pinba_pool *request_pool = &D->per_thread_request_pools[d->thread_num];
	Pinba__Request *request;
	pinba_tmp_stats_record *tmp_record;
	pinba_stats_record *record;
	unsigned int timers_cnt, dict_size;
	double req_time, ru_utime, ru_stime, doc_size;
	int tmp_id, request_id;
	int prev_time = 0;

	tmp_id = d->prefix;
	if (tmp_id >= temp_pool->size) {
		tmp_id -= temp_pool->size;
	}

	d->timers_cnt = 0;

	request_id = 0;
	/* we start with the last record, which should be already empty at the moment */
	for (k = 0; request_id < request_pool->in; k++, tmp_id = (tmp_id == temp_pool->size - 1) ? 0 : tmp_id + 1) {
		record = REQ_POOL(request_pool) + request_id;
		tmp_record = TMP_POOL(temp_pool) + tmp_id;
		request = tmp_record->request;

		timers_cnt = request->n_timer_hit_count;
		if (timers_cnt != (unsigned int)request->n_timer_value || timers_cnt != (unsigned int)request->n_timer_tag_count) {
			pinba_debug("internal error: timer_hit_count_size != timer_value_size || timer_hit_count_size != timer_tag_count_size");
			continue;
		}

		dict_size = request->n_dictionary;
		if (dict_size == 0 && timers_cnt > 0) {
			pinba_debug("internal error: dict_size == 0, but timers_cnt > 0");
			continue;
		}

		if (timers_cnt > 0) {
			record->timers_start = d->timers_prefix + d->timers_cnt;
			if (record->timers_start >= timer_pool->size) {
				record->timers_start -= timer_pool->size;
			}

			record->timers_cnt = timers_cnt;
			d->timers_cnt += _add_timers(record, request, &d->timertag_cnt);
		}
		request_id++;
	}
//	pinba_error(P_WARNING, "added timers: %d", d->timers_cnt);
}
/* }}} */

static void request_copy_job_func(void *job_data) /* {{{ */
{
	int i;
	unsigned int tmp_id;
	pinba_stats_record *temp_record, *record;
	struct packets_job_data *d = (struct packets_job_data *)job_data;
	pinba_pool *temp_request_pool = &D->per_thread_request_pools[d->thread_num];
	pinba_pool *request_pool = &D->request_pool;
	pinba_pool *timer_pool = &D->timer_pool;
	pinba_timer_record *timer;

	tmp_id = request_pool->in + d->prefix;
	if (tmp_id >= request_pool->size) {
		tmp_id -= request_pool->size;
	}

	for (i = 0; i < temp_request_pool->in; i++, tmp_id = (tmp_id == request_pool->size - 1) ? 0 : tmp_id + 1) {
			temp_record = REQ_POOL(temp_request_pool) + i;
			record = REQ_POOL(request_pool) + tmp_id;

			memcpy(record, temp_record, sizeof(pinba_stats_record));
			if (record->timers_cnt > 0) {
				int k;

				timer = record_get_timer(timer_pool, record, 0);
				if (timer->hit_count == 0) {
					pinba_error(P_WARNING, "record id: %d has apparently wrong timers_start value: %d", tmp_id, record->timers_start);
				}

				for (k = 0; k < record->timers_cnt; k++) {
					timer = record_get_timer(timer_pool, record, k);
					timer->request_id = tmp_id;
				}
			}
	}
}
/* }}} */

void *pinba_stats_main(void *arg) /* {{{ */
{
	struct timeval launch;
	struct tag_reports_job_data *job_data_arr;
	struct packets_job_data *packets_job_data_arr;
	struct reports_job_data *rep_job_data_arr = NULL;
	int prev_request_id, new_request_id;
	size_t base_reports_alloc = 0;
	pinba_pool *temp_pool = &D->temp_pool;
	pinba_pool *request_pool = &D->request_pool;
	pinba_pool *timer_pool = &D->timer_pool;
	PPvoid_t ppvalue;
	uint8_t index[PINBA_MAX_LINE_LEN] = {0};

	pinba_debug("starting up stats thread");

	/* yes, it's a minor memleak. once per process start. */
	job_data_arr = (struct tag_reports_job_data *)malloc(sizeof(struct tag_reports_job_data) * D->thread_pool->size);
	packets_job_data_arr = (struct packets_job_data *)malloc(sizeof(struct packets_job_data) * D->thread_pool->size);

	gettimeofday(&launch, 0);

	for (;;) {
		struct timeval tv1, from;
		int deleted_timer_cnt = 0;
		int base_reports_cnt;

		pthread_rwlock_wrlock(&D->collector_lock);
		/* make sure we don't store any OLD data */
		from.tv_sec = launch.tv_sec - D->settings.stats_history;
		from.tv_usec = launch.tv_usec;

		pthread_rwlock_rdlock(&D->base_reports_lock);
		if (base_reports_alloc < D->base_reports_cnt) {
			base_reports_alloc = D->base_reports_cnt * 2;
			rep_job_data_arr = (struct reports_job_data *)realloc(rep_job_data_arr, sizeof(struct reports_job_data) * base_reports_alloc);
		}

		memset(job_data_arr, 0, sizeof(struct tag_reports_job_data) * D->thread_pool->size);
		memset(packets_job_data_arr, 0, sizeof(struct packets_job_data) * D->thread_pool->size);
		memset(rep_job_data_arr, 0, sizeof(struct reports_job_data) * base_reports_alloc);
		prev_request_id = request_pool->out;

		pinba_request_pool_delete_old(from, &deleted_timer_cnt);

		new_request_id = request_pool->out;

		{
			thread_pool_barrier_t barrier;
			int i, accounted, job_size, num;

			if (new_request_id == prev_request_id) {
				num = 0;
			} else if (new_request_id > prev_request_id) {
				num = new_request_id - prev_request_id;
			} else {
				num = request_pool->size - (prev_request_id - new_request_id);
			}

			if (num > 0) { /* pass the work to the threads {{{ */
				th_pool_barrier_init(&barrier);
				th_pool_barrier_start(&barrier);

				index[0] = '\0';
				i = 0;
				for (ppvalue = JudySLFirst(D->base_reports, index, NULL); ppvalue != NULL; ppvalue = JudySLNext(D->base_reports, index, NULL)) {
					rep_job_data_arr[i].prefix = prev_request_id;
					rep_job_data_arr[i].count = num;
					rep_job_data_arr[i].report = (pinba_report *)*ppvalue;
					rep_job_data_arr[i].add = 0;
					th_pool_dispatch(D->thread_pool, &barrier, update_reports_func, &(rep_job_data_arr[i]));
					i++;
				}

				if (deleted_timer_cnt > 0) {
					if (num < (D->thread_pool->size * PINBA_THREAD_POOL_THRESHOLD_AMOUNT)) {
						job_size = num;
					} else {
						job_size = num/D->thread_pool->size;
					}

					g_timertag_cnt = 0;

					pthread_rwlock_wrlock(&D->timer_lock);

					accounted = 0;
					for (i = 0; i < D->thread_pool->size; i++) {
						job_data_arr[i].prefix = prev_request_id + accounted;
						job_data_arr[i].count = job_size;
						accounted += job_size;
						if (accounted > num) {
							job_data_arr[i].count -= (accounted - num);
							accounted = num;
						} else {
							if (i == (D->thread_pool->size - 1)) {
								job_data_arr[i].count -= (accounted - num);
								accounted = num;
							}
						}
						th_pool_dispatch(D->thread_pool, &barrier, update_tag_reports_delete_func, &(job_data_arr[i]));
						if (accounted == num) {
							break;
						}
					}
				}
				th_pool_barrier_end(&barrier);

				if (deleted_timer_cnt > 0) {
					if ((timer_pool->out + deleted_timer_cnt) >= timer_pool->size) {
						timer_pool->out = (timer_pool->out + deleted_timer_cnt) - timer_pool->size;
					} else {
						timer_pool->out += deleted_timer_cnt;
					}
					D->timertags_cnt -= g_timertag_cnt;
					pthread_rwlock_unlock(&D->timer_lock);
				}
			}
			/* }}} */
		}

		pthread_rwlock_wrlock(&D->temp_lock);
		if (LIKELY(pinba_pool_num_records(&D->temp_pool) > 0)) {
			int timers_added = 0;

			prev_request_id = request_pool->in;

			{ /* {{{ merge temporary and the actual pools */
				thread_pool_barrier_t barrier;
				int i, accounted, job_size, num;
				int temp_records_processed, records_added;

				num = pinba_pool_num_records(&D->temp_pool);

				if (num < (D->thread_pool->size * PINBA_THREAD_POOL_THRESHOLD_AMOUNT)) {
					job_size = num;
				} else {
					job_size = num/D->thread_pool->size;
				}

				th_pool_barrier_init(&barrier);
				th_pool_barrier_start(&barrier);

				accounted = 0;
				for (i = 0; i < D->thread_pool->size; i++) {
					pinba_pool *temp_request_pool = D->per_thread_request_pools + i;

					packets_job_data_arr[i].prefix = temp_pool->out + accounted;
					packets_job_data_arr[i].count = job_size;
					packets_job_data_arr[i].thread_num = i;
					accounted += job_size;
					if (accounted > num) {
						packets_job_data_arr[i].count -= (accounted - num);
						accounted = num;
					} else {
						if (i == (D->thread_pool->size - 1)) {
							packets_job_data_arr[i].count -= (accounted - num);
							accounted = num;
						}
					}
					th_pool_dispatch(D->thread_pool, &barrier, merge_pools_func, &(packets_job_data_arr[i]));
					if (accounted == num) {
						break;
					}
				}
				th_pool_barrier_end(&barrier);

				records_added = 0;
				timers_added = 0;
				for (i = 0; i < D->thread_pool->size; i++) {
					pinba_pool *temp_request_pool = D->per_thread_request_pools + i;
					if (temp_request_pool->in == 0) {
						break;
					}
					records_added += temp_request_pool->in;
					packets_job_data_arr[i].timers_prefix = timers_added + timer_pool->in;
//					pinba_error(P_WARNING, "timers_prefix: %d", packets_job_data_arr[i].timers_prefix);
					timers_added += packets_job_data_arr[i].timers_cnt;
				}

//				pinba_error(P_WARNING, "timers_added: %d", timers_added);

				if ((pinba_pool_num_records(request_pool) + accounted) >= request_pool->size) {
					pinba_error(P_WARNING, "growing request_pool !!! to new size: %d", request_pool->size + ((accounted / 1024) + 1) * 1024);
					pinba_pool_grow(request_pool, ((accounted / 1024) + 1) * 1024);
				}

				if (timers_added > 0) {
					pthread_rwlock_wrlock(&D->timer_lock);

					timer_pool_add(timers_added);

					th_pool_barrier_init(&barrier);
					th_pool_barrier_start(&barrier);

					temp_records_processed = 0;
					for (i = 0; i < D->thread_pool->size; i++) {
						pinba_pool *temp_request_pool = D->per_thread_request_pools + i;

						if (temp_request_pool->in == 0) {
							break;
						}

						packets_job_data_arr[i].prefix = temp_records_processed + temp_pool->out;
						packets_job_data_arr[i].thread_num = i;
						temp_records_processed += packets_job_data_arr[i].temp_records_processed;
						th_pool_dispatch(D->thread_pool, &barrier, merge_timers_func, &(packets_job_data_arr[i]));
						//merge_timers_func(&(packets_job_data_arr[i]));
					}
					th_pool_barrier_end(&barrier);

					for (i = 0; i < D->thread_pool->size; i++) {
						D->timertags_cnt += packets_job_data_arr[i].timertag_cnt;
					}
					pthread_rwlock_unlock(&D->timer_lock);
				}

				th_pool_barrier_init(&barrier);
				th_pool_barrier_start(&barrier);

				accounted = 0;
				for (i = 0; i < D->thread_pool->size; i++) {
					pinba_pool *temp_request_pool = D->per_thread_request_pools + i;

					if (temp_request_pool->in == 0) {
						break;
					}

					packets_job_data_arr[i].prefix = accounted;
					packets_job_data_arr[i].thread_num = i;
					accounted += temp_request_pool->in;
					th_pool_dispatch(D->thread_pool, &barrier, request_copy_job_func, &(packets_job_data_arr[i]));
				}
				th_pool_barrier_end(&barrier);

				if ((request_pool->in + accounted) >= request_pool->size) {
					request_pool->in = (request_pool->in + accounted) - request_pool->size;
				} else {
					request_pool->in += accounted;
				}
				temp_pool->out = temp_pool->in;

				for (i = 0; i < D->thread_pool->size; i++) {
					pinba_pool *temp_request_pool = D->per_thread_request_pools + i;
					temp_request_pool->in = 0;
				}
			}
			/* }}} */

			/* pinba_merge_pools(&added_timer_cnt); */

			new_request_id = request_pool->in;

			{ /* pass the work to the threads {{{ */
				thread_pool_barrier_t barrier;
				int i, accounted, job_size, num;

				if (new_request_id > prev_request_id) {
					num = new_request_id - prev_request_id;
				} else {
					num = request_pool->size - (prev_request_id - new_request_id);
				}

				if (num < (D->thread_pool->size * PINBA_THREAD_POOL_THRESHOLD_AMOUNT)) {
					job_size = num;
				} else {
					job_size = num/D->thread_pool->size;
				}

				if (num > 0) {

					th_pool_barrier_init(&barrier);
					th_pool_barrier_start(&barrier);

					index[0] = '\0';
					i = 0;
					for (ppvalue = JudySLFirst(D->base_reports, index, NULL); ppvalue != NULL; ppvalue = JudySLNext(D->base_reports, index, NULL)) {
						rep_job_data_arr[i].prefix = prev_request_id;
						rep_job_data_arr[i].count = num;
						rep_job_data_arr[i].report = (pinba_report *)*ppvalue;
						rep_job_data_arr[i].add = 1;
						th_pool_dispatch(D->thread_pool, &barrier, update_reports_func, &(rep_job_data_arr[i]));
						i++;
					}

					if (timers_added > 0) {
						accounted = 0;
						for (i = 0; i < D->thread_pool->size; i++) {
							job_data_arr[i].prefix = prev_request_id + accounted;
							job_data_arr[i].count = job_size;
							accounted += job_size;
							if (accounted > num) {
								job_data_arr[i].count -= (accounted - num);
								accounted = num;
							} else {
								if (i == (D->thread_pool->size - 1)) {
									job_data_arr[i].count -= (accounted - num);
									accounted = num;
								}
							}
							th_pool_dispatch(D->thread_pool, &barrier, update_tag_reports_add_func, &(job_data_arr[i]));
							if (accounted == num) {
								break;
							}
						}
					}
					th_pool_barrier_end(&barrier);
				}
			}
			/* }}} */

			if (D->settings.tag_report_timeout != -1) {
				pinba_tag_reports_destroy(0);
			}
		}
		pthread_rwlock_unlock(&D->temp_lock);
		pthread_rwlock_unlock(&D->base_reports_lock);
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
