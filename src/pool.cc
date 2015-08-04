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

static size_t pinba_pool_new_size(pinba_pool *p, size_t grow_size)
{
	size_t new_size;

	if (grow_size) {
		new_size = p->size + grow_size;
	} else {
		new_size = p->size + p->grow_size;
	}

	if (p->limit_size > 0 && new_size > p->limit_size) {
		new_size = p->limit_size;
	}
	return new_size;
}

int pinba_pool_push(pinba_pool *p, size_t grow_size, void *data) /* {{{ */
{
	if (UNLIKELY(p->in == p->size)) {
		int ret;

		ret = pinba_pool_grow(p, grow_size);
		if (ret != P_SUCCESS) {
			return ret;
		}
	}

	p->data[p->in++] = data;
	return P_SUCCESS;
}
/* }}} */

int pinba_pool_grow(pinba_pool *p, size_t more) /* {{{ */
{
	size_t old_size = p->size;
	size_t new_size = pinba_pool_new_size(p, more);

	more = new_size - old_size;

	if (old_size == new_size) {
		return P_FAILURE;
	}

	p->size = new_size;

	if (p->size <= 0 || p->size < old_size) {
		p->size = old_size;
		return P_FAILURE;
	}

	if (p->limit_size > 0 && new_size == p->limit_size) {
		pinba_error(P_WARNING, "reached size limit for %s (0x%x) - %zd items", p->name, p, p->limit_size);
	}

	p->data = (void **)realloc(p->data, p->size * p->element_size);

	if (!p->data) {
		pinba_error(P_ERROR, "out of memory when reallocating %s (0x%x) to new size of %zd bytes", p->name, p, p->size * p->element_size);
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

int pinba_pool_init(pinba_pool *p, size_t size, size_t element_size, size_t limit_size, size_t grow_size, pool_dtor_func_t dtor, char *pool_name) /* {{{ */
{
	memset(p, 0, sizeof(pinba_pool));
	p->element_size = element_size;
	p->dtor = dtor;
	p->limit_size = limit_size;
	p->grow_size = grow_size ? grow_size : size;
	strncpy(p->name, pool_name, PINBA_POOL_NAME_SIZE);
	p->name[PINBA_POOL_NAME_SIZE - 1] = '\0';

	if (limit_size > 0 && limit_size < size) {
		pinba_error(P_ERROR, "initial size %zd of %s is greater than size limit %zd, fix this in the settings", size, p->name, limit_size);
		return P_FAILURE;
	}

	pinba_error(P_NOTICE, "initializing %s (0x%x) with the size of %zd items", p->name, p, size);
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

	pinba_update_delete(&D->base_reports_arr, request_id, record);

	pthread_rwlock_rdlock(&D->tag_reports_lock);
	pthread_rwlock_wrlock(&D->timer_lock);
	if (record->timers_cnt > 0) {
		pinba_timer_record *timer;
		int tag_sum = 0;

		pinba_update_delete(&D->tag_reports_arr, request_id, record);

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
	pthread_rwlock_unlock(&D->timer_lock);
	pthread_rwlock_unlock(&D->tag_reports_lock);
}
/* }}} */

void pinba_stats_record_tags_dtor(pinba_stats_record *record) /* {{{ */
{
	if (record->data.tag_names) {
		free(record->data.tag_names);
	}

	if (record->data.tag_values) {
		free(record->data.tag_values);
	}

	record->data.tags_alloc_cnt = 0;
	record->data.tags_cnt = 0;
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

	for (i = 0; i < p->size; i++) {
		record = REQ_POOL(p) + i;
		pinba_stats_record_tags_dtor(record);
	}
}
/* }}} */

void pinba_per_thread_request_pool_dtor(void *pool) /* {{{ */
{
	pinba_pool *p = (pinba_pool *)pool;
	size_t i;
	Pinba__Request *request;

	for (i = 0; i < p->size; i++) {
		request = REQ_DATA_POOL(p)[i];
		if (request) {
			pinba__request__free_unpacked(request, NULL);
		}
	}
}
/* }}} */

void pinba_per_thread_tmp_pool_dtor(void *pool) /* {{{ */
{
	pinba_pool *p = (pinba_pool *)pool;
	unsigned int i;
	pinba_stats_record_ex *record_ex;

	for (i = 0; i < p->size; i++) {
		record_ex = REQ_POOL_EX(p) + i;
		pinba_stats_record_tags_dtor(&record_ex->record);
		if (record_ex->request && record_ex->can_free) {
			pinba__request__free_unpacked(record_ex->request, NULL);
			record_ex->request = NULL;
			record_ex->can_free = 0;
		}
		if (record_ex->words) {
			free(record_ex->words);
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

		if (timer_pool->out > timer_pool->in) {
			int prev_request_id = -1;
			pinba_stats_record *record;
			pinba_timer_record *timer;
			pinba_pool *request_pool = &D->request_pool;
			int min_id = -1;
			size_t i, cnt = 0, rec_cnt = 0;

			for (i = timer_pool->out; i < timer_pool->size; i++) {
				timer = TIMER_POOL(timer_pool) + i;

				cnt++;

				if (timer->tag_num == 0) {
					continue;
				}

				timer->index = i;

				if (timer->request_id == (unsigned int)prev_request_id) {
					continue;
				}

				if (min_id == -1) {
					min_id = timer->request_id;
				}

				record = REQ_POOL(request_pool) + timer->request_id;
				if (!record->timers_cnt) {
					pinba_error(P_WARNING, "timer %d references record %d which doesn't have timers", i, timer->request_id);
					continue;
				}
				record->timers_start += more;

				rec_cnt ++;

				prev_request_id = timer->request_id;
			}
			pinba_error(P_WARNING, "moved timers_start for %zd timers to timers_start + %d for %zd records from %d to %d", cnt, more, rec_cnt, min_id, prev_request_id);
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

struct tag_reports_job_data {
	unsigned int prefix;
	unsigned int count;
};

struct packets_job_data {
	unsigned int prefix;
	unsigned int count;
	unsigned int timertag_cnt;
};

void update_reports_func(void *job_data) /* {{{ */
{
	struct reports_job_data *d = (struct reports_job_data *)job_data;
	unsigned int i, tmp_id;
	pinba_pool *request_pool = &D->request_pool;
	pinba_stats_record *record;
	pinba_report_update_function *func;
	pinba_std_report *report = (pinba_std_report *)d->report;

	tmp_id = d->prefix;
	if (tmp_id >= request_pool->size) {
		tmp_id = tmp_id - request_pool->size;
	}

	pthread_rwlock_wrlock(&report->lock);
	if (d->add) {
		func = report->add_func;
		if (report->start.tv_sec == 0) {
			record = REQ_POOL(request_pool) + tmp_id;
			report->start = record->time;
			report->request_pool_start_id = record->counter;
		}
	} else {
		func = report->delete_func;
	}

	for (i = 0; i < d->count; i++, tmp_id = (tmp_id == request_pool->size - 1) ? 0 : tmp_id + 1) {
		record = REQ_POOL(request_pool) + tmp_id;

		CHECK_REPORT_CONDITIONS_CONTINUE(report, record);
		func(tmp_id, report, record);
	}

	report->time_interval = pinba_get_time_interval(report);
	pthread_rwlock_unlock(&report->lock);
}
/* }}} */

void clear_record_timers_func(void *job_data) /* {{{ */
{
	struct packets_job_data *d = (struct packets_job_data *)job_data;
	unsigned int i, tmp_id, j;
	pinba_pool *request_pool = &D->request_pool;
	pinba_stats_record *record;
	pinba_timer_record *timer;
	pinba_pool *timer_pool = &D->timer_pool;

	tmp_id = d->prefix;
	if (tmp_id >= request_pool->size) {
		tmp_id = tmp_id - request_pool->size;
	}

	for (i = 0; i < d->count; i++, tmp_id = (tmp_id == request_pool->size - 1) ? 0 : tmp_id + 1) {
		int warn = 0;
		record = REQ_POOL(request_pool) + tmp_id;

		for (j = 0; j < record->timers_cnt; j++) {
			timer = record_get_timer(timer_pool, record, j);
			if (timer->hit_count == 0 && !warn) {
				pinba_error(P_WARNING, "already cleared timer! timer_id: %ld, tmp_id: %d, timers_cnt: %d, timers_start: %d, timer_pool->size: %d", record->timers_start + j, tmp_id, record->timers_cnt, record->timers_start, timer_pool->size);
				warn = 1;
			}
			d->timertag_cnt += timer->tag_num;
			timer->tag_num = 0;
			timer->value.tv_sec = 0;
			timer->value.tv_usec = 0;
			timer->hit_count = 0;
		}
		/* record->timers_cnt = 0; can't do that under read lock */
	}
}
/* }}} */

void update_tag_reports_func(void *job_data) /* {{{ */
{
	struct reports_job_data *d = (struct reports_job_data *)job_data;
	unsigned int i, n, tmp_id, saved_tmp_id;
	pinba_pool *request_pool = &D->request_pool;
	pinba_stats_record *record;
	pinba_report_update_function *func;
	pinba_std_report *report;

	tmp_id = d->prefix;
	if (tmp_id >= request_pool->size) {
		tmp_id = tmp_id - request_pool->size;
	}
	saved_tmp_id = tmp_id;

	for (n = 0; n < D->tag_reports_arr.size; n++) {
		report = (pinba_std_report *)D->tag_reports_arr.data[n];

		func = d->add ? report->add_func : report->delete_func;

		tmp_id = saved_tmp_id;

		pthread_rwlock_wrlock(&report->lock);
		for (i = 0; i < d->count; i++, tmp_id = (tmp_id == request_pool->size - 1) ? 0 : tmp_id + 1) {
			record = REQ_POOL(request_pool) + tmp_id;

			if (record->timers_cnt > 0) {
				CHECK_REPORT_CONDITIONS_CONTINUE(report, record);
				func(tmp_id, report, record);
			}
		}

		report->time_interval = pinba_get_time_interval(report);
		pthread_rwlock_unlock(&report->lock);
	}
}
/* }}} */

inline void pinba_request_pool_delete_old(struct timeval from, size_t *deleted_timer_cnt, size_t *rtags_cnt) /* {{{ */
{
	pinba_pool *p = &D->request_pool;
	pinba_stats_record *record;
	unsigned int i;

	pool_traverse_forward(i, p) {
		record = REQ_POOL(p) + i;

		if (timercmp(&record->time, &from, <)) {

			(*deleted_timer_cnt) += record->timers_cnt;
			(*rtags_cnt) += record->data.tags_cnt;

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

void *pinba_stats_main(void *arg) /* {{{ */
{
	struct timeval launch;
	struct packets_job_data *packets_job_data_arr;
	struct reports_job_data *rep_job_data_arr = NULL;
	struct reports_job_data *tag_rep_job_data_arr = NULL;
	struct reports_job_data *rtag_rep_job_data_arr = NULL;
	int prev_request_id, new_request_id;
	unsigned int base_reports_alloc = 0, rtag_reports_alloc = 0;
	pinba_pool *request_pool = &D->request_pool;
	pinba_pool *timer_pool = &D->timer_pool;
	thread_pool_barrier_t *barrier1, *barrier2, *barrier3, *barrier4;

	pinba_debug("starting up stats thread");

	/* yes, it's a minor memleak. once per process start. */
	packets_job_data_arr = (struct packets_job_data *)malloc(sizeof(struct packets_job_data) * D->thread_pool->size);
	tag_rep_job_data_arr = (struct reports_job_data *)malloc(sizeof(struct reports_job_data) * D->thread_pool->size);
	barrier1 = (thread_pool_barrier_t *)malloc(sizeof(*barrier1));
	barrier2 = (thread_pool_barrier_t *)malloc(sizeof(*barrier2));
	barrier3 = (thread_pool_barrier_t *)malloc(sizeof(*barrier3));
	barrier4 = (thread_pool_barrier_t *)malloc(sizeof(*barrier4));
	th_pool_barrier_init(barrier1);
	th_pool_barrier_init(barrier2);
	th_pool_barrier_init(barrier3);
	th_pool_barrier_init(barrier4);

	gettimeofday(&launch, 0);

	for (;;) {
		struct timeval tv1, from;
		size_t deleted_timer_cnt = 0, rtags_cnt = 0;

		if (D->in_shutdown) {
			return NULL;
		}

		pthread_rwlock_wrlock(&D->collector_lock);
		/* make sure we don't store any OLD data */
		from.tv_sec = launch.tv_sec - D->settings.stats_history;
		from.tv_usec = launch.tv_usec;

		memset(packets_job_data_arr, 0, sizeof(struct packets_job_data) * D->thread_pool->size);
		prev_request_id = request_pool->out;

		pinba_request_pool_delete_old(from, &deleted_timer_cnt, &rtags_cnt);

		new_request_id = request_pool->out;
		pthread_rwlock_unlock(&D->collector_lock);

		/* relock for reading */
		pthread_rwlock_rdlock(&D->collector_lock);

		{
			unsigned int i, accounted, job_size, num;

			if (new_request_id == prev_request_id) {
				num = 0;
			} else if (new_request_id > prev_request_id) {
				num = new_request_id - prev_request_id;
			} else {
				num = request_pool->size - (prev_request_id - new_request_id);
			}

			if (num > 0) { /* pass the work to the threads {{{ */

				/* update base reports - one report per thread */
				pthread_rwlock_rdlock(&D->base_reports_lock);
				if (base_reports_alloc < D->base_reports_arr.size) {
					base_reports_alloc = D->base_reports_arr.size * 2;
					rep_job_data_arr = (struct reports_job_data *)realloc(rep_job_data_arr, sizeof(struct reports_job_data) * base_reports_alloc);
				}
				memset(rep_job_data_arr, 0, sizeof(struct reports_job_data) * base_reports_alloc);

				th_pool_barrier_start(barrier1);

				for (i= 0; i < D->base_reports_arr.size; i++) {
					rep_job_data_arr[i].prefix = prev_request_id;
					rep_job_data_arr[i].count = num;
					rep_job_data_arr[i].report = D->base_reports_arr.data[i];
					rep_job_data_arr[i].add = 0;
					th_pool_dispatch(D->thread_pool, barrier1, update_reports_func, &(rep_job_data_arr[i]));
				}
				th_pool_barrier_wait(barrier1);

				pthread_rwlock_unlock(&D->base_reports_lock);

				if (rtags_cnt) {
					/* update rtag reports - one report per thread */
					pthread_rwlock_rdlock(&D->rtag_reports_lock);
					if (rtag_reports_alloc < D->rtag_reports_arr.size) {
						rtag_reports_alloc = D->rtag_reports_arr.size * 2;
						rtag_rep_job_data_arr = (struct reports_job_data *)realloc(rtag_rep_job_data_arr, sizeof(struct reports_job_data) * rtag_reports_alloc);
					}

					memset(rtag_rep_job_data_arr, 0, sizeof(struct reports_job_data) * rtag_reports_alloc);

					th_pool_barrier_start(barrier4);
					for (i= 0; i < D->rtag_reports_arr.size; i++) {
						rtag_rep_job_data_arr[i].prefix = prev_request_id;
						rtag_rep_job_data_arr[i].count = num;
						rtag_rep_job_data_arr[i].report = D->rtag_reports_arr.data[i];
						rtag_rep_job_data_arr[i].add = 0;
						th_pool_dispatch(D->thread_pool, barrier4, update_reports_func, &(rtag_rep_job_data_arr[i]));
					}
					th_pool_barrier_wait(barrier4);
					pthread_rwlock_unlock(&D->rtag_reports_lock);
				}

				if (deleted_timer_cnt > 0) {
					if (num < (D->thread_pool->size * PINBA_THREAD_POOL_THRESHOLD_AMOUNT)) {
						job_size = num;
					} else {
						job_size = num/D->thread_pool->size;
					}

					pthread_rwlock_wrlock(&D->timer_lock);
					pthread_rwlock_rdlock(&D->tag_reports_lock);

					/* update tag reports - all threads share the reports */
					accounted = 0;
					th_pool_barrier_start(barrier2);
					for (i= 0; i < D->thread_pool->size; i++) {
						unsigned l_job_size = job_size;

						if (i == D->thread_pool->size - 1) {
							l_job_size = num - accounted;
						}

						tag_rep_job_data_arr[i].prefix = prev_request_id + accounted;
						tag_rep_job_data_arr[i].count = l_job_size;
						tag_rep_job_data_arr[i].add = 0;
						accounted += l_job_size;

						th_pool_dispatch(D->thread_pool, barrier2, update_tag_reports_func, &(tag_rep_job_data_arr[i]));

						if (accounted == num) {
							break;
						}
					}
					th_pool_barrier_wait(barrier2);
					pthread_rwlock_unlock(&D->tag_reports_lock);

					accounted = 0;
					th_pool_barrier_start(barrier3);
					for (i= 0; i < D->thread_pool->size; i++) {
						unsigned l_job_size = job_size;

						if (i == D->thread_pool->size - 1) {
							l_job_size = num - accounted;
						}
						packets_job_data_arr[i].prefix = prev_request_id + accounted;
						packets_job_data_arr[i].count = l_job_size;
						packets_job_data_arr[i].timertag_cnt = 0;
						accounted += l_job_size;
						th_pool_dispatch(D->thread_pool, barrier3, clear_record_timers_func, &(packets_job_data_arr[i]));
						if (accounted == num) {
							break;
						}
					}
					th_pool_barrier_wait(barrier3);

					if ((timer_pool->out + deleted_timer_cnt) >= timer_pool->size) {
						timer_pool->out = (timer_pool->out + deleted_timer_cnt) - timer_pool->size;
					} else {
						timer_pool->out += deleted_timer_cnt;
					}

					for (i = 0; i < D->thread_pool->size; i++) {
						D->timertags_cnt -= packets_job_data_arr[i].timertag_cnt;
					}
					pthread_rwlock_unlock(&D->timer_lock);
				}
			}
			/* }}} */
		}
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
