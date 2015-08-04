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

#include <sys/types.h>
#include <sys/socket.h>
#include "tag_report_map.h"

#ifdef PINBA_ENGINE_VCS_DATE

static struct pinba_version_info version_info[] __attribute__((used)) = {
	"VCS date: " PINBA_ENGINE_VCS_DATE,
	"VCS branch: " PINBA_ENGINE_VCS_BRANCH,
	"VCS full hash: " PINBA_ENGINE_VCS_FULL_HASH,
	"VCS short hash: " PINBA_ENGINE_VCS_SHORT_HASH,
	"VCS WC modified: " PINBA_ENGINE_VCS_WC_MODIFIED
};

#else

static struct pinba_version_info version_info[] __attribute__((used)) = {
	"VCS date: not available",
	"VCS branch: not available",
	"VCS full hash: not available",
	"VCS short hash: not available",
	"VCS WC modified: not available"
};

#endif

struct timeval null_timeval = {0, 0};
static pthread_t data_thread;
static pthread_t *collector_threads;
static pthread_t stats_thread;

int pinba_get_time_interval(pinba_std_report *report) /* {{{ */
{
	pinba_pool *p = &D->request_pool;
	time_t start, end, res;

	if (report->results_cnt < 2) {
		return 1;
	}

	start = REQ_POOL(p)[p->out].time.tv_sec;
	if (p->in > 0) {
		end = REQ_POOL(p)[p->in - 1].time.tv_sec;
	} else {
		end = REQ_POOL(p)[p->size - 1].time.tv_sec;
	}

	res = end - start;
	if (res <= 0) {
		return 1;
	}
	return res;
}
/* }}} */

int pinba_get_processors_number(void) /* {{{ */
{
	long res = 0;

#if defined(PINBA_ENGINE_HAVE_SYSCONF) && defined(_SC_NPROCESSORS_ONLN)
	res = sysconf( _SC_NPROCESSORS_ONLN );
#endif

	return res;
}
/* }}} */

int pinba_collector_init(pinba_daemon_settings settings) /* {{{ */
{
	size_t i;
	int cpu_cnt, cpu_num;
	pthread_rwlockattr_t attr;

	if (settings.port < 0 || settings.port >= 65536) {
		pinba_error(P_ERROR, "port number is invalid (%d)", settings.port);
		return P_FAILURE;
	}

	if (settings.temp_pool_size < 10) {
		pinba_error(P_ERROR, "temp_pool_size is too small (%zd)", settings.temp_pool_size);
		return P_FAILURE;
	}

	if (settings.request_pool_size < 10) {
		pinba_error(P_ERROR, "request_pool_size is too small (%zd)", settings.request_pool_size);
		return P_FAILURE;
	}

	pinba_debug("initializing collector");

	D = (pinba_daemon *)calloc(1, sizeof(pinba_daemon));

	pthread_rwlockattr_init(&attr);

#ifdef __USE_UNIX98
	/* prefer readers over writers */
	pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_READER_NP);
#endif

	pthread_rwlock_init(&D->collector_lock, &attr);
	pthread_rwlock_init(&D->timer_lock, &attr);
	pthread_rwlock_init(&D->data_lock, &attr);
	pthread_rwlock_init(&D->words_lock, &attr);

	pthread_rwlock_init(&D->tag_reports_lock, &attr);
	pthread_rwlock_init(&D->rtag_reports_lock, &attr);
	pthread_rwlock_init(&D->base_reports_lock, &attr);
	pthread_rwlock_init(&D->stats_lock, &attr);
	pthread_rwlock_init(&D->per_thread_pools_lock, &attr);

	if (pinba_pool_init(&D->request_pool, settings.request_pool_size, sizeof(pinba_stats_record), 0, 0/* won't grow it anyway */, pinba_request_pool_dtor, (char *)"request pool") != P_SUCCESS) {
		pinba_error(P_ERROR, "failed to initialize request pool (%d elements). not enough memory?", settings.request_pool_size);
		return P_FAILURE;
	}

	if (pinba_pool_init(&D->timer_pool, settings.timer_pool_size, sizeof(pinba_timer_record), 0, PINBA_TIMER_POOL_GROW_SIZE, pinba_timer_pool_dtor, (char *)"timer pool") != P_SUCCESS) {
		pinba_error(P_ERROR, "failed to initialize timer pool (%d elements). not enough memory?", settings.timer_pool_size);
		return P_FAILURE;
	}

	D->timertags_cnt = 0;

	D->settings = settings;

	cpu_cnt = pinba_get_processors_number();
	if (cpu_cnt <= 1) {
		cpu_cnt = PINBA_THREAD_POOL_DEFAULT_SIZE;
	}
	D->thread_pool = th_pool_create(cpu_cnt);

#ifdef PINBA_ENGINE_HAVE_PTHREAD_SETAFFINITY_NP
	cpu_num = 0;
	for (i = 0; i < D->thread_pool->size; i++, cpu_num = (cpu_num == (cpu_cnt-1)) ? 0 : cpu_num + 1) {
		cpu_set_t mask;

		CPU_ZERO(&mask);
		CPU_SET(cpu_num, &mask);
		pthread_setaffinity_np(D->thread_pool->threads[i], sizeof(mask), &mask);
	}
#endif

	D->per_thread_request_pool[0] = (pinba_pool *)calloc(cpu_cnt, sizeof(pinba_pool));
	D->per_thread_request_pool[1] = (pinba_pool *)calloc(cpu_cnt, sizeof(pinba_pool));
	if (!D->per_thread_request_pool[0] || !D->per_thread_request_pool[1]) {
		pinba_error(P_ERROR, "failed to allocate per_thread_request_pool structs. not enough memory?");
		return P_FAILURE;
	}

	D->per_thread_tmp_pool = (pinba_pool *)calloc(cpu_cnt, sizeof(pinba_pool));
	if (!D->per_thread_tmp_pool) {
		pinba_error(P_ERROR, "failed to allocate per_thread_tmp_pool. not enough memory?");
		return P_FAILURE;
	}

	for (i = 0; i < (size_t)cpu_cnt; i++) {
		char name[PINBA_POOL_NAME_SIZE];

		sprintf(name, "per_thread_request_pool[0][%zd]", i);
		if (pinba_pool_init(D->per_thread_request_pool[0] + i, PINBA_PER_THREAD_POOL_GROW_SIZE, sizeof(Pinba__Request *), settings.temp_pool_size_limit, 0, pinba_per_thread_request_pool_dtor, name) != P_SUCCESS) {
			pinba_error(P_ERROR, "failed to initialize per-thread request pool (%d elements). not enough memory?", PINBA_PER_THREAD_POOL_GROW_SIZE);
			return P_FAILURE;
		}

		sprintf(name, "per_thread_request_pool[1][%zd]", i);
		if (pinba_pool_init(D->per_thread_request_pool[1] + i, PINBA_PER_THREAD_POOL_GROW_SIZE, sizeof(Pinba__Request *), settings.temp_pool_size_limit, 0, pinba_per_thread_request_pool_dtor, name) != P_SUCCESS) {
			pinba_error(P_ERROR, "failed to initialize per-thread request pool (%d elements). not enough memory?", PINBA_PER_THREAD_POOL_GROW_SIZE);
			return P_FAILURE;
		}

		sprintf(name, "per_thread_tmp_pool[%zd]", i);
		if (pinba_pool_init(D->per_thread_tmp_pool + i, PINBA_PER_THREAD_POOL_GROW_SIZE, sizeof(pinba_stats_record_ex), settings.temp_pool_size_limit, 0, pinba_per_thread_tmp_pool_dtor, name) != P_SUCCESS) {
			pinba_error(P_ERROR, "failed to initialize per-thread request pool (%d elements). not enough memory?", PINBA_PER_THREAD_POOL_GROW_SIZE);
			return P_FAILURE;
		}
	}

	D->pool_num = 0;
	D->current_read_pool = D->per_thread_request_pool[D->pool_num];
	D->current_write_pool = D->per_thread_request_pool[!D->pool_num];

	D->collector_socket = pinba_socket_open(D->settings.address, D->settings.port);
	if (!D->collector_socket) {
		return P_FAILURE;
	}

	collector_threads = (pthread_t *)calloc(cpu_cnt, sizeof(pthread_t));
	if (!collector_threads) {
		pinba_error(P_ERROR, "out of memory");
		return P_FAILURE;
	}

	for (i = 0; i < (size_t)cpu_cnt; i++) {
		if (pthread_create(&collector_threads[i], NULL, pinba_collector_main, (void *)i)) {
			return P_FAILURE;
		}

#ifdef PINBA_ENGINE_HAVE_PTHREAD_SETAFFINITY_NP
		{
			cpu_set_t mask;

			CPU_ZERO(&mask);
			CPU_SET(i, &mask);
			pthread_setaffinity_np(collector_threads[i], sizeof(mask), &mask);
		}
#endif
	}

	if (pthread_create(&data_thread, NULL, pinba_data_main, NULL)) {
		return P_FAILURE;
	}

	if (pthread_create(&stats_thread, NULL, pinba_stats_main, NULL)) {
		pthread_cancel(data_thread);
		return P_FAILURE;
	}

#ifdef PINBA_ENGINE_HAVE_PTHREAD_SETAFFINITY_NP
	{
		cpu_set_t mask;

		CPU_ZERO(&mask);
		CPU_SET(settings.cpu_start + 1, &mask);
		pthread_setaffinity_np(data_thread, sizeof(mask), &mask);

		CPU_ZERO(&mask);
		CPU_SET(settings.cpu_start + 2, &mask);
		pthread_setaffinity_np(stats_thread, sizeof(mask), &mask);
	}
#endif

	return P_SUCCESS;
}
/* }}} */

void pinba_collector_shutdown(void) /* {{{ */
{
	Word_t id;
	pinba_tag *tag;
	PPvoid_t ppvalue;
	size_t i, thread_pool_size;

	pinba_debug("shutting down..");

	D->in_shutdown = 1;

	for (i = 0; i < D->thread_pool->size; i++) {
		pthread_cancel(collector_threads[i]);
		pthread_join(collector_threads[i], NULL);
	}

	pthread_join(data_thread, NULL);
	pthread_join(stats_thread, NULL);

	pthread_rwlock_wrlock(&D->collector_lock);
	pthread_rwlock_wrlock(&D->data_lock);

	thread_pool_size = D->thread_pool->size;
	th_pool_destroy(D->thread_pool);

	pinba_socket_free(D->collector_socket);

	pinba_debug("shutting down with %ld (of %ld) elements in the pool", pinba_pool_num_records(&D->request_pool), D->request_pool.size);
	pinba_debug("shutting down with %ld (of %ld) elements in the timer pool", pinba_pool_num_records(&D->timer_pool), D->timer_pool.size);

	pinba_pool_destroy(&D->request_pool);
	pinba_pool_destroy(&D->timer_pool);

	for (i = 0; i < thread_pool_size; i++) {
		pinba_pool_destroy(D->per_thread_request_pool[0] + i);
		pinba_pool_destroy(D->per_thread_request_pool[1] + i);
		pinba_pool_destroy(D->per_thread_tmp_pool + i);
	}
	free(D->per_thread_request_pool[0]);
	free(D->per_thread_request_pool[1]);
	free(D->per_thread_tmp_pool);

	pinba_debug("shutting down with %ld elements in tag.table", JudyLCount(D->tag.table, 0, -1, NULL));
	pinba_debug("shutting down with %ld elements in tag.name_index", JudyLCount(D->tag.name_index, 0, -1, NULL));

	pthread_rwlock_unlock(&D->data_lock);
	pthread_rwlock_destroy(&D->data_lock);

	pthread_rwlock_unlock(&D->collector_lock);
	pthread_rwlock_destroy(&D->collector_lock);

	pinba_tag_reports_destroy();
	pthread_rwlock_destroy(&D->tag_reports_lock);

	pinba_rtag_reports_destroy();
	pthread_rwlock_destroy(&D->rtag_reports_lock);

	pinba_reports_destroy();
	pthread_rwlock_destroy(&D->base_reports_lock);

	JudySLFreeArray(&D->tables_to_reports, NULL);

	pthread_rwlock_destroy(&D->words_lock);
	pthread_rwlock_destroy(&D->timer_lock);
	pthread_rwlock_destroy(&D->stats_lock);

	id = 0;
	for (ppvalue = JudyLFirst(D->tag.table, &id, NULL); ppvalue && ppvalue != PPJERR; ppvalue = JudyLNext(D->tag.table, &id, NULL)) {
		tag = (pinba_tag *)*ppvalue;
		free(tag);
	}

	id = 0;
/*	for (ppvalue = JudyLFirst(D->dictionary, &id, NULL); ppvalue; ppvalue = JudyLNext(D->dictionary, &id, NULL)) {
		pinba_word *word = (pinba_word *)*ppvalue;
		free(word->str);
		free(word);
	}
*/
	JudyLFreeArray(&D->tag.table, NULL);
	JudyLFreeArray(&D->tag.name_index, NULL);
//	JudyLFreeArray(&D->dictionary, NULL);

	free(D);
	D = NULL;

	pinba_debug("collector shut down");
}
/* }}} */

void *pinba_collector_main(void *arg) /* {{{ */
{
	size_t thread_num = (size_t)arg;

	pinba_debug("starting up collector thread %zd", thread_num);

	pinba_eat_udp(D->collector_socket, thread_num);

	/* unreachable */
	return NULL;
}
/* }}} */

struct data_job_data {
	size_t start;
	size_t end;
	struct timeval now;
	unsigned int thread_num;
	size_t invalid_packets;
	size_t timers_cnt;
	size_t rtags_cnt;
	size_t timers_prefix;
	unsigned int timertag_cnt;
	unsigned int res_cnt;
};

static inline int request_to_record(Pinba__Request *request, pinba_stats_record_ex *record_ex) /* {{{ */
{
	pinba_word **tag_names, **tag_values;
	unsigned int tags_alloc_cnt, timers_cnt, dict_size;
	double req_time, ru_utime, ru_stime, doc_size;
	pinba_stats_record *record = &record_ex->record;

	/* save the tags */
	tag_names = record->data.tag_names;
	tag_values = record->data.tag_values;
	tags_alloc_cnt = record->data.tags_alloc_cnt;

	memset(record, 0, sizeof(*record));

	record->data.tag_names = tag_names;
	record->data.tag_values = tag_values;
	record->data.tags_alloc_cnt = tags_alloc_cnt;

	timers_cnt = request->n_timer_hit_count;
	if (timers_cnt != (unsigned int)request->n_timer_value || timers_cnt != (unsigned int)request->n_timer_tag_count) {
		pinba_error(P_WARNING, "malformed data: timer_hit_count_size != timer_value_size || timer_hit_count_size != timer_tag_count_size");
		return -1;
	}

	if (request->n_tag_name != request->n_tag_value) {
		pinba_error(P_WARNING, "malformed data: n_tag_name != n_tag_value");
		return -1;
	}

	dict_size = request->n_dictionary;
	if (dict_size == 0) {
		if (timers_cnt > 0) {
			pinba_error(P_WARNING, "malformed data: dict_size == 0, but timers_cnt > 0");
			return -1;
		}
		if (request->n_tag_name > 0) {
			pinba_error(P_WARNING, "malformed data: dict_size == 0, but tags are present");
			return -1;
		}
	}

	record_ex->words_cnt = 0;

	if (request->n_tag_name > 0) {
		pinba_word *word_ptr;
		unsigned int i;

		if (record_ex->words_alloc < request->n_dictionary) {
			record_ex->words = (pinba_word **)realloc(record_ex->words, sizeof(pinba_word *) * request->n_dictionary);
			if (!record_ex->words) {
				pinba_warning("out of memory when allocating record_ex->words");
				return -1;
			}
			record_ex->words_alloc = request->n_dictionary;
		}

		pthread_rwlock_rdlock(&D->words_lock);
		for (i = 0; i < request->n_dictionary; i++) { /* {{{ */
			char *str;
			uint64_t str_hash;
			int str_len;

			str = request->dictionary + PINBA_DICTIONARY_ENTRY_SIZE * i;
			str_len = strlen(str);
			str_hash = XXH64((const uint8_t*)str, str_len, 2001);

			record_ex->words[i] = NULL;
			record_ex->words_cnt++;

			word_ptr = (pinba_word *)tag_report_map_get(D->dictionary, str);
			if (UNLIKELY(!word_ptr)) {
				pthread_rwlock_unlock(&D->words_lock);
				pthread_rwlock_wrlock(&D->words_lock);

				word_ptr = (pinba_word *)tag_report_map_get(D->dictionary, str);
				if (word_ptr) {
					pthread_rwlock_unlock(&D->words_lock);
					pthread_rwlock_rdlock(&D->words_lock);
					goto race_condition;
				}

				word_ptr = (pinba_word *)malloc(sizeof(*word_ptr));

				/* insert */
				word_ptr->len = (str_len >= PINBA_TAG_VALUE_SIZE) ? PINBA_TAG_VALUE_SIZE - 1 : str_len;
				word_ptr->str = strndup(str, word_ptr->len);
				word_ptr->hash = str_hash;

				D->dictionary = tag_report_map_add(D->dictionary, str, word_ptr);
				pthread_rwlock_unlock(&D->words_lock);
				pthread_rwlock_rdlock(&D->words_lock);
			}
race_condition:
			record_ex->words[i] = word_ptr;
		}
		/* }}} */
		pthread_rwlock_unlock(&D->words_lock);

		if (record->data.tags_alloc_cnt < request->n_tag_name) {
			record->data.tag_names = (pinba_word **)realloc(record->data.tag_names, request->n_tag_name * sizeof(pinba_word *));
			if (!record->data.tag_names) {
				pinba_error(P_WARNING, "internal error: realloc(.., %d) returned NULL", request->n_tag_name * sizeof(pinba_word *));
				record->data.tags_alloc_cnt = 0;
				return -1;
			}

			record->data.tag_values = (pinba_word **)realloc(record->data.tag_values, request->n_tag_name * sizeof(pinba_word *));
			if (!record->data.tag_values) {
				pinba_error(P_WARNING, "internal error: realloc(.., %d) returned NULL", request->n_tag_name * sizeof(pinba_word *));
				record->data.tags_alloc_cnt = 0;
				return -1;
			}

			memset(record->data.tag_names + record->data.tags_alloc_cnt, 0, sizeof(pinba_word *) * (request->n_tag_name - record->data.tags_alloc_cnt));
			memset(record->data.tag_values + record->data.tags_alloc_cnt, 0, sizeof(pinba_word *) * (request->n_tag_name - record->data.tags_alloc_cnt));
			record->data.tags_alloc_cnt = request->n_tag_name;
		}

		for (i = 0; i < request->n_tag_name; i++) {
			if (request->tag_name[i] >= request->n_dictionary) {
				pinba_error(P_WARNING, "malformed data: tag_name[%d] (%d) >= request->n_dictionary (%d)", i, request->tag_name[i], request->n_dictionary);
				return -1;
			}

			if (request->tag_value[i] >= request->n_dictionary) {
				pinba_error(P_WARNING, "malformed data: tag_value[%d] (%d) >= request->n_dictionary (%d)", i, request->tag_value[i], request->n_dictionary);
				return -1;
			}

			record->data.tag_names[i] = record_ex->words[request->tag_name[i]];
			record->data.tag_values[i] = record_ex->words[request->tag_value[i]];
			record->data.tags_cnt++;
		}
	}

	memcpy_static(record->data.script_name, request->script_name, strlen(request->script_name), record->data.script_name_len);
	memcpy_static(record->data.server_name, request->server_name, strlen(request->server_name), record->data.server_name_len);
	memcpy_static(record->data.hostname, request->hostname, strlen(request->hostname), record->data.hostname_len);
	if (request->schema && request->schema[0]) {
		memcpy_static(record->data.schema, request->schema, strlen(request->schema), record->data.schema_len);
	}
	req_time = (double)request->request_time;
	ru_utime = (double)request->ru_utime;
	ru_stime = (double)request->ru_stime;
	doc_size = (double)request->document_size / 1024;

	if (req_time < 0 || doc_size < 0) {
		pinba_error(P_WARNING, "invalid packet data: req_time=%f, ru_utime=%f, ru_stime=%f, doc_size=%f, hostname=%s, script_name=%s", req_time, ru_utime, ru_stime, doc_size, request->hostname, request->script_name);

		if (req_time < 0) {
			req_time = 0;
		}

		if (doc_size < 0) {
			doc_size = 0;
		}
	}

	if (ru_utime < 0 || ru_stime < 0) { /* I have no idea why this happens, but this happens VERY often */
		ru_utime = 0;
		ru_stime = 0;
	}

	record->data.req_time = float_to_timeval(req_time);
	record->data.ru_utime = float_to_timeval(ru_utime);
	record->data.ru_stime = float_to_timeval(ru_stime);
	record->data.req_count = request->request_count;
	record->data.doc_size = (float)doc_size; /* Kbytes*/
	record->data.mem_peak_usage = (float)request->memory_peak / 1024; /* Kbytes */
	if (request->has_memory_footprint) {
		record->data.memory_footprint = (float)request->memory_footprint / 1024; /* Kbytes */
	} else {
		record->data.memory_footprint = 0;
	}

	record->data.status = request->has_status ? request->status : 0;
	return 0;
}
/* }}} */

inline static int _add_timers(pinba_stats_record *record, const pinba_stats_record_ex *record_ex, unsigned int *timertag_cnt, int request_id, unsigned int timers_cnt) /* {{{ */
{
	pinba_pool *timer_pool = &D->timer_pool;
	pinba_timer_record *timer;
	float timer_value;
	unsigned int i, j, timer_tag_cnt, timer_hit_cnt;
	int tag_value, tag_name;
	unsigned int ti = 0, tt = 0;
	PPvoid_t ppvalue;
	pinba_word *word_ptr;
	char *str;
	pinba_tag *tag;
	uint64_t str_hash;
	int res, dict_size, str_len;
	pinba_word *temp_words_static[PINBA_TEMP_DICTIONARY_SIZE] = {0};
	pinba_word **temp_words_dynamic = NULL;
	pinba_word **temp_words;
	pinba_tag *temp_tags_static[PINBA_TEMP_DICTIONARY_SIZE] = {0};
	pinba_tag **temp_tags_dynamic = NULL;
	pinba_tag **temp_tags;
	Pinba__Request *request = record_ex->request;

	record->timers_cnt = 0;

	if (request->n_dictionary > 0 && record_ex->words_cnt == 0) {
		if (request->n_dictionary > PINBA_TEMP_DICTIONARY_SIZE) {
			temp_words_dynamic = (pinba_word **)malloc(sizeof(void *) * request->n_dictionary);
			if (!temp_words_dynamic) {
				pinba_warning("out of memory when allocating temp words");
				return 0;
			}
			temp_words = temp_words_dynamic;

			temp_tags_dynamic = (pinba_tag **)malloc(sizeof(void *) * request->n_dictionary);
			if (!temp_tags_dynamic) {
				pinba_warning("out of memory when allocating temp tags");
				return 0;
			}
			temp_tags = temp_tags_dynamic;
		} else {
			temp_words = temp_words_static;
			temp_tags = temp_tags_static;
		}

		for (i = 0; i < request->n_dictionary; i++) { /* {{{ */

			str = request->dictionary + PINBA_DICTIONARY_ENTRY_SIZE * i;
			str_len = strlen(str);
			str_hash = XXH64((const uint8_t*)str, str_len, 2001);

			temp_words[i] = NULL;
			temp_tags[i] = NULL;

			ppvalue = JudyLGet(D->tag.name_index, str_hash, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				/* do nothing */
			} else {
				temp_tags[i] = (pinba_tag *)*ppvalue;
			}

			word_ptr = (pinba_word *)tag_report_map_get(D->dictionary, str);
			if (UNLIKELY(!word_ptr)) {
				pthread_rwlock_unlock(&D->words_lock);
				pthread_rwlock_wrlock(&D->words_lock);

				word_ptr = (pinba_word *)tag_report_map_get(D->dictionary, str);
				if (word_ptr) {
					pthread_rwlock_unlock(&D->words_lock);
					pthread_rwlock_rdlock(&D->words_lock);
					goto race_condition;
				}

				word_ptr = (pinba_word *)malloc(sizeof(*word_ptr));

				/* insert */
				word_ptr->len = (str_len >= PINBA_TAG_VALUE_SIZE) ? PINBA_TAG_VALUE_SIZE - 1 : str_len;
				word_ptr->str = strndup(str, word_ptr->len);
				word_ptr->hash = str_hash;

				D->dictionary = tag_report_map_add(D->dictionary, str, word_ptr);
				pthread_rwlock_unlock(&D->words_lock);
				pthread_rwlock_rdlock(&D->words_lock);
			}
race_condition:
			temp_words[i] = word_ptr;
		}
		/* }}} */
	} else {
		temp_words = record_ex->words;
		if (request->n_dictionary > PINBA_TEMP_DICTIONARY_SIZE) {
			temp_tags_dynamic = (pinba_tag **)malloc(sizeof(void *) * request->n_dictionary);
			if (!temp_tags_dynamic) {
				pinba_warning("out of memory when allocating temp tags");
				return 0;
			}
			temp_tags = temp_tags_dynamic;
		} else {
			temp_tags = temp_tags_static;
		}

		for (i = 0; i < request->n_dictionary; i++) { /* {{{ */
			pinba_word *word = temp_words[i];

			if (!word) {
				continue;
			}
			temp_tags[i] = NULL;

			ppvalue = JudyLGet(D->tag.name_index, word->hash, NULL);
			if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
				/* do nothing */
			} else {
				temp_tags[i] = (pinba_tag *)*ppvalue;
			}
		}
		/* }}} */
	}

	dict_size = request->n_dictionary;

	/* add timers to the timers hash */
	for (i = 0; i < timers_cnt; i++, ti++) {
		timer_value = request->timer_value[ti];
		timer_tag_cnt = request->timer_tag_count[ti];
		timer_hit_cnt = request->timer_hit_count[ti];

		timer = record_get_timer(timer_pool, record, i);
		timer->index = record_get_timer_id(timer_pool, record, i);
		timer->request_id = request_id;

		if (request->n_timer_ru_stime > i) {
			timer->ru_stime = float_to_timeval(request->timer_ru_stime[i]);
		} else {
			timer->ru_stime = null_timeval;
		}

		if (request->n_timer_ru_utime > i) {
			timer->ru_utime = float_to_timeval(request->timer_ru_utime[i]);
		} else {
			timer->ru_utime = null_timeval;
		}

		if (!timer_hit_cnt) {
			pinba_debug("timer.hit_count is 0");
			continue;
		}

		if (!timer_tag_cnt) {
			pinba_debug("timer.hit_count is 0");
			continue;
		}

		if (timer_value < 0) {
			pinba_debug("timer.value is negative: %0.3f", timer_value);
			timer_value = 0;
		}

		int allocate_num = 0;
		if (timer->tag_num_allocated < timer_tag_cnt) {
			allocate_num = timer_tag_cnt;
			if (timer_tag_cnt < PINBA_MIN_TAG_VALUES_CNT_MAGIC_NUMBER) {
				allocate_num = PINBA_MIN_TAG_VALUES_CNT_MAGIC_NUMBER;
			}
			timer->tag_ids = (int *)realloc(timer->tag_ids, sizeof(int) * allocate_num);
			timer->tag_values = (pinba_word **)realloc(timer->tag_values, sizeof(pinba_word *) * allocate_num);
			timer->tag_num_allocated = allocate_num;
		}

		if (timer_value > 0.0) {
			timer->value = float_to_timeval(timer_value);
		} else {
			timer->value = float_to_timeval(0);
		}
		timer->hit_count = timer_hit_cnt;
		timer->num_in_request = record->timers_cnt;

		if (!timer->tag_ids || !timer->tag_values) {
			timer->tag_num_allocated = 0;
			pinba_warning("out of memory when allocating tag attributes (num: %ld)", allocate_num);
			continue;
		}

		record->timers_cnt++;
		timer->tag_num = 0;

		for (j = 0; j < timer_tag_cnt; j++, tt++) {

			tag_value = request->timer_tag_value[tt];
			tag_name = request->timer_tag_name[tt];

			timer->tag_values[j] = NULL;

			if (LIKELY(tag_value < dict_size && tag_name < dict_size && tag_value >= 0 && tag_name >= 0)) {
				word_ptr = temp_words[tag_value];
				if (!word_ptr) {
					continue;
				}
			} else {
				pinba_warning("tag_value >= dict_size || tag_name >= dict_size");
				continue;
			}

			timer->tag_values[j] = word_ptr;

			word_ptr = temp_words[tag_name];
			tag = temp_tags[tag_name];

			if (!tag) {
				ppvalue = JudyLGet(D->tag.name_index, word_ptr->hash, NULL);
				if (UNLIKELY(!ppvalue || ppvalue == PPJERR)) {
					/* doesn't exist, create */
					int dummy;
					Word_t tag_id = 0;

					pthread_rwlock_unlock(&D->words_lock);
					pthread_rwlock_wrlock(&D->words_lock);

					/* get the first empty ID */
					res = JudyLFirstEmpty(D->tag.table, &tag_id, NULL);
					if (res < 0) {
						pinba_warning("no empty indexes in tag.table");
						continue;
					}

					tag = (pinba_tag *)malloc(sizeof(pinba_tag));
					if (!tag) {
						pinba_warning("failed to allocate tag");
						continue;
					}

					tag->id = tag_id;
					tag->name_len = word_ptr->len;
					tag->hash = word_ptr->hash;
					memcpy_static(tag->name, word_ptr->str, tag->name_len, dummy);

					/* add the tag to the table */
					ppvalue = JudyLIns(&D->tag.table, tag_id, NULL);
					if (!ppvalue || ppvalue == PJERR) {
						free(tag);
						pinba_warning("failed to insert tag into tag.table");
						continue;
					}
					*ppvalue = tag;

					/* add the tag to the index */
					ppvalue = JudyLIns(&D->tag.name_index, word_ptr->hash, NULL);
					if (UNLIKELY(ppvalue == PJERR)) {
						JudyLDel(&D->tag.table, tag_id, NULL);
						free(tag);
						pinba_warning("failed to insert tag into tag.name_index");
						continue;
					} else {
						*ppvalue = tag;
					}
					pthread_rwlock_unlock(&D->words_lock);
					pthread_rwlock_rdlock(&D->words_lock);
				} else {
					tag = (pinba_tag *)*ppvalue;
				}
			}

			timer->tag_ids[j] = tag->id;
			timer->tag_num++;
			(*timertag_cnt)++;
		}
	}

	if (temp_words_dynamic) {
		free(temp_words_dynamic);
	}

	if (temp_tags_dynamic) {
		free(temp_tags_dynamic);
	}

	return record->timers_cnt;
}
/* }}} */

void merge_timers_func(void *job_data) /* {{{ */
{
	struct data_job_data *d = (struct data_job_data *)job_data;
	pinba_pool *timer_pool = &D->timer_pool;
	pinba_pool *tmp_pool = &D->per_thread_tmp_pool[d->thread_num];
	pinba_pool *request_pool = &D->request_pool;
	Pinba__Request *request;
	pinba_stats_record *record;
	pinba_stats_record_ex *record_ex;
	unsigned int timers_cnt, real_timers_cnt, dict_size, k, request_id;

	d->timers_cnt = 0;

	request_id = 0;
	pthread_rwlock_rdlock(&D->words_lock);
	for (k = 0; request_id < d->end; k++) {
		record_ex = REQ_POOL_EX(tmp_pool) + request_id;
		record = REQ_POOL(request_pool) + record_ex->request_id;
		request = record_ex->request;

		timers_cnt = request->n_timer_hit_count;
		if (timers_cnt != (unsigned int)request->n_timer_value || timers_cnt != (unsigned int)request->n_timer_tag_count) {
			pinba_debug("internal error: timer_hit_count_size != timer_value_size || timer_hit_count_size != timer_tag_count_size");
			request_id++;
			continue;
		}

		dict_size = request->n_dictionary;
		if (dict_size == 0 && timers_cnt > 0) {
			pinba_debug("internal error: dict_size == 0, but timers_cnt > 0");
			request_id++;
			continue;
		}

		if (timers_cnt > 0) {
			record->timers_start = d->timers_prefix + d->timers_cnt;
			if (record->timers_start >= timer_pool->size) {
				record->timers_start -= timer_pool->size;
			}

			real_timers_cnt = _add_timers(record, record_ex, &d->timertag_cnt, record_ex->request_id, timers_cnt);
			d->timers_cnt += real_timers_cnt;
		}
		request_id++;
	}
	pthread_rwlock_unlock(&D->words_lock);
}
/* }}} */

static void data_job_func(void *data) /* {{{ */
{
	pinba_stats_record_ex *record_ex;
	int sub_request_num = -1;
	int current_sub_request = -1;
	Pinba__Request *parent_request = NULL;
	struct data_job_data *d = (struct data_job_data *)data;
	pinba_pool *request_pool = D->current_read_pool + d->thread_num;
	pinba_pool *tmp_pool = D->per_thread_tmp_pool + d->thread_num;
	size_t i;

	for (i = 0; i < request_pool->in; i++) {
		do {
			Pinba__Request *request;

			if (tmp_pool->in == tmp_pool->size) {
				int ret;

				ret = pinba_pool_grow(tmp_pool, 0);
				if (ret != 0) {
					/* XXX losing packets and leaking memory ! */
					return;
				}
			}

			record_ex = REQ_POOL_EX(tmp_pool) + tmp_pool->in;
			if (record_ex->request && record_ex->can_free) {
				pinba__request__free_unpacked(record_ex->request, NULL);
				record_ex->request = NULL;
				record_ex->can_free = 0;
			}

			if (sub_request_num == -1) {

				request = REQ_DATA_POOL(request_pool)[request_pool->out];
				REQ_DATA_POOL(request_pool)[request_pool->out] = NULL;
				request_pool->out++;
				if (UNLIKELY(request == NULL)) {
					//d->invalid_packets++;
					continue;
				}

				if (request->n_timer_hit_count != request->n_timer_value || request->n_timer_hit_count != request->n_timer_tag_count) {
					pinba_debug("internal error: timer_hit_count_size (%d) != timer_value_size (%d) || timer_hit_count_size (%d) != timer_tag_count_size (%d)", request->n_timer_hit_count, request->n_timer_value, request->n_timer_hit_count, request->n_timer_tag_count);
					//d->invalid_packets++;
					continue;
				}

				sub_request_num = request->n_requests;
				if (sub_request_num > 0) {
					parent_request = request;
					current_sub_request = 0;
				} else {
					sub_request_num = -1;
				}
				record_ex->request = request;
				record_ex->can_free = 1;
			} else {
				request = parent_request->requests[current_sub_request];
				record_ex->request = request;
				record_ex->can_free = 0;
				current_sub_request++;
			}

			if (request_to_record(request, record_ex) < 0) {
				//	d->invalid_packets++;
				continue;
			}
			record_ex->record.time = d->now;
			tmp_pool->in++;
		} while (current_sub_request < sub_request_num);
	}
}
/* }}} */

static void request_copy_job_func(void *job_data) /* {{{ */
{
	unsigned int i, tmp_id;
	pinba_stats_record_ex *temp_record_ex;
	pinba_stats_record *temp_record, *record;
	struct data_job_data *d = (struct data_job_data *)job_data;
	pinba_pool *tmp_pool = D->per_thread_tmp_pool + d->thread_num;
	pinba_pool *request_pool = &D->request_pool;

	tmp_id = request_pool->in + d->start;
	if (tmp_id >= request_pool->size) {
		tmp_id -= request_pool->size;
	}

	for (i = 0; i < d->end; i++) {
		pinba_word **tag_names, **tag_values;
		unsigned int tags_alloc_cnt, n;

		temp_record_ex = REQ_POOL_EX(tmp_pool) + i;
		temp_record_ex->request_id = tmp_id;
		temp_record = &temp_record_ex->record;
		record = REQ_POOL(request_pool) + tmp_id;

		/* save the tags */
		tag_names = record->data.tag_names;
		tag_values = record->data.tag_values;
		tags_alloc_cnt = record->data.tags_alloc_cnt;

		memcpy(record, temp_record, sizeof(pinba_stats_record));

		record->counter = D->request_pool_counter + d->start + i;
		record->data.tag_names = tag_names;
		record->data.tag_values = tag_values;
		record->data.tags_alloc_cnt = tags_alloc_cnt;

		if (record->data.tags_alloc_cnt < temp_record->data.tags_cnt) {
			record->data.tag_names = (pinba_word **)realloc(record->data.tag_names, temp_record->data.tags_cnt * sizeof(pinba_word *));
			if (!record->data.tag_names) {
				pinba_error(P_WARNING, "internal error: realloc(.., %d) returned NULL", temp_record->data.tags_cnt * sizeof(pinba_word *));
				record->data.tags_alloc_cnt = 0;
				continue;
			}

			record->data.tag_values = (pinba_word **)realloc(record->data.tag_values, temp_record->data.tags_cnt * sizeof(pinba_word *));
			if (!record->data.tag_values) {
				pinba_error(P_WARNING, "internal error: realloc(.., %d) returned NULL", temp_record->data.tags_cnt * sizeof(pinba_word *));
				record->data.tags_alloc_cnt = 0;
				continue;
			}

			memset(record->data.tag_names + record->data.tags_alloc_cnt, 0, sizeof(char *) * (temp_record->data.tags_cnt - record->data.tags_alloc_cnt));
			memset(record->data.tag_values + record->data.tags_alloc_cnt, 0, sizeof(char *) * (temp_record->data.tags_cnt - record->data.tags_alloc_cnt));
			record->data.tags_alloc_cnt = temp_record->data.tags_cnt;
		}

		record->data.tags_cnt = 0;
		for (n = 0; n < temp_record->data.tags_cnt; n++) {
			record->data.tag_names[n] = temp_record->data.tag_names[n];
			record->data.tag_values[n] = temp_record->data.tag_values[n];
			record->data.tags_cnt++;
		}
		temp_record->data.tags_cnt = 0;

		d->rtags_cnt += record->data.tags_cnt;

		d->timers_cnt += temp_record_ex->request->n_timer_hit_count;
		d->res_cnt++;

		if (tmp_id == (request_pool->size - 1)) {
			tmp_id = 0;
		} else {
			tmp_id++;
		}
	}
}
/* }}} */

static void free_data_func(void *job_data) /* {{{ */
{
	struct data_job_data *d = (struct data_job_data *)job_data;
	pinba_pool *temp_request_pool = &D->per_thread_request_pool[!D->pool_num][d->thread_num];
	pinba_stats_record_ex *temp_record_ex;
	unsigned int i;

	for (i = 0; i < temp_request_pool->in; i++) {
		temp_record_ex = REQ_POOL_EX(temp_request_pool) + i;

		if (temp_record_ex->request && temp_record_ex->can_free) {
			pinba__request__free_unpacked(temp_record_ex->request, NULL);
			temp_record_ex->words_cnt = 0;
			temp_record_ex->request = NULL;
			temp_record_ex->can_free = 0;
		}
	}
	temp_request_pool->in = 0;
}
/* }}} */

void *pinba_data_main(void *arg) /* {{{ */
{
	struct timeval launch, tv1;
	struct data_job_data *job_data_arr;
	pinba_pool *request_pool = &D->request_pool;
	thread_pool_barrier_t *barrier1, *barrier2, *barrier3, *barrier4, *barrier5, *barrier6, *barrier7;
	struct reports_job_data *rep_job_data_arr = NULL;
	struct reports_job_data *tag_rep_job_data_arr = NULL;
	struct reports_job_data *rtag_rep_job_data_arr = NULL;
	unsigned int base_reports_alloc = 0, rtag_reports_alloc = 0;

	barrier1 = (thread_pool_barrier_t *)malloc(sizeof(*barrier1));
	barrier2 = (thread_pool_barrier_t *)malloc(sizeof(*barrier2));
	barrier3 = (thread_pool_barrier_t *)malloc(sizeof(*barrier3));
	barrier4 = (thread_pool_barrier_t *)malloc(sizeof(*barrier4));
	barrier5 = (thread_pool_barrier_t *)malloc(sizeof(*barrier5));
	barrier6 = (thread_pool_barrier_t *)malloc(sizeof(*barrier6));
	barrier7 = (thread_pool_barrier_t *)malloc(sizeof(*barrier7));
	th_pool_barrier_init(barrier1);
	th_pool_barrier_init(barrier2);
	th_pool_barrier_init(barrier3);
	th_pool_barrier_init(barrier4);
	th_pool_barrier_init(barrier5);
	th_pool_barrier_init(barrier6);
	th_pool_barrier_init(barrier7);

	pinba_debug("starting up data harvester thread");

	/* yes, it's a minor memleak. once per process start. */
	job_data_arr = (struct data_job_data *)malloc(sizeof(struct data_job_data) * D->thread_pool->size);
	tag_rep_job_data_arr = (struct reports_job_data *)malloc(sizeof(struct reports_job_data) * D->thread_pool->size);

	gettimeofday(&launch, NULL);
	for (;;) {
		size_t stats_records, records_to_copy, timers_added, free_slots, records_created;
		size_t accounted, job_size, invalid_packets = 0, lost_tmp_records = 0, rtags_found;
		size_t i;

		if (D->in_shutdown) {
			return NULL;
		}

		/* since we now support multi-request packets, we cannot assume that
		   the number of data packets == the number of requests, so we have to do this
		   in two steps */

		/* Step 1: harvest the data and put the decoded packets to per-thread temp pools */

		/* swap the pools and free the lock */
		pthread_rwlock_wrlock(&D->per_thread_pools_lock);
		D->current_read_pool = D->per_thread_request_pool[D->pool_num];
		D->current_write_pool = D->per_thread_request_pool[!D->pool_num];
		D->pool_num = !D->pool_num;
		pthread_rwlock_unlock(&D->per_thread_pools_lock);

		records_to_copy = 0;
		for (i = 0; i < D->thread_pool->size; i++) {
			pinba_pool *temp_request_pool = D->current_read_pool + i;
			records_to_copy += temp_request_pool->in;
		}

		if (!records_to_copy) {
			goto sleep;
		}

		memset(job_data_arr, 0, sizeof(struct data_job_data) * D->thread_pool->size);

		th_pool_barrier_start(barrier1);
		accounted = 0;
		for (i = 0; i < D->thread_pool->size; i++) {
			pinba_pool *request_pool = D->current_read_pool + i;
			if (request_pool->in == 0) {
				continue;
			}
			job_data_arr[i].thread_num = i;
			job_data_arr[i].now = launch;
			th_pool_dispatch(D->thread_pool, barrier1, data_job_func, &(job_data_arr[i]));
		}
		th_pool_barrier_wait(barrier1);

		records_to_copy = 0;
		for (i = 0; i < D->thread_pool->size; i++) {
			pinba_pool *tmp_pool = D->per_thread_tmp_pool + i;
			records_to_copy += tmp_pool->in;
		}

		if (!records_to_copy) {
			goto sleep;
		}

		pthread_rwlock_wrlock(&D->collector_lock);

		/* determine how much free slots we have in the request pool */
		free_slots = request_pool->size - pinba_pool_num_records(request_pool) - 1;
		if (free_slots < records_to_copy) {
			lost_tmp_records = records_to_copy - free_slots;
			pinba_error(P_WARNING, "%d free slots found in the request pool, throwing away %d new requests! increase your request pool size accordingly", free_slots, lost_tmp_records);
			records_to_copy = free_slots;
		}

		stats_records = records_to_copy;

		/* process new stats data and update base reports */
		accounted = 0;
		th_pool_barrier_start(barrier2);
		for (i = 0; i < D->thread_pool->size; i++) {
			pinba_pool *tmp_pool = D->per_thread_tmp_pool + i;

			if (tmp_pool->in == 0) {
				continue;
			}

			job_data_arr[i].start = accounted;
			job_data_arr[i].thread_num = i;
			job_data_arr[i].res_cnt = 0;
			job_data_arr[i].timers_cnt = 0;
			job_data_arr[i].end = tmp_pool->in;
			if (tmp_pool->in > records_to_copy) {
				job_data_arr[i].end = records_to_copy;
			}
			accounted += job_data_arr[i].end;
			records_to_copy -= job_data_arr[i].end;
			th_pool_dispatch(D->thread_pool, barrier2, request_copy_job_func, &(job_data_arr[i]));
		}
		th_pool_barrier_wait(barrier2);

		records_created = 0;
		timers_added = 0;
		rtags_found = 0;
		for (i = 0; i < D->thread_pool->size; i++) {
			struct data_job_data *data = &job_data_arr[i];
			pinba_pool *tmp_pool = D->per_thread_tmp_pool + i;

			records_created += data->res_cnt;
			timers_added += data->timers_cnt;
			rtags_found += data->rtags_cnt;
			tmp_pool->in = 0;
		}

		D->request_pool_counter += records_created;

		if (records_created < (D->thread_pool->size * PINBA_THREAD_POOL_THRESHOLD_AMOUNT)) {
			job_size = records_created;
		} else {
			job_size = records_created/D->thread_pool->size;
		}

		/* update base reports - one report per thread */
		pthread_rwlock_rdlock(&D->base_reports_lock);

		for (i = 0; i < D->base_reports_arr.size; i++) {
			pinba_std_report *report = (pinba_std_report *)D->base_reports_arr.data[i];

			pthread_rwlock_wrlock(&report->lock);
			if (report->start.tv_sec == 0) {
				pinba_stats_record *record = REQ_POOL(request_pool) + request_pool->in;
				report->start = record->time;
				report->request_pool_start_id = record->counter;
			}
			pthread_rwlock_unlock(&report->lock);
		}

		if (base_reports_alloc < D->base_reports_arr.size) {
			base_reports_alloc = D->base_reports_arr.size * 2;
			rep_job_data_arr = (struct reports_job_data *)realloc(rep_job_data_arr, sizeof(struct reports_job_data) * base_reports_alloc);
		}

		memset(rep_job_data_arr, 0, sizeof(struct reports_job_data) * base_reports_alloc);

		th_pool_barrier_start(barrier4);
		for (i= 0; i < D->base_reports_arr.size; i++) {
			rep_job_data_arr[i].prefix = request_pool->in;
			rep_job_data_arr[i].count = records_created;
			rep_job_data_arr[i].report = D->base_reports_arr.data[i];
			rep_job_data_arr[i].add = 1;
			th_pool_dispatch(D->thread_pool, barrier4, update_reports_func, &(rep_job_data_arr[i]));
		}
		th_pool_barrier_wait(barrier4);
		pthread_rwlock_unlock(&D->base_reports_lock);

		if (rtags_found) {
			/* update rtag reports - one report per thread */
			pthread_rwlock_rdlock(&D->rtag_reports_lock);

			for (i = 0; i < D->rtag_reports_arr.size; i++) {
				pinba_std_report *report = (pinba_std_report *)D->rtag_reports_arr.data[i];

				pthread_rwlock_wrlock(&report->lock);
				if (report->start.tv_sec == 0) {
					pinba_stats_record *record = REQ_POOL(request_pool) + request_pool->in;
					report->start = record->time;
					report->request_pool_start_id = record->counter;
				}
				pthread_rwlock_unlock(&report->lock);
			}

			if (rtag_reports_alloc < D->rtag_reports_arr.size) {
				rtag_reports_alloc = D->rtag_reports_arr.size * 2;
				rtag_rep_job_data_arr = (struct reports_job_data *)realloc(rtag_rep_job_data_arr, sizeof(struct reports_job_data) * rtag_reports_alloc);
			}

			memset(rtag_rep_job_data_arr, 0, sizeof(struct reports_job_data) * rtag_reports_alloc);

			th_pool_barrier_start(barrier7);
			for (i= 0; i < D->rtag_reports_arr.size; i++) {
				rtag_rep_job_data_arr[i].prefix = request_pool->in;
				rtag_rep_job_data_arr[i].count = records_created;
				rtag_rep_job_data_arr[i].report = D->rtag_reports_arr.data[i];
				rtag_rep_job_data_arr[i].add = 1;
				th_pool_dispatch(D->thread_pool, barrier7, update_reports_func, &(rtag_rep_job_data_arr[i]));
			}
			th_pool_barrier_wait(barrier7);
			pthread_rwlock_unlock(&D->rtag_reports_lock);
		}

		if (timers_added > 0) {
			unsigned int timer_pool_in;

			/* create timers and update timer reports */
			pthread_rwlock_wrlock(&D->timer_lock);

			timer_pool_in = timer_pool_add(timers_added);

			th_pool_barrier_start(barrier3);

			timers_added = 0;
			for (i = 0; i < D->thread_pool->size; i++) {
				pinba_pool *temp_request_pool = D->current_read_pool + i;
				if (temp_request_pool->in == 0) {
					continue;
				}
				job_data_arr[i].timers_prefix = timers_added + timer_pool_in;
				timers_added += job_data_arr[i].timers_cnt;
			}

			pthread_rwlock_rdlock(&D->tag_reports_lock);

			for (i = 0; i < D->tag_reports_arr.size; i++) {
				pinba_std_report *report = (pinba_std_report *)D->tag_reports_arr.data[i];

				pthread_rwlock_wrlock(&report->lock);
				if (report->start.tv_sec == 0) {
					pinba_stats_record *record = REQ_POOL(request_pool) + request_pool->in;
					report->start = record->time;
					report->request_pool_start_id = record->counter;
				}
				pthread_rwlock_unlock(&report->lock);
			}

			records_to_copy = stats_records;
			for (i = 0; i < D->thread_pool->size; i++) {
				pinba_pool *temp_request_pool = D->current_read_pool + i;

				if (temp_request_pool->in == 0) {
					continue;
				}

				job_data_arr[i].thread_num = i;
				job_data_arr[i].end = temp_request_pool->in;
				if (temp_request_pool->in > records_to_copy) {
					job_data_arr[i].end = records_to_copy;
				}
				records_to_copy -= job_data_arr[i].end;
				th_pool_dispatch(D->thread_pool, barrier3, merge_timers_func, &(job_data_arr[i]));
			}
			th_pool_barrier_wait(barrier3);

			for (i = 0; i < D->thread_pool->size; i++) {
				D->timertags_cnt += job_data_arr[i].timertag_cnt;
			}

			/* update tag reports - all threads share the reports */
			accounted = 0;
			th_pool_barrier_start(barrier5);
			for (i= 0; i < D->thread_pool->size; i++) {
				if (i == D->thread_pool->size - 1) {
					job_size = stats_records - accounted;
				}

				tag_rep_job_data_arr[i].prefix = request_pool->in + accounted;
				tag_rep_job_data_arr[i].count = job_size;
				tag_rep_job_data_arr[i].add = 1;
				accounted += job_size;

				th_pool_dispatch(D->thread_pool, barrier5, update_tag_reports_func, &(tag_rep_job_data_arr[i]));

				if (accounted == stats_records) {
					break;
				}
			}
			th_pool_barrier_wait(barrier5);

			pthread_rwlock_unlock(&D->tag_reports_lock);
			pthread_rwlock_unlock(&D->timer_lock);
		}

		if ((request_pool->in + records_created) >= request_pool->size) {
			request_pool->in = (request_pool->in + records_created) - request_pool->size;
		} else {
			request_pool->in += records_created;
		}

		pthread_rwlock_unlock(&D->collector_lock);

		for (i = 0; i < D->thread_pool->size; i++) {
			pinba_pool *temp_request_pool = D->current_read_pool + i;
			temp_request_pool->in = 0;
			temp_request_pool->out = 0;
		}
/*
		th_pool_barrier_start(barrier6);
		for (i = 0; i < D->thread_pool->size; i++) {
			pinba_pool *temp_request_pool = D->per_thread_request_pool[!D->pool_num] + i;

			if (temp_request_pool->in == 0) {
				break;
			}
			job_data_arr[i].thread_num = i;
			th_pool_dispatch(D->thread_pool, barrier6, free_data_func, &(job_data_arr[i]));
		}
		th_pool_barrier_wait(barrier6);
*/
		if (invalid_packets > 0 || lost_tmp_records > 0) {
			pthread_rwlock_wrlock(&D->stats_lock);
			D->stats.invalid_packets += invalid_packets;
			D->stats.lost_tmp_records += lost_tmp_records;
			pthread_rwlock_unlock(&D->stats_lock);
		}

sleep:

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

time_t last_error_time;
char last_errormsg[PINBA_ERR_BUFFER];

pthread_mutex_t error_mutex = PTHREAD_MUTEX_INITIALIZER;

char *pinba_error_ex(int return_error, int type, const char *file, int line, const char *format, ...) /* {{{ */
{
	va_list args;
	const char *type_name;
	char *tmp;
	char tmp_format[PINBA_ERR_BUFFER/2];
	char errormsg[PINBA_ERR_BUFFER];

	switch (type) {
		case P_DEBUG_DUMP:
			type_name = "debug dump";
			break;
		case P_DEBUG:
			type_name = "debug";
			break;
		case P_NOTICE:
			type_name = "notice";
			break;
		case P_WARNING:
			type_name = "warning";
			break;
		case P_ERROR:
			type_name = "error";
			break;
		default:
			type_name = "unknown error";
			break;
	}

	snprintf(tmp_format, PINBA_ERR_BUFFER/2, "[PINBA] %s: %s:%d %s", type_name, file, line, format);

	va_start(args, format);
	vsnprintf(errormsg, PINBA_ERR_BUFFER, tmp_format, args);
	va_end(args);

	if (!return_error) {
		time_t t;
		struct tm *tmp;
		char timebuf[256] = {0};

		pthread_mutex_lock(&error_mutex);
		t = time(NULL);
		if ((t - last_error_time) < 1 && strcmp(last_errormsg, errormsg) == 0) {
			/* don't flood the logs */
			pthread_mutex_unlock(&error_mutex);
			return NULL;
		}
		last_error_time = t;
		strncpy(last_errormsg, errormsg, PINBA_ERR_BUFFER);

		tmp = localtime(&t);

		if (tmp) {
			strftime(timebuf, sizeof(timebuf), "%Y-%m-%d %H:%M:%S", tmp);
			fprintf(stderr, "[%s] %s\n", timebuf, errormsg);
		} else {
			fprintf(stderr, "%s\n", errormsg);
		}
		fflush(stderr);
		pthread_mutex_unlock(&error_mutex);
		return NULL;
	}
	tmp = strdup(errormsg);
	return tmp;
}
/* }}} */

#if PINBA_ENGINE_HAVE_RECVMMSG

#define PINBA_VLEN 64

#ifdef MSG_WAITFORONE
# define PINBA_RECVMMSG_FLAGS MSG_WAITFORONE
#else
# define PINBA_RECVMMSG_FLAGS 0
#endif

void pinba_eat_udp(pinba_socket *sock, size_t thread_num) /* {{{ */
{
	int i;
	struct mmsghdr *msgs;
	struct iovec *iovecs;
	char *bufs;

	msgs = (struct mmsghdr *)calloc(PINBA_VLEN, sizeof(struct mmsghdr));
	iovecs = (struct iovec *)calloc(PINBA_VLEN, sizeof(struct iovec));
	bufs = (char *)calloc(PINBA_VLEN, PINBA_UDP_BUFFER_SIZE);

	if (!msgs || !iovecs || !bufs) {
		pinba_error(P_ERROR, "out of memory");
		return;
	}

	for (i = 0; i < PINBA_VLEN; i++) {
		iovecs[i].iov_base = bufs + PINBA_UDP_BUFFER_SIZE * i;
		iovecs[i].iov_len = PINBA_UDP_BUFFER_SIZE;
		msgs[i].msg_hdr.msg_iov = &iovecs[i];
		msgs[i].msg_hdr.msg_iovlen = 1;
	}

	for (;;) {
		int num;

		num = recvmmsg(sock->listen_sock, msgs, PINBA_VLEN, PINBA_RECVMMSG_FLAGS, NULL);

		if (num > 0) {
			pinba_pool *req_pool;

			pthread_rwlock_rdlock(&D->per_thread_pools_lock);
			req_pool = D->current_write_pool + thread_num;

			for (i = 0; i < num; i++) {
				if (msgs[i].msg_len > 0) {
					Pinba__Request *request;
					int ret;

					request = pinba__request__unpack(NULL, msgs[i].msg_len, (const unsigned char *)bufs + PINBA_UDP_BUFFER_SIZE * i);
					if (UNLIKELY(request == NULL)) {
						//d->invalid_packets++;
						continue;
					}
					ret = pinba_pool_push(req_pool, 0, request);
					if (ret != 0) {
						pinba__request__free_unpacked(request, NULL); /* XXX */
						break; /* XXX */
					}
				}
			}

			pthread_rwlock_unlock(&D->per_thread_pools_lock);
		} else if (num < 0) {
			if (errno == EINTR) {
				continue;
			}
			pinba_error(P_WARNING, "recvmmsg() failed: %s (%d)", strerror(errno), errno);
		} else {
			pinba_error(P_WARNING, "recvmmsg() returned 0");
		}
	}
}
/* }}} */
#else
void pinba_eat_udp(pinba_socket *sock, size_t thread_num) /* {{{ */
{
	for (;;) {
		int ret;
		unsigned char buf[PINBA_UDP_BUFFER_SIZE];

		ret = recv(sock->listen_sock, buf, PINBA_UDP_BUFFER_SIZE, 0);

		if (ret > 0) {
			pinba_pool *req_pool;
			Pinba__Request *request;

			pthread_rwlock_rdlock(&D->per_thread_pools_lock);
			req_pool = D->current_write_pool + thread_num;

			request = pinba__request__unpack(NULL, ret, (const unsigned char *)buf);
			if (LIKELY(request != NULL)) {
				pinba_pool_push(req_pool, 0, request);
			} else {
				//d->invalid_packets++;
			}
			pthread_rwlock_unlock(&D->per_thread_pools_lock);
		} else if (ret < 0) {
			if (errno == EINTR) {
				continue;
			}
			pinba_error(P_WARNING, "recv() failed: %s (%d)", strerror(errno), errno);
		} else {
			pinba_error(P_WARNING, "recv() returned 0");
		}
	}
}
/* }}} */
#endif

void pinba_socket_free(pinba_socket *socket) /* {{{ */
{
	if (!socket) {
		return;
	}

	if (socket->listen_sock >= 0) {
		close(socket->listen_sock);
		socket->listen_sock = -1;
	}

	free(socket);
}
/* }}} */

pinba_socket *pinba_socket_open(char *ip, int listen_port) /* {{{ */
{
	struct sockaddr_in addr;
	pinba_socket *s;
	int sfd, yes = 1;

	if ((sfd = socket(AF_INET, SOCK_DGRAM, 0)) == -1) {
		pinba_error(P_ERROR, "socket() failed: %s (%d)", strerror(errno), errno);
		return NULL;
	}

	if(setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
		close(sfd);
		return NULL;
	}

	s = (pinba_socket *)calloc(1, sizeof(pinba_socket));
	if (!s) {
		return NULL;
	}
	s->listen_sock = sfd;

	memset(&addr, 0, sizeof(addr));

	addr.sin_family = AF_INET;
	addr.sin_port = htons(listen_port);
	addr.sin_addr.s_addr = htonl(INADDR_ANY);

	if (ip && *ip) {
		struct in_addr tmp;

		if (inet_aton(ip, &tmp)) {
			addr.sin_addr.s_addr = tmp.s_addr;
		} else {
			pinba_error(P_WARNING, "inet_aton(%s) failed, listening on ANY IP-address", ip);
		}
	}

	if (bind(s->listen_sock, (struct sockaddr *)&addr, sizeof(addr))) {
		pinba_socket_free(s);
		pinba_error(P_ERROR, "bind() failed: %s (%d)", strerror(errno), errno);
		return NULL;
	}

	return s;
}
/* }}} */

#ifndef PINBA_ENGINE_HAVE_STRNDUP
char *pinba_strndup(const char *s, unsigned int length) /* {{{ */
{
	char *p;

	p = (char *) malloc(length + 1);
	if (UNLIKELY(p == NULL)) {
		return p;
	}
	if (length) {
		memcpy(p, s, length);
	}
	p[length] = 0;
	return p;
}
/* }}} */
#endif

/*
 * vim600: sw=4 ts=4 fdm=marker
 */
