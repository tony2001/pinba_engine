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

int pinba_get_time_interval() /* {{{ */
{
	pinba_pool *p = &D->request_pool;
	time_t start, end, res;

	start = REQ_POOL(p)[p->out].time.tv_sec;
	if (p->in > 0) {
		end = REQ_POOL(p)[p->in - 1].time.tv_sec;
	} else {
		end = start;
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
	int i;
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

	D->base = event_base_new();

	pthread_rwlockattr_init(&attr);

#ifdef __USE_UNIX98
	/* prefer readers over writers */
	pthread_rwlockattr_setkind_np(&attr, PTHREAD_RWLOCK_PREFER_READER_NP);
#endif

	pthread_rwlock_init(&D->collector_lock, &attr);
	pthread_rwlock_init(&D->temp_lock, &attr);
	pthread_rwlock_init(&D->data_lock, &attr);

	pthread_rwlock_init(&D->tag_reports_lock, &attr);
	pthread_rwlock_init(&D->base_reports_lock, &attr);
	pthread_rwlock_init(&D->timer_lock, &attr);

	if (pinba_pool_init(&D->temp_pool, settings.temp_pool_size, sizeof(pinba_tmp_stats_record), pinba_temp_pool_dtor) != P_SUCCESS) {
		pinba_error(P_ERROR, "failed to initialize temporary pool (%d elements). not enough memory?", settings.temp_pool_size);
		return P_FAILURE;
	}

	if (pinba_pool_init(&D->data_pool, settings.temp_pool_size, sizeof(pinba_data_bucket), pinba_data_pool_dtor) != P_SUCCESS) {
		pinba_error(P_ERROR, "failed to initialize data pool (%d elements). not enough memory?", settings.temp_pool_size);
		return P_FAILURE;
	}

	if (pinba_pool_init(&D->request_pool, settings.request_pool_size, sizeof(pinba_stats_record), pinba_request_pool_dtor) != P_SUCCESS) {
		pinba_error(P_ERROR, "failed to initialize request pool (%d elements). not enough memory?", settings.request_pool_size);
		return P_FAILURE;
	}

	if (pinba_pool_init(&D->timer_pool, PINBA_TIMER_POOL_GROW_SIZE, sizeof(pinba_timer_record), pinba_timer_pool_dtor) != P_SUCCESS) {
		pinba_error(P_ERROR, "failed to initialize timer pool (%d elements). not enough memory?", PINBA_TIMER_POOL_GROW_SIZE);
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

	D->per_thread_temp_pools = (pinba_pool *)calloc(cpu_cnt, sizeof(pinba_pool));
	if (!D->per_thread_temp_pools) {
		pinba_error(P_ERROR, "failed to allocate per_thread_temp_pools struct. not enough memory?");
		return P_FAILURE;
	}

	for (i = 0; i < cpu_cnt; i++) {
		int n;
		if (pinba_pool_init(D->per_thread_temp_pools + i, PINBA_PER_THREAD_POOL_GROW_SIZE, sizeof(pinba_tmp_stats_record), pinba_temp_pool_dtor) != P_SUCCESS) {
			pinba_error(P_ERROR, "failed to initialize per-thread temporary pool (%d elements). not enough memory?", PINBA_PER_THREAD_POOL_GROW_SIZE);
			return P_FAILURE;
		}
	}

	D->per_thread_request_pools = (pinba_pool *)calloc(cpu_cnt, sizeof(pinba_pool));
	if (!D->per_thread_request_pools) {
		pinba_error(P_ERROR, "failed to allocate per_thread_request_pools struct. not enough memory?");
		return P_FAILURE;
	}

	for (i = 0; i < cpu_cnt; i++) {
		int n;
		if (pinba_pool_init(D->per_thread_request_pools + i, PINBA_PER_THREAD_POOL_GROW_SIZE, sizeof(pinba_stats_record), pinba_per_thread_request_pool_dtor) != P_SUCCESS) {
			pinba_error(P_ERROR, "failed to initialize per-thread request pool (%d elements). not enough memory?", PINBA_PER_THREAD_POOL_GROW_SIZE);
			return P_FAILURE;
		}
	}

#if 0
	for (i = PINBA_TABLE_REPORT_INFO; i <= PINBA_TABLE_REPORT12; i++) {
		pinba_report *report;
		PPvoid_t ppvalue;
		uint8_t index[PINBA_MAX_LINE_LEN] = {0};

		sprintf((char *)index, "%d", i);
		ppvalue = JudySLIns(&D->base_reports, index, NULL);
		if (ppvalue && ppvalue != PPJERR) {
			report = (pinba_report *)calloc(1, sizeof(pinba_report));
			if (!report) {
				pinba_error(P_ERROR, "failed to allocate report (%d). not enough memory?", i);
				return P_FAILURE;
			}
			report->std.type = i;
			report->time_interval = 1;

#define SET_HANDLERS(id)													\
				case PINBA_TABLE_REPORT##id:								\
					report->add_func = pinba_update_report##id##_add;		\
					report->delete_func = pinba_update_report##id##_delete;	\
					break

			switch (i) {
				case PINBA_TABLE_REPORT_INFO:
					report->add_func = pinba_update_report_info_add;
					report->delete_func = pinba_update_report_info_delete;
					break;
				SET_HANDLERS(1);
				SET_HANDLERS(2);
				SET_HANDLERS(3);
				SET_HANDLERS(4);
				SET_HANDLERS(5);
				SET_HANDLERS(6);
				SET_HANDLERS(7);
				SET_HANDLERS(8);
				SET_HANDLERS(9);
				SET_HANDLERS(10);
				SET_HANDLERS(11);
				SET_HANDLERS(12);
			}
			pthread_rwlock_init(&(report->lock), &attr);
			*ppvalue = report;

			if (pinba_base_reports_array_add(report) < 0) {
				JudySLDel(&D->base_reports, index, NULL);
				pthread_rwlock_unlock(&report->lock);
				pthread_rwlock_destroy(&report->lock);
				pinba_std_report_dtor(report);
				free(report);
			}
		}
	}
#endif

	return P_SUCCESS;
}
/* }}} */

void pinba_collector_shutdown(void) /* {{{ */
{
	Word_t id;
	pinba_word *word;
	pinba_tag *tag;
	PPvoid_t ppvalue;
	int i;

	pinba_debug("shutting down..");
	pthread_rwlock_wrlock(&D->collector_lock);
	pthread_rwlock_wrlock(&D->data_lock);
	pthread_rwlock_wrlock(&D->temp_lock);

	pinba_socket_free(D->collector_socket);


	pinba_debug("shutting down with %ld (of %ld) elements in the pool", pinba_pool_num_records(&D->request_pool), D->request_pool.size);
	pinba_debug("shutting down with %ld (of %ld) elements in the temp pool", pinba_pool_num_records(&D->temp_pool), D->temp_pool.size);
	pinba_debug("shutting down with %ld (of %ld) elements in the timer pool", pinba_pool_num_records(&D->timer_pool), D->timer_pool.size);

	pinba_pool_destroy(&D->request_pool);
	pinba_pool_destroy(&D->data_pool);
	pinba_pool_destroy(&D->temp_pool);
	pinba_pool_destroy(&D->timer_pool);

	for (i = 0; i < D->thread_pool->size; i++) {
		pinba_pool_destroy(D->per_thread_temp_pools + i);
	}
	free(D->per_thread_temp_pools);

	for (i = 0; i < D->thread_pool->size; i++) {
		pinba_pool_destroy(D->per_thread_request_pools + i);
	}
	free(D->per_thread_request_pools);

	pinba_debug("shutting down with %ld elements in tag.table", JudyLCount(D->tag.table, 0, -1, NULL));
	pinba_debug("shutting down with %ld elements in tag.name_index", JudyLCount(D->tag.name_index, 0, -1, NULL));

	th_pool_destroy(D->thread_pool);

	pthread_rwlock_unlock(&D->collector_lock);
	pthread_rwlock_destroy(&D->collector_lock);

	pthread_rwlock_unlock(&D->temp_lock);
	pthread_rwlock_destroy(&D->temp_lock);

	pthread_rwlock_unlock(&D->data_lock);
	pthread_rwlock_destroy(&D->data_lock);

	pinba_tag_reports_destroy(1);
	JudySLFreeArray(&D->tag_reports, NULL);

	pthread_rwlock_destroy(&D->timer_lock);
	pinba_reports_destroy();

	JudySLFreeArray(&D->base_reports, NULL);

	pthread_rwlock_unlock(&D->tag_reports_lock);
	pthread_rwlock_destroy(&D->tag_reports_lock);

	pthread_rwlock_unlock(&D->base_reports_lock);
	pthread_rwlock_destroy(&D->base_reports_lock);

	for (id = 0; id < D->dict.count; id++) {
		word = D->dict.table[id];
		free(word->str);
		free(word);
	}

	id = 0;
	for (ppvalue = JudyLFirst(D->tag.table, &id, NULL); ppvalue && ppvalue != PPJERR; ppvalue = JudyLNext(D->tag.table, &id, NULL)) {
		tag = (pinba_tag *)*ppvalue;
		free(tag);
	}

	free(D->dict.table);
	JudyLFreeArray(&D->tag.table, NULL);
	JudySLFreeArray(&D->tag.name_index, NULL);
	JudySLFreeArray(&D->dict.word_index, NULL);

	event_base_free(D->base);
	free(D);
	D = NULL;

	pinba_debug("collector shut down");
}
/* }}} */

void *pinba_collector_main(void *arg) /* {{{ */
{
	pinba_debug("starting up collector thread");

	D->collector_socket = pinba_socket_open(D->settings.address, D->settings.port);
	if (!D->collector_socket) {
		return NULL;
	}

	event_base_dispatch(D->base);

	/* unreachable */
	return NULL;
}
/* }}} */

struct data_job_data {
	int start;
	int end;
	struct timeval now;
	int thread_num;
};

static void data_job_func(void *job_data) /* {{{ */
{
	int i, bucket_id;
	bool res;
	pinba_data_bucket *bucket;
	pinba_tmp_stats_record *tmp_record;
	struct data_job_data *d = (struct data_job_data *)job_data;
	pinba_pool *data_pool = &D->data_pool;
	pinba_pool *temp_pool = &D->per_thread_temp_pools[d->thread_num];

	bucket_id = d->start;
	if (bucket_id >= data_pool->size) {
		bucket_id = bucket_id - data_pool->size;
	}

	for (i = d->start; i < d->end; i++, bucket_id = (bucket_id == data_pool->size - 1) ? 0 : bucket_id + 1) {
		int sub_request_num = -1;
		int current_sub_request = -1;
		Pinba__Request *parent_request = NULL;

		bucket = DATA_POOL(data_pool) + bucket_id;

		do {
			if (UNLIKELY(temp_pool->in == temp_pool->size)) {
				int old_size = temp_pool->size;
				unsigned int n;

				/* enlarge the pool */
				if (pinba_pool_grow(temp_pool, PINBA_PER_THREAD_POOL_GROW_SIZE) != P_SUCCESS) {
					return;
				}
			}

			tmp_record = TMP_POOL(temp_pool) + temp_pool->in;
			tmp_record->time = d->now;

			if (sub_request_num == -1) {

				tmp_record->request = pinba__request__unpack(NULL, bucket->len, (const unsigned char *)bucket->buf);
				if (UNLIKELY(tmp_record->request == NULL)) {
					continue;
				}

				tmp_record->free = 1;

				sub_request_num = tmp_record->request->n_requests;
				if (sub_request_num > 0) {
					parent_request = tmp_record->request;
					current_sub_request = 0;
				} else {
					sub_request_num = -1;
				}
			} else {
				tmp_record->request = parent_request->requests[current_sub_request];
				tmp_record->free = 0;
				current_sub_request++;
			}
			temp_pool->in++;
		} while (current_sub_request < sub_request_num);
	}
}
/* }}} */

static void data_copy_job_func(void *job_data) /* {{{ */
{
	int i;
	unsigned int tmp_id;
	pinba_tmp_stats_record *thread_tmp_record, *tmp_record;
	struct data_job_data *d = (struct data_job_data *)job_data;
	pinba_pool *thread_temp_pool = &D->per_thread_temp_pools[d->thread_num];
	pinba_pool *temp_pool = &D->temp_pool;

	tmp_id = temp_pool->in + d->start;
	if (tmp_id >= temp_pool->size) {
		tmp_id = tmp_id - temp_pool->size;
	}

	for (i = 0; i < thread_temp_pool->in; i++, tmp_id = (tmp_id == temp_pool->size - 1) ? 0 : tmp_id + 1) {
			thread_tmp_record = TMP_POOL(thread_temp_pool) + i;
			tmp_record = TMP_POOL(temp_pool) + tmp_id;

			tmp_record->time = thread_tmp_record->time;
			if (tmp_record->request && tmp_record->free) {
				pinba__request__free_unpacked(tmp_record->request, NULL);
			}
			tmp_record->request = thread_tmp_record->request;
			tmp_record->free = thread_tmp_record->free;
			thread_tmp_record->request = NULL;
	}
	thread_temp_pool->in = 0;
}
/* }}} */

void *pinba_data_main(void *arg) /* {{{ */
{
	struct timeval launch;
	struct data_job_data *job_data_arr;

	pinba_debug("starting up data harvester thread");

	/* yes, it's a minor memleak. once per process start. */
	job_data_arr = (struct data_job_data *)malloc(sizeof(struct data_job_data) * D->thread_pool->size);

	gettimeofday(&launch, 0);

	for (;;) {
		struct timeval tv1;

		pthread_rwlock_rdlock(&D->data_lock);
		if (UNLIKELY(pinba_pool_num_records(&D->data_pool) == 0)) {
			pthread_rwlock_unlock(&D->data_lock);
		} else {
			pinba_pool *data_pool = &D->data_pool;
			pinba_pool *temp_pool = &D->temp_pool;
			int i = 0, num, accounted, job_size;
			thread_pool_barrier_t barrier;

			pthread_rwlock_unlock(&D->data_lock);

			/* since we now support multi-request packets, we cannot assume that
			   the number of data packets == the number of requests, so we have to do this
			   in two steps */

			/* Step 1: harvest the data and put the decoded packets to per-thread temp pools */

			pthread_rwlock_wrlock(&D->data_lock);
			num = pinba_pool_num_records(data_pool);

			if (num < (D->thread_pool->size * PINBA_THREAD_POOL_THRESHOLD_AMOUNT)) {
				job_size = num;
			} else {
				job_size = num/D->thread_pool->size;
			}

			memset(job_data_arr, 0, sizeof(struct data_job_data) * D->thread_pool->size);

			th_pool_barrier_init(&barrier);
			th_pool_barrier_start(&barrier);

			accounted = 0;
			for (i = 0; i < D->thread_pool->size; i++) {
				job_data_arr[i].start = accounted;
				job_data_arr[i].end = accounted + job_size;
				if (job_data_arr[i].end > num) {
					job_data_arr[i].end = num;
					accounted = num;
				} else {
					accounted += job_size;
					if (i == (D->thread_pool->size - 1)) {
						job_data_arr[i].end = num;
						accounted = num;
					}
				}
				job_data_arr[i].thread_num = i;
				job_data_arr[i].now.tv_sec = launch.tv_sec;
				job_data_arr[i].now.tv_usec = launch.tv_usec;
				th_pool_dispatch(D->thread_pool, &barrier, data_job_func, &(job_data_arr[i]));
				if (accounted == num) {
					break;
				}
			}
			th_pool_barrier_end(&barrier);

			data_pool->in = data_pool->out = 0;
			pthread_rwlock_unlock(&D->data_lock);

			/* now we know how much request packets we have and can divide the job between the threads */
			/* Step 2: move decoded packets from per-thread temp pools to the global temp pool */
			pthread_rwlock_wrlock(&D->temp_lock);

			th_pool_barrier_init(&barrier);
			th_pool_barrier_start(&barrier);
			accounted = 0;
			for (i = 0; i < D->thread_pool->size; i++) {
				pinba_pool *thread_temp_pool = D->per_thread_temp_pools + i;

				if (thread_temp_pool->in == 0) {
					break;
				}

				job_data_arr[i].start = accounted;
				job_data_arr[i].end = accounted + thread_temp_pool->in;
				job_data_arr[i].thread_num = i;
				accounted += thread_temp_pool->in;
				th_pool_dispatch(D->thread_pool, &barrier, data_copy_job_func, &(job_data_arr[i]));
			}
			th_pool_barrier_end(&barrier);

//			pinba_error(P_WARNING, "new packets: %d, num temp_pool: %d, temp_pool->in: %d", accounted, pinba_pool_num_records(temp_pool), temp_pool->in);

			int old_num = pinba_pool_num_records(temp_pool);

			if ((temp_pool->in + accounted) >= temp_pool->size) {
				temp_pool->in = (temp_pool->in + accounted) - temp_pool->size;
			} else {
				temp_pool->in += accounted;
			}
			if ((pinba_pool_num_records(temp_pool) - old_num) != accounted) {
				pinba_error(P_WARNING, "new temp packets: %d != accounted: %d", pinba_pool_num_records(temp_pool) - old_num, accounted);
			}
			pthread_rwlock_unlock(&D->temp_lock);
		}

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

char *pinba_error_ex(int return_error, int type, const char *file, int line, const char *format, ...) /* {{{ */
{
	va_list args;
	const char *type_name;
	char *tmp;
	int errormsg_len = 0;
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
	errormsg_len = vsnprintf(errormsg, PINBA_ERR_BUFFER, tmp_format, args);
	va_end(args);

	if (!return_error) {
		fprintf(stderr, "%s\n", errormsg);
		fflush(stderr);
		return NULL;
	}
	tmp = strdup(errormsg);
	return tmp;
}
/* }}} */

void pinba_udp_read_callback_fn(int sock, short event, void *arg) /* {{{ */
{
	if (event & EV_READ) {
		int ret;
		unsigned char buf[PINBA_UDP_BUFFER_SIZE];
		struct sockaddr_in from;
		socklen_t fromlen = sizeof(struct sockaddr_in);

		while((ret = recvfrom(sock, buf, PINBA_UDP_BUFFER_SIZE-1, MSG_DONTWAIT, (sockaddr *)&from, &fromlen)) > 0) {
			pinba_data_bucket *bucket;
			pinba_pool *data_pool = &D->data_pool;

			pthread_rwlock_wrlock(&D->data_lock);
			if (UNLIKELY(pinba_pool_is_full(data_pool))) {
				/* the pool is full! fly, you fool! */
			} else {
				bucket = DATA_POOL(data_pool) + data_pool->in;
				bucket->len = 0;
				if (bucket->alloc_len < ret) {
					bucket->buf = (char *)realloc(bucket->buf, ret);
					bucket->alloc_len = ret;
				}

				if (UNLIKELY(!bucket->buf)) {
					/* OUT OF MEM */
					bucket->alloc_len = ret;
				} else {
					memcpy(bucket->buf, buf, ret);
					bucket->len = ret;

					data_pool->in++;
					if (UNLIKELY(data_pool->in == data_pool->size)) {
						data_pool->in = 0;
					}
				}
			}
			pthread_rwlock_unlock(&D->data_lock);
		}
	}
}
/* }}} */

void pinba_socket_free(pinba_socket *socket) /* {{{ */
{
	if (!socket) {
		return;
	}

	if (socket->listen_sock >= 0) {
		close(socket->listen_sock);
		socket->listen_sock = -1;
	}

	if (socket->accept_event) {
		event_del(socket->accept_event);
		free(socket->accept_event);
		socket->accept_event = NULL;
	}

	free(socket);
}
/* }}} */

pinba_socket *pinba_socket_open(char *ip, int listen_port) /* {{{ */
{
	struct sockaddr_in addr;
	pinba_socket *s;
	int sfd, flags, yes = 1;

	if ((sfd = socket(AF_INET, SOCK_DGRAM, 0)) == -1) {
		pinba_error(P_ERROR, "socket() failed: %s (%d)", strerror(errno), errno);
		return NULL;
	}

	if ((flags = fcntl(sfd, F_GETFL, 0)) < 0 || fcntl(sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
		close(sfd);
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

	s->accept_event = (struct event *)calloc(1, sizeof(struct event));
	if (!s->accept_event) {
		pinba_error(P_ERROR, "calloc() failed: %s (%d)", strerror(errno), errno);
		pinba_socket_free(s);
		return NULL;
	}

	event_set(s->accept_event, s->listen_sock, EV_READ | EV_PERSIST, pinba_udp_read_callback_fn, s);
	event_base_set(D->base, s->accept_event);
	event_add(s->accept_event, NULL);
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
