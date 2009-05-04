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

/* $Id: main.cc,v 1.1.2.14 2009/04/16 11:53:34 tony Exp $ */

#include "pinba.h"

int pinba_collector_init(pinba_daemon_settings settings) /* {{{ */
{
	int i;

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

	if (settings.show_protobuf_errors == 0) {
		google::protobuf::SetLogHandler(NULL);
	}

	pinba_debug("initializing collector");

	D = (pinba_daemon *)calloc(1, sizeof(pinba_daemon));

	D->base = event_base_new();

	pthread_rwlock_init(&D->collector_lock, 0);
	pthread_rwlock_init(&D->temp_lock, 0);
	
	pthread_rwlock_init(&D->tag_reports_lock, 0);

	if (pinba_pool_init(&D->temp_pool, settings.temp_pool_size + 1, sizeof(pinba_tmp_stats_record), pinba_temp_pool_dtor) != P_SUCCESS) {
		return P_FAILURE;
	}
	
	for (i = 0; i < settings.temp_pool_size + 1; i++) {
		pinba_tmp_stats_record *tmp_record = TMP_POOL(&D->temp_pool) + i;
		new (&(tmp_record->request)) Pinba::Request;
	}

	if (pinba_pool_init(&D->request_pool, settings.request_pool_size + 1, sizeof(pinba_stats_record), pinba_request_pool_dtor) != P_SUCCESS) {
		return P_FAILURE;
	}

	if (pinba_pool_init(&D->timer_pool, PINBA_TIMER_POOL_GROW_SIZE, sizeof(pinba_timer_position), NULL) != P_SUCCESS) {
		return P_FAILURE;
	}

	D->timers_cnt = 0;
	D->timertags_cnt = 0;

	D->settings = settings;

	for (i = 0; i < PINBA_BASE_REPORT_LAST; i++) {
		pthread_rwlock_init(&(D->base_reports[i].lock), 0);
	}

	return P_SUCCESS;
}
/* }}} */

void pinba_collector_shutdown(void) /* {{{ */
{
	int i;
	Word_t id;
	pinba_word *word;
	pinba_tag *tag;
	PPvoid_t ppvalue;

	pinba_debug("shutting down..");
	pthread_rwlock_wrlock(&D->collector_lock);
	pthread_rwlock_wrlock(&D->temp_lock);

	pinba_socket_free(D->collector_socket);


	pinba_debug("shutting down with %ld (of %ld) elements in the pool", pinba_pool_num_records(&D->request_pool), D->request_pool.size);
	pinba_debug("shutting down with %ld (of %ld) elements in the temp pool", pinba_pool_num_records(&D->temp_pool), D->temp_pool.size);
	pinba_debug("shutting down with %ld (of %ld) elements in the timer pool", pinba_pool_num_records(&D->timer_pool), D->timer_pool.size);

	pinba_pool_destroy(&D->request_pool);
	pinba_pool_destroy(&D->temp_pool);
	pinba_pool_destroy(&D->timer_pool);

	pinba_debug("shutting down with %ld elements in tag.table", JudyLCount(D->tag.table, 0, -1, NULL));
	pinba_debug("shutting down with %ld elements in tag.name_index", JudyLCount(D->tag.name_index, 0, -1, NULL));

	pthread_rwlock_unlock(&D->collector_lock);
	pthread_rwlock_destroy(&D->collector_lock);
	
	pthread_rwlock_unlock(&D->temp_lock);
	pthread_rwlock_destroy(&D->temp_lock);

	pinba_tag_reports_destroy(1);
	JudySLFreeArray(&D->tag_reports, NULL);

	pthread_rwlock_unlock(&D->tag_reports_lock);
	pthread_rwlock_destroy(&D->tag_reports_lock);
	
	pinba_reports_destroy();

	for (i = 0; i < PINBA_BASE_REPORT_LAST; i++) {
		pthread_rwlock_unlock(&D->base_reports[i].lock);
		pthread_rwlock_destroy(&D->base_reports[i].lock);
	}

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

		ret = recvfrom(sock, buf, PINBA_UDP_BUFFER_SIZE-1, MSG_DONTWAIT, (sockaddr *)&from, &fromlen);
		if (ret > 0) {
			/* pinba_debug("GOT DATA (%d bytes, errno: %d, error: %s)", ret, errno, errno ? strerror(errno) : ""); */
			if (pinba_process_stats_packet(buf, ret) != P_SUCCESS) {
				pinba_debug("failed to parse data received from %s", inet_ntoa(from.sin_addr));
			}
		} else if (ret < 0) {
			if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
				/* pinba_debug("read() failed: %s (%d)", strerror(errno), errno); */
				return;
			}
			pinba_error(P_WARNING, "recv() failed: %s (%d)", strerror(errno), errno);
		} else {
			pinba_error(P_WARNING, "recv() returned 0");
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

/* 
 * vim600: sw=4 ts=4 fdm=marker
 */
