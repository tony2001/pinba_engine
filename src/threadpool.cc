/**
 * threadpool.c
 *
 * This file will contain your implementation of a threadpool.
 */

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>
#include <errno.h>

#include "pinba.h"
#include "threadpool.h"

#define MAX_QUEUE_MEMORY_SIZE 65536

static inline queue_head_t *queue_create(int initial_cap) /* {{{ */
{
	queue_head_t *theQueue = (queue_head_t *) malloc (sizeof(queue_head_t));
	int max_cap = MAX_QUEUE_MEMORY_SIZE / (sizeof(queue_node_t));
	int i;
	queue_node_t *temp;

	if (theQueue == NULL) {
		return NULL;
	}

	if (initial_cap > max_cap) {
		initial_cap = max_cap;
	}

	if (initial_cap == 0) {
		return NULL;
	}

	theQueue->capacity = initial_cap;
	theQueue->posted = 0;
	theQueue->max_capacity = max_cap;
	theQueue->head = NULL;
	theQueue->tail = NULL;
	theQueue->freeHead = (queue_node_t *) malloc (sizeof(queue_node_t));

	if (theQueue->freeHead == NULL) {
		free(theQueue);
		return NULL;
	}
	theQueue->freeTail = theQueue->freeHead;

	//populate the free queue
	for(i = 0; i< initial_cap; i++) {
		temp = (queue_node_t *) malloc (sizeof(queue_node_t));
		if (temp == NULL) {
			return theQueue;
		}
		temp->next = theQueue->freeHead;
		temp->prev = NULL;
		theQueue->freeHead->prev = temp;
		theQueue->freeHead = temp;
	}

	return theQueue;
}
/* }}} */

static inline void queue_destroy(queue_head_t *queue) /* {{{ */
{
	queue_node_t *node, *temp;

	temp = queue->head;
	while (temp) {
		node = temp;
		temp = node->next;
		free(node);
	}
	
	temp = queue->freeHead;
	while (temp) {
		node = temp;
		temp = node->next;
		free(node);
	}
	free(queue);
}
/* }}} */

static inline void queue_post_job(queue_head_t * theQueue, thread_pool_barrier_t *barrier, dispatch_fn_t func1, void * arg1, dispatch_fn_t func2, void * arg2) /* {{{ */
{
	queue_node_t *temp;
		
	if (theQueue->freeTail == NULL) {
	    temp = (queue_node_t *) malloc (sizeof(queue_node_t));
		if (temp == NULL) {
			return;
		}
		temp->next = NULL;
		temp->prev = NULL;
		theQueue->freeHead = temp;
		theQueue->freeTail = temp;
		theQueue->capacity++;
	}
	
	temp = theQueue->freeTail;
	if (theQueue->freeTail->prev == NULL) {
		theQueue->freeTail = NULL;
		theQueue->freeHead = NULL;
	} else {
		theQueue->freeTail->prev->next= NULL;
		theQueue->freeTail = theQueue->freeTail->prev;
		theQueue->freeTail->next = NULL;
	}

	theQueue->posted++;
	temp->func_to_dispatch = func1;
	temp->func_arg = arg1;
	temp->cleanup_func = func2;
	temp->cleanup_arg = arg2;
	temp->barrier = barrier;
	if (barrier) {
		barrier->posted_count++;
	}

	temp->prev = theQueue->tail;
	temp->next = NULL;

	if (theQueue->tail) {
		theQueue->tail->next = temp;
	} else {
		theQueue->head = temp;
	}
	theQueue->tail = temp;
}
/* }}} */

static inline void queue_fetch_job(queue_head_t * theQueue, thread_pool_barrier_t **barrier, dispatch_fn_t * func1, void ** arg1, dispatch_fn_t * func2, void ** arg2) /* {{{ */
{
	queue_node_t *temp;
		
	temp = theQueue->tail;
	if (temp == NULL) {
		return;
	}

	if (temp->prev) {
		temp->prev->next = NULL;
	} else {
		theQueue->head = NULL;
	}
	theQueue->tail = temp->prev;

	*func1 = temp->func_to_dispatch;
	*arg1  = temp->func_arg;
	*func2 = temp->cleanup_func;
	*arg2  = temp->cleanup_arg;
	*barrier = temp->barrier;

	temp->next = NULL;
	temp->prev = NULL;
	if (theQueue->freeHead == NULL) {
		theQueue->freeTail = temp;
		theQueue->freeHead = temp;
	} else {
		temp->next = theQueue->freeHead;
		theQueue->freeHead->prev = temp;
		theQueue->freeHead = temp;
	}
}
/* }}} */

static inline int queue_can_accept_order(queue_head_t * theQueue) /* {{{ */
{
	return (theQueue->freeTail != NULL || theQueue->capacity <= theQueue->max_capacity);
}
/* }}} */

static inline int queue_is_job_available(queue_head_t * theQueue) /* {{{ */
{
  return (theQueue->tail != NULL);
}
/* }}} */


#if THREAD_POOL_DEBUG
static inline void etfprintf(struct timeval then, ...) /* {{{ */
{
	va_list         args;
	struct timeval  now;
	struct timeval  diff;
	FILE           *out;
	char           *fmt;

	va_start(args, then);
	out = va_arg(args, FILE *);
	fmt = va_arg(args, char *);

	gettimeofday(&now, NULL);
	timersub(&now, &then, &diff);
	fprintf(out, "%03d.%06d:", diff.tv_sec, diff.tv_usec);
	vfprintf(out, fmt, args);
	fflush(NULL);
	va_end(args);
}
/* }}} */
#endif

static void th_pool_mutex_unlock_wrapper(void *data) /* {{{ */
{
	pthread_mutex_t *mutex = (pthread_mutex_t *)data;
	pthread_mutex_unlock(mutex);
}
/* }}} */

/* The Worker function */
static void *th_do_work(void *data) /* {{{ */
{
	thread_pool_t *pool = (thread_pool_t *)data; 
	thread_pool_barrier_t *barrier;
#if THREAD_POOL_DEBUG
	int myid = pool->live; /* this is wrong, but we need it only for debugging */
#endif
	
	/* When we get a posted job, we copy it into these local vars */
	dispatch_fn_t  myjob;
	void        *myarg;  
	dispatch_fn_t  mycleaner;
	void        *mycleanarg;

	TP_DEBUG(pool, " >>> Thread[%d] starting, grabbing mutex.\n", myid);

	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, NULL);
	pthread_cleanup_push(th_pool_mutex_unlock_wrapper, (void *)&pool->mutex);

	/* Grab mutex so we can begin waiting for a job */
	if (0 != pthread_mutex_lock(&pool->mutex)) {
		return NULL;
	}

	/* Main loop: wait for job posting, do job(s) ... forever */
	for( ; ; ) {

		TP_DEBUG(pool, " <<< Thread[%d] waiting for signal.\n", myid);

		/* only look for jobs if we're not in shutdown */
		while(queue_is_job_available(pool->job_queue) == 0) {
			pthread_cond_wait(&pool->job_posted, &pool->mutex);
		}

		TP_DEBUG(pool, " >>> Thread[%d] received signal.\n", myid);

		/* else execute all the jobs first */
		queue_fetch_job(pool->job_queue, &barrier, &myjob, &myarg, &mycleaner, &mycleanarg);
		if (myjob == (void *)-1) {
			break;
		}
		pthread_cond_signal(&pool->job_taken);

		TP_DEBUG(pool, " <<< Thread[%d] yielding mutex, taking job.\n", myid);

		/* unlock mutex so other jobs can be posted */
		if (0 != pthread_mutex_unlock(&pool->mutex)) {
			return NULL;
		}

		/* Run the job we've taken */
		if(mycleaner != NULL) {
			pthread_cleanup_push(mycleaner,mycleanarg);
			myjob(myarg);
			pthread_cleanup_pop(1);
		} else {
			myjob(myarg);
		}

		TP_DEBUG(pool, " >>> Thread[%d] JOB DONE!\n", myid);
		if (barrier) {
			/* Job done! */
			th_pool_barrier_signal(barrier);
		}

		/* Grab mutex so we can grab posted job, or (if no job is posted)
		   begin waiting for next posting. */
		if (0 != pthread_mutex_lock(&pool->mutex)) {
			return NULL;
		}
	}

	/* If we get here, we broke from loop because state is ALL_EXIT */
	--pool->live;

	TP_DEBUG(pool, " <<< Thread[%d] exiting (signalling 'job_taken').\n", myid);

	/* We're not really taking a job ... but this signals the destroyer
	   that one thread has exited, so it can keep on destroying. */
	pthread_cond_signal(&pool->job_taken);

	pthread_cleanup_pop(0);
	pthread_mutex_unlock(&pool->mutex);
	return NULL;
}  
/* }}} */


thread_pool_t *th_pool_create(int num_threads_in_pool) /* {{{ */
{
	thread_pool_t *pool;
	int i;

	if (num_threads_in_pool <= 0 || num_threads_in_pool > MAXT_IN_POOL) {
		return NULL;
	}

	pool = (thread_pool_t *) malloc(sizeof(thread_pool_t));
	if (pool == NULL) {
		return NULL;
	}

	pthread_mutex_init(&(pool->mutex), NULL);
	pthread_cond_init(&(pool->job_posted), NULL);
	pthread_cond_init(&(pool->job_taken), NULL);
	pool->size = num_threads_in_pool;
	pool->job_queue = queue_create(num_threads_in_pool);
#if THREAD_POOL_DEBUG
	gettimeofday(&pool->created, NULL);
#endif

	pool->threads = (pthread_t *) malloc(pool->size * sizeof(pthread_t));
	if (NULL == pool->threads) {
		free(pool);
		return NULL;
	}

	pool->live = 0;
	for (i = 0; i < pool->size; i++) {
		if (0 != pthread_create(pool->threads + i, NULL, th_do_work, (void *) pool)) {
			free(pool->threads);
			free(pool);
			return NULL;
		}
		pool->live++;
		pthread_detach(pool->threads[i]);
	}

	TP_DEBUG(pool, " <<< Threadpool created with %d threads.\n", num_threads_in_pool);

	return pool;
}
/* }}} */

void th_pool_dispatch_with_cleanup(thread_pool_t *from_me, thread_pool_barrier_t *barrier, dispatch_fn_t dispatch_to_here, void *arg, dispatch_fn_t cleaner_func, void * cleaner_arg) /* {{{ */
{
	thread_pool_t *pool = (thread_pool_t *) from_me;
	
	pthread_cleanup_push(th_pool_mutex_unlock_wrapper, (void *) &pool->mutex);
	TP_DEBUG(pool, " >>> Dispatcher: grabbing mutex.\n");

	if (0 != pthread_mutex_lock(&pool->mutex)) {
		TP_DEBUG(pool, " >>> Dispatcher: failed to lock mutex!\n");
		return;
	}

	while(!queue_can_accept_order(pool->job_queue)) {
		TP_DEBUG(pool, " <<< Dispatcher: job queue full, %s\n", "signaling 'posted', waiting on 'taken'.");
		pthread_cond_signal(&pool->job_posted);
		pthread_cond_wait(&pool->job_taken, &pool->mutex);
	}

	/* Finally, there's room to post a job. Do so and signal workers */
	TP_DEBUG(pool, " <<< Dispatcher: posting job, signaling 'posted', yielding mutex\n");
	queue_post_job(pool->job_queue, barrier, dispatch_to_here, arg, cleaner_func, cleaner_arg);

	pthread_cond_signal(&pool->job_posted);

	pthread_cleanup_pop(0);
	/* unlock mutex so a worker can pick up the job */
	pthread_mutex_unlock(&pool->mutex);
}
/* }}} */

void th_pool_destroy(thread_pool_t *destroyme) /* {{{ */
{
	thread_pool_t *pool = (thread_pool_t *) destroyme;
	int oldtype;

	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, &oldtype);
	pthread_cleanup_push(th_pool_mutex_unlock_wrapper, (void *) &pool->mutex); 

	/* Cause all threads to exit. Because they were detached when created,
	   the underlying memory for each is automatically reclaimed. */
	TP_DEBUG(pool, " >>> Destroyer: grabbing mutex, posting exit job. Live = %d\n", pool->live);

	if (0 != pthread_mutex_lock(&pool->mutex)) {
		return;
	}

	while (pool->live > 0) {
		TP_DEBUG(pool, " <<< Destroyer: signalling 'job_posted', waiting on 'job_taken'.\n");
		queue_post_job(pool->job_queue, NULL, (dispatch_fn_t) -1, NULL, NULL, NULL);
		/* get workers to check in ... */
		pthread_cond_signal(&pool->job_posted);
		/* ... and wake up when they check out */
		pthread_cond_wait(&pool->job_taken, &pool->mutex);
		TP_DEBUG(pool, " >>> Destroyer: received 'job_taken'. Live = %d\n", pool->live);
	}

	memset(pool->threads, 0, pool->size * sizeof(pthread_t));
	free(pool->threads);

	TP_DEBUG(pool, " <<< Destroyer: releasing mutex prior to destroying it.\n");

	pthread_cleanup_pop(0);  
	if (0 != pthread_mutex_unlock(&pool->mutex)) {
		return;
	}

	TP_DEBUG(pool, " --- Destroyer: destroying mutex.\n");

	if (0 != pthread_mutex_destroy(&pool->mutex)) {
		return;
	}

	TP_DEBUG(pool, " --- Destroyer: destroying conditional variables.\n");

	if (0 != pthread_cond_destroy(&pool->job_posted)) {
		return;
	}

	if (0 != pthread_cond_destroy(&pool->job_taken)) {
		return;
	}

	queue_destroy(pool->job_queue);
	memset(pool, 0, sizeof(thread_pool_t));

	free(pool);
	pool = NULL;
	destroyme = NULL;
}
/* }}} */

void th_pool_destroy_immediately(thread_pool_t *destroymenow) /* {{{ */
{
	thread_pool_t *pool = (thread_pool_t *) destroymenow;
	int oldtype,i;

	pthread_setcanceltype(PTHREAD_CANCEL_DEFERRED, &oldtype);

	pthread_cleanup_push(th_pool_mutex_unlock_wrapper, (void *) &pool->mutex);
	pthread_mutex_lock(&pool->mutex);


	for(i = 0; i < pool->size; i++) {
		pthread_cancel(pool->threads[i]);
	}

	TP_DEBUG(pool, " --- Destroyer: destroying mutex.\n");

	pthread_cleanup_pop(0);

	if (0 != pthread_mutex_destroy(&pool->mutex)) {
		TP_DEBUG(pool, " --- failed to destroy mutex: %s (%d)\n", strerror(errno), errno);
	}

	TP_DEBUG(pool, " --- Destroyer: destroying conditional variables.\n");

	pthread_cond_destroy(&pool->job_posted);
	pthread_cond_destroy(&pool->job_taken);
	
	memset(pool, 0, sizeof(thread_pool_t));
	free(pool);
	pool = NULL;
	destroymenow = NULL;
}
/* }}} */

void th_pool_barrier_init(thread_pool_barrier_t *b) /* {{{ */
{
	pthread_mutex_init(&b->mutex, NULL);
	pthread_cond_init(&b->var, NULL);
	b->posted_count = 0;
	b->done_count = 0;
}
/* }}} */

int th_pool_barrier_start(thread_pool_barrier_t *b) /* {{{ */
{
	b->posted_count = 0;
	b->done_count = 0;
	return 0;
}
/* }}} */

void th_pool_barrier_signal(thread_pool_barrier_t *b) /* {{{ */
{
	pthread_mutex_lock(&b->mutex);
	b->done_count++;
	pthread_cond_signal(&b->var);
	pthread_mutex_unlock(&b->mutex);
}
/* }}} */

void th_pool_barrier_wait(thread_pool_barrier_t *b) /* {{{ */
{
	pthread_mutex_lock(&b->mutex);
	while (b->done_count < b->posted_count) {
		pthread_cond_wait(&b->var, &b->mutex);
	}
	pthread_mutex_unlock(&b->mutex);
}
/* }}} */

void th_pool_barrier_destroy(thread_pool_barrier_t *b) /* {{{ */
{
	pthread_mutex_destroy(&b->mutex);
	pthread_cond_destroy(&b->var);
}
/* }}} */

void th_pool_barrier_end(thread_pool_barrier_t *b) /* {{{ */
{
	th_pool_barrier_wait(b);
	th_pool_barrier_destroy(b);
}
/* }}} */

