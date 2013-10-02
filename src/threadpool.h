#ifndef THREADPOOL_H
# define THREADPOOL_H

#define THREAD_POOL_DEBUG 0

// maximum number of threads allowed in a pool
#define MAXT_IN_POOL 200

// You must hide the internal details of the threadpool
// structure from callers, thus declare threadpool of type "void".
// In threadpool.c, you will use type conversion to coerce
// variables of type "threadpool" back and forth to a
// richer, internal type.  (See threadpool.c for details.)

//typedef void *thread_pool_t;

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>

// "dispatch_fn_t" declares a typed function pointer.  A
// variable of type "dispatch_fn_t" points to a function
// with the following signature:
// 
//     void dispatch_function(void *arg);

typedef struct _thread_pool_barrier_t {
	pthread_mutex_t mutex;
	pthread_cond_t  var;
	int posted_count;
	int done_count;
} thread_pool_barrier_t;

typedef void (*dispatch_fn_t)(void *);

typedef struct _queue_node_t queue_node_t;

struct _queue_node_t
{
  dispatch_fn_t func_to_dispatch;
  void *func_arg;
  dispatch_fn_t cleanup_func;
  void *cleanup_arg;
  thread_pool_barrier_t *barrier;
  queue_node_t *next;
  queue_node_t *prev;
};

typedef struct _queue_head_t
{
	queue_node_t *head;
	queue_node_t *tail;
	queue_node_t *freeHead;
	queue_node_t *freeTail;
	int capacity;
	int max_capacity;
	int posted;
} queue_head_t;


/*----------------------------------------------------------------------
  thread_pool_t is the internal threadpool structure that is cast to type 
  "threadpool" before it given out to callers.
  All accesses to a thread_pool_t (both read and write) must be protected 
  by that pool's mutex.  
  Worker threads wait on a "job_posted" signal; when one is received the 
  receiving thread (which has the mutex):
 * checks the pool state. If it is ALL_EXIT the thread decrements 
 the pool's 'live' count, yields the mutex and exits; otherwise 
continues:
 * while the job queue is not empty:
 * copy the job and arg fields into the thread, 
 * NULL out the pool's job and arg fields
 * signal "job_taken" 
 * yield the mutex and run the job on the arg.
 * grab the mutex
 * when a null job field is found, wait on "job_posted"
 The dispatcher thread dispatches a job as follows:
 * grab the pool's mutex
 * while the job field is not NULL: 
 * signal "job_posted"
 * wait on "job_taken"
 * copy job and args to pool, signal "job_posted", yield the mutex
 */
typedef struct _thread_pool_t {
#if THREAD_POOL_DEBUG
	struct timeval  created;    // When the threadpool was created.
#endif
	pthread_t      *threads;       // The threads themselves.
	pthread_mutex_t mutex;      // protects all vars declared below.
	size_t             size;       // Number of threads in the pool
	size_t             live;       // Number of live threads in pool (when
	//   pool is being destroyed, live<=arrsz)

	pthread_cond_t  job_posted; // dispatcher: "Hey guys, there's a job!"
	pthread_cond_t  job_taken;  // a worker: "Got it!"

	queue_head_t      *job_queue;      // queue of work orders
} thread_pool_t;

/**
 * Creates a fixed-sized thread
 * pool.  If the function succeeds, it returns a (non-NULL)
 * "threadpool", else it returns NULL.
 */
thread_pool_t *th_pool_create(int num_threads_in_pool);

/**
 * Sends a thread off to do some work.  If all threads in the pool are busy, dispatch will
 * block until a thread becomes free and is dispatched.
 * 
 * Once a thread is dispatched, this function returns immediately.
 *
 * Also enables the user to define cleanup handlers in 
 * cases of immediate cancel.  The cleanup handler function (cleaner_func) is 
 * executed automatically with cleaner_arg as the argument after the 
 * dispatch_to_here function is done; in the case that the thread is destroyed,
 * cleaner_func is also called before the thread exits.
 */
void th_pool_dispatch_with_cleanup(thread_pool_t *from_me, thread_pool_barrier_t *barrier, dispatch_fn_t dispatch_to_here, void * arg, dispatch_fn_t cleaner_func, void* cleaner_arg);

/**
 * 
 * The dispatched thread calls into the function
 * "dispatch_to_here" with argument "arg".
 */
#define th_pool_dispatch(from, barrier, to, arg) th_pool_dispatch_with_cleanup((from), (barrier), (to), (arg), NULL, NULL)

/**
 * Kills the threadpool, causing
 * all threads in it to commit suicide, and then
 * frees all the memory associated with the threadpool.
 */
void th_pool_destroy(thread_pool_t *destroyme);

/**
 * Cancels all threads in the threadpool immediately.
 * It is potentially dangerous to use with libraries that are not specifically 
 * asynchronous cancel thread safe. Also, without cleanup handlers, any dynamic 
 * memory or system resource in use could potentially be left without a reference
 * to it. 
 */
void th_pool_destroy_immediately(thread_pool_t *destroymenow);


void th_pool_barrier_init(thread_pool_barrier_t *b);
int th_pool_barrier_start(thread_pool_barrier_t *b);
void th_pool_barrier_signal(thread_pool_barrier_t *b);
void th_pool_barrier_wait(thread_pool_barrier_t *b);
void th_pool_barrier_end(thread_pool_barrier_t *b);
void th_pool_barrier_destroy(thread_pool_barrier_t *b);

#if THREAD_POOL_DEBUG
# define TP_DEBUG(pool, ...) etfprintf((pool)->created, stderr, __VA_ARGS__);
#else
# define TP_DEBUG(pool, ...)
#endif

#endif /* ifndef THREADPOOL_H */
