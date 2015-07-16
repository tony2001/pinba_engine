
	/* (c) 2013-2014 Andrei Nigmatulin */

	/* Naive memory pool allocator */

#ifndef NMPA_H
#define NMPA_H 1

#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <errno.h>

#include "array.h"

struct nmpa_s {
	struct array_s pool;
	struct array_s big_chunks;
	size_t block_sz;
	unsigned next_empty;
};


static inline size_t nmpa_mem_used(const struct nmpa_s *nmpa)
{
	size_t ret = array_mem_used(&nmpa->pool) + array_mem_used(&nmpa->big_chunks);

	unsigned i;
	for (i = 0; i < nmpa->pool.used; i++) {
		const struct array_s *a = array_v(&nmpa->pool, struct array_s) + i;
		ret += array_mem_used(a);
	}

	for (i = 0; i < nmpa->big_chunks.used; i++) {
		const struct array_s *a = array_v(&nmpa->big_chunks, struct array_s) + i;
		ret += array_mem_used(a);
	}

	return ret;
}


static inline void nmpa_init(struct nmpa_s *nmpa, size_t block_sz)
{
	memset(nmpa, 0, sizeof(*nmpa));
	array_init(&nmpa->pool, sizeof(struct array_s), 0);
	array_init(&nmpa->big_chunks, sizeof(struct array_s), 0);
	nmpa->block_sz = block_sz;
}


static inline void nmpa_empty(struct nmpa_s *nmpa)
{
	const unsigned max_allocated_v = 16;
	const unsigned max_pool_items = 1;

	unsigned i;
	for (i = 0; i < nmpa->big_chunks.used; i++) {
		struct array_s *a = array_v(&nmpa->big_chunks, struct array_s) + i;
		array_free(a);
	}

	nmpa->big_chunks.used = 0;

	if (nmpa->big_chunks.allocated > max_allocated_v) {
		array_enlarge(&nmpa->big_chunks, max_allocated_v, 0);
	}

	while (nmpa->pool.used > max_pool_items) {
		struct array_s *a = (struct array_s *)array_item_last(&nmpa->pool);
		array_free(a);
		nmpa->pool.used --;
	}

	if (nmpa->pool.allocated > max_allocated_v) {
		array_enlarge(&nmpa->pool, max_allocated_v, 0);
	}

	nmpa->next_empty = 0;
}


static inline void nmpa_free(struct nmpa_s *nmpa)
{
	nmpa_empty(nmpa);
	for (unsigned i = 0; i < nmpa->pool.used; i++) {
		struct array_s *a = array_v(&nmpa->pool, struct array_s) + i;
		array_free(a);
	}
	array_free(&nmpa->pool);
	array_free(&nmpa->big_chunks);
	nmpa_init(nmpa, nmpa->block_sz);
}


static void *nmpa___big_chunk_alloc(struct nmpa_s *nmpa, size_t sz)
{
	struct array_s *a = (struct array_s *)array_push(&nmpa->big_chunks);

	if (!a) {
		errno = ENOMEM;
		return 0;
	}

	if (0 == array_init(a, 1, sz)) {
		nmpa->big_chunks.used --;
		errno = ENOMEM;
		return 0;
	}

	return a->data;
}


static inline void *nmpa___array_alloc(struct array_s *a, size_t sz)
{
	void *ret = (char *) a->data + a->used;

	a->used += sz;

	return ret;
}


static inline void *nmpa_alloc(struct nmpa_s *nmpa, size_t sz)
{
	if (__builtin_expect(sz > nmpa->block_sz, 0)) {
		return nmpa___big_chunk_alloc(nmpa, sz);
	}

	/* align to nearest sizeof(long) boundary */
	sz = (sz + sizeof(long) - 1) & -sizeof(long);

	struct array_s *a;

	if (__builtin_expect(nmpa->next_empty > 0, 1)) {
		/* can we allocate in the last non-empty array ? */
		a = array_v(&nmpa->pool, struct array_s) + nmpa->next_empty - 1;

		if (__builtin_expect(a->used + sz <= a->allocated, 1)) {
			return nmpa___array_alloc(a, sz);
		}
	}

	/* need one more array */
	if (__builtin_expect(nmpa->next_empty == nmpa->pool.used, 0)) {
		a = (struct array_s *)array_push(&nmpa->pool);

		if (!a) {
			errno = ENOMEM;
			return 0;
		}

		if (0 == array_init(a, 1, nmpa->block_sz)) {
			nmpa->pool.used --;
			errno = ENOMEM;
			return 0;
		}
	}
	else {
		a = array_v(&nmpa->pool, struct array_s) + nmpa->next_empty;
		a->used = 0;
	}

	nmpa->next_empty ++;

	return nmpa___array_alloc(a, sz);
}


static inline void *nmpa_calloc(struct nmpa_s *nmpa, size_t sz)
{
	void *ret = nmpa_alloc(nmpa, sz);

	if (ret) {
		memset(ret, 0, sz);
	}

	return ret;
}


static inline char *nmpa_strdup(struct nmpa_s *nmpa, const char *str)
{
	size_t len = strlen(str);

	char *dst = (char *) nmpa_alloc(nmpa, len + 1);

	if (dst) {
		strcpy(dst, str);
	}

	return dst;
}


static inline void *nmpa_realloc(struct nmpa_s *nmpa, void *old_ptr, size_t old_sz, size_t new_sz)
{
	/* in case one wants to decrease allocation size */
	if (old_ptr && new_sz <= old_sz) {
		return old_ptr;
	}

	void *new_ptr = nmpa_alloc(nmpa, new_sz);

	if (!new_ptr) {
		return 0;
	}

	if (old_ptr) {
		memcpy(new_ptr, old_ptr, old_sz);
	}

	return new_ptr;
}


static inline int nmpa_belongs(const struct nmpa_s *nmpa, const void *ptr)
{
	unsigned i;

	for (i = 0; i < nmpa->pool.used; i++) {
		const struct array_s *a = array_v(&nmpa->pool, struct array_s) + i;
		if (ptr >= a->data && (char *) ptr < (char *) a->data + a->allocated) {
			return 1;
		}
	}

	for (i = 0; i < nmpa->big_chunks.used; i++) {
		const struct array_s *a = array_v(&nmpa->big_chunks, struct array_s) + i;
		if (ptr >= a->data && (char *) ptr < (char *) a->data + a->allocated) {
			return 1;
		}
	}

	return 0;
}


static inline char *nmpa_vprintf(struct nmpa_s *nmpa, const char *fmt, va_list ap)
{
	va_list ap2;
	va_copy(ap2, ap);
	size_t len = 1 + vsnprintf(0, 0, fmt, ap2);
	va_end(ap2);

	char *text = (char *)nmpa_alloc(nmpa, len);

	if (text) {
		vsnprintf(text, len, fmt, ap);
	}

	return text;
}


static inline char * __attribute__ ((format(printf,2,3))) nmpa_printf(struct nmpa_s *nmpa, const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	char *text = nmpa_vprintf(nmpa, fmt, ap);
	va_end(ap);
	return text;
}


static inline void *nmpa_memdup(struct nmpa_s *nmpa, const void *buf, size_t size)
{
	void *ptr = nmpa_alloc(nmpa, size);

	if (ptr) {
		memcpy(ptr, buf, size);
	}

	return ptr;
}

#endif
