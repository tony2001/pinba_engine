
	/* (c) 2007-2012 Andrei Nigmatulin */

#ifndef ARRAY_H
#define ARRAY_H 1

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct array_s {
	void *data;
	size_t sz;
	unsigned long used;
	unsigned long allocated;
};

typedef struct array_s array_t;

/* fuck php */
#ifndef array_init
#define array_init array_init0
#endif

static inline struct array_s *array_init0(struct array_s *a, size_t sz, unsigned long initial_num)
{
	void *allocated = 0;

	if (!a) {
		a = (struct array_s *)malloc(sizeof(struct array_s));

		if (!a) {
			return 0;
		}

		allocated = a;
	}

	a->sz = sz;

	if (initial_num) {
		a->data = calloc(sz, initial_num);

		if (!a->data) {
			free(allocated);
			return 0;
		}
	}
	else {
		a->data = 0;
	}

	a->allocated = initial_num;
	a->used = 0;

	return a;
}

static inline void *array_item(struct array_s *a, unsigned long n)
{
	void *ret;

	ret = (char *) a->data + a->sz * n;

	return ret;
}

#define array_v(a, type) ((type *) (a)->data)

#define array_mem_used(a) ( (a)->sz * (a)->allocated )

#define array_init_static(a) { .data = a, .sz = sizeof(a[0]), .allocated = sizeof(a) / sizeof(a[0]) }

static inline void *array_item_last(struct array_s *a)
{
	return array_item(a, a->used - 1);
}

static inline unsigned long array_item_remove(struct array_s *a, unsigned long n)
{
	/* XXX: it's broken for size_t */
	size_t ret = ~ (size_t) 0;

	if (n < a->used - 1) {
		void *last = array_item(a, a->used - 1);
		void *to_remove = array_item(a, n);

		memcpy(to_remove, last, a->sz);

		ret = n;
	}

	--a->used;

	return ret;
}

static inline void array_item_remove_with_shift(struct array_s *a, unsigned long n)
{
	if (n >= a->used) {
		return;
	}

	memmove(array_item(a, n), array_item(a, n + 1), a->sz * (a->used - n - 1));
	a->used --;
}

static inline unsigned long array_item_no(struct array_s *a, void *item)
{
	return ((uintptr_t) item - (uintptr_t) a->data) / a->sz;
}

static inline unsigned long array_item_remove_ptr(struct array_s *a, void *item)
{
	return array_item_remove(a, array_item_no(a, item));
}

static inline int array_item_in(struct array_s *a, void *item)
{
	if (item < a->data || (char *) item >= (char *) a->data + a->sz * a->used) {
		return 0;
	}

	return 1;
}

static inline void *array_push(struct array_s *a)
{
	void *ret;

	if (a->used == a->allocated) {
		unsigned long new_allocated = a->allocated ? a->allocated * 2 : 16;
		void *new_ptr = realloc(a->data, a->sz * new_allocated);

		if (!new_ptr) {
			return 0;
		}

		a->data = new_ptr;
		a->allocated = new_allocated;
	}

	ret = array_item(a, a->used);

	++a->used;

	return ret;
}

#define array_push_v(a, type) ((type *) array_push(a))

static inline int array_enlarge(struct array_s *a, unsigned long new_sz, int clean)
{
	void *new_ptr = realloc(a->data, a->sz * new_sz);

	if (!new_ptr) {
		return -1;
	}

	if (clean && new_sz > a->allocated) {
		memset((char *) new_ptr + a->sz * a->allocated, 0, a->sz * (new_sz - a->allocated));
	}

	a->data = new_ptr;
	a->allocated = new_sz;

	return 0;
}

static inline int array_shrink(struct array_s *a)
{
	if (a->used == a->allocated) {
		return 0;
	}

	void *new_ptr = realloc(a->data, a->sz * a->used);
	if (!new_ptr) {
		return -1;
	}

	a->data = new_ptr;
	a->allocated = a->used;

	return 0;
}

static inline int array_reserve_with_ratio(struct array_s *a, unsigned long count, float ratio)
{
	if (a->used + count <= a->allocated) {
		return 0;
	}

	unsigned long new_sz = a->allocated ? a->allocated * ratio : 16;

	while (a->used + count > new_sz) {
		new_sz *= 2;
	}

	return array_enlarge(a, new_sz, 0);
}

static inline int array_reserve(struct array_s *a, unsigned long count)
{
	return array_reserve_with_ratio(a, count, 2.0);
}

static inline void array_free(struct array_s *a)
{
	free(a->data);
	a->data = 0;
	a->sz = 0;
	a->used = a->allocated = 0;
}

static inline int array_copy(struct array_s *d, const struct array_s *s)
{
	if (0 == array_init0(d, s->sz, s->used)) {
		return -1;
	}

	memcpy(d->data, s->data, s->sz * s->used);
	d->used = s->used;

	return 0;
}

static inline int array_append(struct array_s *d, const struct array_s *s)
{
	if (d->sz != s->sz) {
		return -1;
	}

	if (0 > array_reserve(d, s->used)) {
		return -1;
	}

	memcpy(array_item(d, d->used), s->data, s->sz * s->used);

	d->used += s->used;

	return 0;
}

#endif
