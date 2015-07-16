
	/* (c) 2013 Andrei Nigmatulin */

	/* Naive memory pool allocator support for protobuf-c */

#ifndef NMPA_PBA_H
#define NMPA_PBA_H 1

#include "nmpa.h"

static inline void *nmpa___pba_alloc(void *v, size_t sz)
{
	struct nmpa_s *nmpa = (struct nmpa_s *)v;
	return nmpa_alloc(nmpa, sz);
}


static inline void nmpa___pba_free(void *v, void *ptr)
{
}

#if 0
#define nmpa_pba(nmpa) (& (ProtobufCAllocator) { \
	.alloc = nmpa___pba_alloc, \
	.free = nmpa___pba_free, \
	.allocator_data = (nmpa) \
	} )
#endif

#define nmpa_pba_init(nmpa_pba, nmpa) \
			(nmpa_pba)->alloc = nmpa___pba_alloc; \
			(nmpa_pba)->free = nmpa___pba_free; \
			(nmpa_pba)->allocator_data = (nmpa);

#endif
