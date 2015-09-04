#ifndef HAVE_PINBA_LMAP_H
# define HAVE_PINBA_LMAP_H

void *pinba_lmap_first(void *map_report, uint64_t *index_to_fill);
void *pinba_lmap_next(void *map_report, uint64_t *index_to_fill);
void *pinba_lmap_get(void *map_report, uint64_t index);
void *pinba_lmap_add(void *map_report, uint64_t index, const void *report);
int pinba_lmap_delete(void *map_report, uint64_t index);
void pinba_lmap_destroy(void *data);
void *pinba_lmap_create();
size_t pinba_lmap_count(void *map_report);

#endif /* HAVE_PINBA_LMAP_H */
