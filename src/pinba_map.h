#ifndef HAVE_PINBA_MAP_H
# define HAVE_PINBA_MAP_H

void *pinba_map_first(void *map_report, char *index_to_fill);
void *pinba_map_next(void *map_report, char *index_to_fill);
void *pinba_map_get(void *map_report, const char *index);
void *pinba_map_add(void *map_report, const char *index, const void *report);
int pinba_map_delete(void *map_report, const char *index);
void pinba_map_destroy(void *data);
void *pinba_map_create();
size_t pinba_map_count(void *map_report);

#endif /* HAVE_PINBA_MAP_H */
