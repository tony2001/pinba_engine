#pragma once

void *pinba_map_first(void *map_report, char *index_to_fill);
void *pinba_map_next(void *map_report, char *index_to_fill);
void *pinba_map_get(void *map_report, const char *index);
void *pinba_map_add(void *map_report, const char *index, const void *report);
int pinba_map_del(void *map_report, const char *index);
void pinba_map_destroy(void *data);

