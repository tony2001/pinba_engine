#pragma once

void *tag_report_map_first(void *map_report, char *index_to_fill);
void *tag_report_map_next(void *map_report, char *index_to_fill);
void *tag_report_map_get(void *map_report, const char *index);
void *tag_report_map_add(void *map_report, const char *index, const void *report);
int tag_report_map_del(void *map_report, const char *index);
void tag_report_destroy(void *map_report);

