#include <sparsehash/src/sparsehash/dense_hash_map>
//#include <unordered_map>
#include <cstring>
#include "xxhash.h"

struct eqstr {
	bool operator()(const char *s1, const char *s2) const {
		return (s1 == s2) || (s1 && s2 && strcmp(s1, s2) == 0);
	}
};


struct xxhash {
	size_t operator()(const char* str) const {
		int str_len = strlen(str);
		return XXH64(str, str_len, 2001);
	}
};

typedef  google::dense_hash_map<const char*, const void*, xxhash, eqstr> dense_hash_t;
//typedef  std::unordered_map<const char*, const void*, xxhash, eqstr> dense_hash_t;

class pinba_map {
	public:
		dense_hash_t hash_map;
		~pinba_map() {};
		pinba_map() {
			hash_map.set_empty_key(NULL);
			hash_map.set_deleted_key("");
		}
		int data_add(const char *index, const void *report);
		int data_delete(const char *index);
		void *data_first(char *first_index);
		void *data_next(char *index);
		void *data_get(const char *index);
		int is_empty();
		size_t size();
		void clear();
};


int pinba_map::is_empty() {
	return hash_map.empty();
}

int pinba_map::data_add(const char *index, const void *report) /* {{{ */
{
	hash_map[strdup(index)] = report;
	return 0;
}
/* }}} */

int pinba_map::data_delete(const char *index) /* {{{ */
{
	dense_hash_t::iterator it = hash_map.find(index);
	if (it != hash_map.end()) {
		char *key = (char *)it->first;
		hash_map.erase(it);
		free(key);
	}
	return 0;
}
/* }}} */

void *pinba_map::data_first(char *index_to_fill) /* {{{ */
{
	dense_hash_t::iterator it = hash_map.begin();
	if (it == hash_map.end()) {
		return NULL;
	}
	strcpy(index_to_fill, it->first);
	return (void*)it->second;
}
/* }}} */

void *pinba_map::data_next(char *index_to_fill) /* {{{ */
{

	dense_hash_t::iterator it = hash_map.find(index_to_fill);
	if (it == hash_map.end()) {
		return NULL;
	}
	it++;

	if (it == hash_map.end()) {
		return NULL;
	}

	strcpy(index_to_fill, it->first);
	return (void*)it->second;
}
/* }}} */

void pinba_map::clear()  /* {{{ */
{
	dense_hash_t::iterator old_it, it = hash_map.begin();

	while (it != hash_map.end()) {
		char *key = (char *)it->first;

		old_it = it;
		it++;

		hash_map.erase(old_it);
		free(key);
	}
}
/* }}} */

void *pinba_map::data_get(const char *index) /* {{{ */
{
	dense_hash_t::iterator it = hash_map.find(index);
	if (it == hash_map.end()) {
		return NULL;
	}
	return (void*)it->second;
}
/* }}} */

size_t pinba_map::size() /* {{{ */
{
	return hash_map.size();
}
/* }}} */

void *pinba_map_first(void *map_report, char *index_to_fill) /* {{{ */
{
	if (!map_report) {
		return NULL;
	}

	pinba_map *map = static_cast<pinba_map*>(map_report);
	return map->data_first(index_to_fill);
}
/* }}} */

void *pinba_map_next(void *map_report, char *index_to_fill) /* {{{ */
{
	if (!map_report) {
		return NULL;
	}

	pinba_map *map = static_cast<pinba_map*>(map_report);

	return map->data_next(index_to_fill);
}
/* }}} */

void *pinba_map_get(void *map_report, const char *index) /* {{{ */
{
	if (!map_report) {
		return NULL;
	}
	pinba_map *map = static_cast<pinba_map*>(map_report);

	return map->data_get(index);
}
/* }}} */

void *pinba_map_add(void *map_report, const char *index, const void *data) /* {{{ */
{
	pinba_map *map;
	if (map_report == NULL) {
		map = new pinba_map();
	} else {
		map = static_cast<pinba_map*>(map_report);
	}
	map->data_add(index, data);

	return map;
}
/* }}} */

void *pinba_map_create() /* {{{ */
{
	return new pinba_map;
}
/* }}} */

int pinba_map_delete(void *map_report, const char *index) /* {{{ */
{
	if (!map_report) {
		return -1;
	}

	pinba_map *map  = static_cast<pinba_map*>(map_report);
	map->data_delete(index);

	if (map->is_empty()) {
		return -1;
	}

	return 0;
}
/* }}} */

void pinba_map_destroy(void *data) /* {{{ */
{
	if (!data) {
		return;
	}

	pinba_map *map  = static_cast<pinba_map*>(data);
	map->clear();
	delete map;
}
/* }}} */

size_t pinba_map_count(void *map_report) /* {{{ */
{
	if (!map_report) {
		return 0;
	}

	pinba_map *map = static_cast<pinba_map *>(map_report);
	return map->size();
}
/* }}} */

