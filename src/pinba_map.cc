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
		// TODO: free
		hash_map.erase(it);
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

void *pinba_map::data_get(const char *index) /* {{{ */
{
	dense_hash_t::iterator it = hash_map.find(index);
	if (it == hash_map.end()) {
		return NULL;
	}
	return (void*)it->second;
}
/* }}} */

void *pinba_map_first(void *map_report, char *index_to_fill) /* {{{ */
{
	if (!map_report) {
		return NULL;
	}

	pinba_map *tag_report = static_cast<pinba_map*>(map_report);
	return tag_report->data_first(index_to_fill);
}
/* }}} */

void *pinba_map_next(void *map_report, char *index_to_fill) /* {{{ */
{
	if (!map_report) {
		return NULL;
	}

	pinba_map *tag_report = static_cast<pinba_map*>(map_report);

	return tag_report->data_next(index_to_fill);
}
/* }}} */

void *pinba_map_get(void *map_report, const char *index) /* {{{ */
{
	if (!map_report) {
		return NULL;
	}
	pinba_map *tag_report = static_cast<pinba_map*>(map_report);

	return tag_report->data_get(index);
}
/* }}} */

void *pinba_map_add(void *map_report, const char *index, const void *report) /* {{{ */
{
	pinba_map *tag_report;
	if (map_report == NULL) {
		tag_report =  new pinba_map();
	} else {
		tag_report = static_cast<pinba_map*>(map_report);
	}
	tag_report->data_add(index, report);

	return tag_report;
}
/* }}} */

void *pinba_map_create() /* {{{ */
{
	return new pinba_map;
}
/* }}} */

int pinba_map_del(void *map_report, const char *index) /* {{{ */
{
	if (map_report == NULL) {
		return -1;
	}

	pinba_map *tag_report  = static_cast<pinba_map*>(map_report);
	tag_report->data_delete(index);

	if (tag_report->is_empty()) {
		return -1;
	}

	return 0;
}
/* }}} */

void pinba_map_destroy(void *data) /* {{{ */
{
	if (data) {
		pinba_map *map  = static_cast<pinba_map*>(data);
		delete map;
	}
}
/* }}} */

