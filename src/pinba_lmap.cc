#include <sparsehash/src/sparsehash/sparse_hash_map>

typedef  google::sparse_hash_map<uint64_t, const void*> sparse_hash_t;

class pinba_lmap {
	public:
		sparse_hash_t hash_map;
		~pinba_lmap() {};
		pinba_lmap() {
			hash_map.set_deleted_key((uint64_t)-1);
		}
		int data_add(uint64_t index, const void *report);
		int data_delete(uint64_t index);
		void *data_first(uint64_t *first_index);
		void *data_next(uint64_t *next_index);
		void *data_get(uint64_t index);
		int is_empty();
		size_t size();
};


int pinba_lmap::is_empty() {
	return hash_map.empty();
}

int pinba_lmap::data_add(uint64_t index, const void *report) /* {{{ */
{
	hash_map[index] = report;
	return 0;
}
/* }}} */

int pinba_lmap::data_delete(uint64_t index) /* {{{ */
{
	sparse_hash_t::iterator it = hash_map.find(index);
	if (it != hash_map.end()) {
		hash_map.erase(it);
	}
	return 0;
}
/* }}} */

void *pinba_lmap::data_first(uint64_t *first_index) /* {{{ */
{
	sparse_hash_t::iterator it = hash_map.begin();
	if (it == hash_map.end()) {
		return NULL;
	}
	*first_index = it->first;
	return (void*)it->second;
}
/* }}} */

void *pinba_lmap::data_next(uint64_t *next_index) /* {{{ */
{

	sparse_hash_t::iterator it = hash_map.find(*next_index);
	if (it == hash_map.end()) {
		return NULL;
	}
	it++;

	if (it == hash_map.end()) {
		return NULL;
	}

	*next_index = it->first;
	return (void*)it->second;
}
/* }}} */

void *pinba_lmap::data_get(uint64_t index) /* {{{ */
{
	sparse_hash_t::iterator it = hash_map.find(index);
	if (it == hash_map.end()) {
		return NULL;
	}
	return (void*)it->second;
}
/* }}} */

size_t pinba_lmap::size() /* {{{ */
{
	return hash_map.size();
}
/* }}} */

void *pinba_lmap_first(void *map_report, uint64_t *index_to_fill) /* {{{ */
{
	if (!map_report) {
		return NULL;
	}

	pinba_lmap *map = static_cast<pinba_lmap*>(map_report);
	return map->data_first(index_to_fill);
}
/* }}} */

void *pinba_lmap_next(void *map_report, uint64_t *index_to_fill) /* {{{ */
{
	if (!map_report) {
		return NULL;
	}

	pinba_lmap *map = static_cast<pinba_lmap*>(map_report);

	return map->data_next(index_to_fill);
}
/* }}} */

void *pinba_lmap_get(void *map_report, uint64_t index) /* {{{ */
{
	if (!map_report) {
		return NULL;
	}
	pinba_lmap *map = static_cast<pinba_lmap*>(map_report);

	return map->data_get(index);
}
/* }}} */

void *pinba_lmap_add(void *map_report, uint64_t index, const void *data) /* {{{ */
{
	pinba_lmap *map;
	if (map_report == NULL) {
		map = new pinba_lmap();
	} else {
		map = static_cast<pinba_lmap*>(map_report);
	}
	map->data_add(index, data);

	return map;
}
/* }}} */

void *pinba_lmap_create() /* {{{ */
{
	return new pinba_lmap;
}
/* }}} */

int pinba_lmap_delete(void *map_report, uint64_t index) /* {{{ */
{
	if (map_report == NULL) {
		return -1;
	}

	pinba_lmap *map  = static_cast<pinba_lmap*>(map_report);
	map->data_delete(index);

	if (map->is_empty()) {
		return -1;
	}

	return 0;
}
/* }}} */

void pinba_lmap_destroy(void *data) /* {{{ */
{
	if (data) {
		pinba_lmap *map  = static_cast<pinba_lmap*>(data);
		delete map;
	}
}
/* }}} */

size_t pinba_lmap_count(void *map_report) /* {{{ */
{
	pinba_lmap *map = static_cast<pinba_lmap *>(map_report);
	return map->size(); 
}
/* }}} */
