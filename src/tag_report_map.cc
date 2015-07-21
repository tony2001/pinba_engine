#include <sparsehash/dense_hash_map>
//#include <unordered_map>
#include <cstring>

struct eqstr {
	bool operator()(const char *s1, const char *s2) const {
		return (s1 == s2) || (s1 && s2 && strcmp(s1, s2) == 0);
	}
};


struct chash {
	size_t operator()(const char* str) const {
		unsigned long hash = 5381;
		int c;

		while ((c = *str++) != 0)
			hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

		return hash;
	}
};

typedef  google::dense_hash_map<const char*, const void*, chash, eqstr> dense_hash_t;
//typedef  std::unordered_map<const char*, const void*, chash, eqstr> dense_hash_t;
class tag_report_map {
public:
	dense_hash_t hash_map;
	~tag_report_map() {};
	tag_report_map() {
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


int tag_report_map::is_empty() {
	return hash_map.empty();
}

int tag_report_map::data_add(const char *index, const void *report) {
	hash_map[strdup(index)] = report;
	return 0;
}

int tag_report_map::data_delete(const char *index) {
	dense_hash_t::iterator it = hash_map.find(index);
	if (it != hash_map.end()) {
		// TODO: free
		hash_map.erase(it);
	}
	return 0;
}

void *tag_report_map::data_first(char *index_to_fill) {
	dense_hash_t::iterator it = hash_map.begin();
	if (it == hash_map.end()) {
		return NULL;
	}
	strcpy(index_to_fill, it->first);
	return (void*)it->second;
}

void *tag_report_map::data_next(char *index_to_fill) {

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

void *tag_report_map::data_get(const char *index) {
	dense_hash_t::iterator it = hash_map.find(index);
	if (it == hash_map.end()) {
		return NULL;
	}
	return (void*)it->second;
}

void *tag_report_map_first(void *map_report, char *index_to_fill) {
	if (!map_report) {
		return NULL;
	}

	tag_report_map *tag_report = static_cast<tag_report_map*>(map_report);
	return tag_report->data_first(index_to_fill);
}

void *tag_report_map_next(void *map_report, char *index_to_fill) {
	if (!map_report) {
		return NULL;
	}

	tag_report_map *tag_report = static_cast<tag_report_map*>(map_report);

	return tag_report->data_next(index_to_fill);
}

void *tag_report_map_get(void *map_report, const char *index) {
	if (!map_report) {
		return NULL;
	}
	tag_report_map *tag_report = static_cast<tag_report_map*>(map_report);

	return tag_report->data_get(index);
}

void *tag_report_map_add(void *map_report, const char *index, const void *report)
{
	tag_report_map *tag_report;
	if (map_report == NULL) {
		tag_report =  new tag_report_map();
	} else {
		tag_report = static_cast<tag_report_map*>(map_report);
	}
	tag_report->data_add(index, report);

	return tag_report;
}

void *tag_report_map_create() {
}

int tag_report_map_del(void *map_report, const char *index)
{
	if (map_report == NULL) {
		return -1;
	}

	tag_report_map *tag_report  = static_cast<tag_report_map*>(map_report);
	tag_report->data_delete(index);

	if (tag_report->is_empty()) {
		return -1;
	}

	return 0;
}

void tag_report_destroy(void *map_report) {
	if (map_report) {
		tag_report_map *tag_report  = static_cast<tag_report_map*>(map_report);
		delete tag_report;
	}
}

#if 0
int main()
{
	tag_report_map report_map;


	tag_report_map_add(&report_map, "lognname|sttest1", reinterpret_cast<const void*>("t1"));
	tag_report_map_add(&report_map, "lognname|sest2", reinterpret_cast<const void*>("t2"));
	tag_report_map_add(&report_map, "test3", reinterpret_cast<const void*>("t3"));
	tag_report_map_add(&report_map, "test4", reinterpret_cast<const void*>("t4"));
	tag_report_map_add(&report_map, "test5", reinterpret_cast<const void*>("t5"));
	tag_report_map_add(&report_map, "test6", reinterpret_cast<const void*>("t6"));
	tag_report_map_del(&report_map, "test3");


	char index[100] = { 0 };
	for (void *data = tag_report_map_first(&report_map, index); data; data = tag_report_map_next(&report_map, index)) {
		printf("index = %s, value = %s\n", index, (char*)data);
	}

	return 0;
}
#endif
