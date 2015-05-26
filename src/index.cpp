#include <iostream>
#include <iterator>
#include <string>
#include <vector>

#include <stdint.h>

#include <elliptics/session.hpp>

#include <msgpack.hpp>

#include <boost/program_options.hpp>

#include <ribosome/timer.hpp>

static inline std::string lexical_cast(size_t value) {
	if (value == 0) {
		return std::string("0");
	}

	std::string result;
	size_t length = 0;
	size_t calculated = value;
	while (calculated) {
		calculated /= 10;
		++length;
	}

	result.resize(length);
	while (value) {
		--length;
		result[length] = '0' + (value % 10);
		value /= 10;
	}

	return result;
}

namespace ioremap { namespace indexes {
static size_t max_page_size = 4096;

#define dprintf(fmt, a...) do {} while (0)
//#define dprintf(fmt, a...) printf(fmt, ##a)


struct key {
	std::string id;
	std::string value;

	size_t size() const {
		return id.size() + value.size();
	}

	bool operator<(const key &other) const {
		return id < other.id;
	}
	bool operator<=(const key &other) const {
		return id <= other.id;
	}
	bool operator==(const key &other) const {
		return id == other.id;
	}
	bool operator!=(const key &other) const {
		return id != other.id;
	}

	operator bool() const {
		return id.size() != 0;
	}
	bool operator !() const {
		return !operator bool();
	}

	std::string str() const {
		return id + ":" + value;
	}

	MSGPACK_DEFINE(id, value);
};

#define PAGE_LEAF		(1<<0)

struct page {
	uint32_t flags = 0;
	std::vector<key> objects;
	size_t total_size = 0;
	std::string next;

	MSGPACK_DEFINE(flags, objects, total_size, next);

	page(bool leaf = false) {
		if (leaf) {
			flags = PAGE_LEAF;
		}
	}

	bool is_empty() const {
		return objects.size() == 0;
	}

	bool operator==(const page &other) const {
		return ((flags == other.flags) && (objects == other.objects));
	}
	bool operator!=(const page &other) const {
		return ((flags != other.flags) || (objects != other.objects));
	}

	std::string str() const {
		std::ostringstream ss;
		if (objects.size() > 0) {
			ss << "[" << objects.front().str() <<
				", " << objects.back().str() <<
				", L" << (is_leaf() ? 1 : 0) <<
				", N" << objects.size() <<
				", T" << total_size <<
				")";
		} else {
			ss << "[" << 
				"L" << (is_leaf() ? 1 : 0) <<
				", N" << objects.size() <<
				", T" << total_size <<
				")";
		}
		return ss.str();
	}

	bool is_leaf() const {
		return flags & PAGE_LEAF;
	}

	void load(const char *data, size_t size) {
		objects.clear();
		flags = 0;
		next.clear();
		total_size = 0;

		msgpack::unpacked result;

		msgpack::unpack(&result, data, size);
		msgpack::object obj = result.get();
		obj.convert(this);

		dprintf("page load: %s\n", str().c_str());
	}

	std::string save() const {
		std::stringstream ss;
		msgpack::pack(ss, *this);

		dprintf("page save: %s\n", str().c_str());

		return ss.str();
	}

	// return position of the given key in @objects vector
	int search_leaf(const key &obj) const {
		if (!is_leaf()) {
			return -1;
		}

		int pos = 0;
		for (auto it = objects.begin(); it != objects.end(); ++it) {
			if (obj.id == it->id) {
				return pos;
			}

			++pos;
		}

		return -1;
	}

	// returns position of the key in @objects vector
	int search_node(const key &obj) const {
		if (objects.size() == 0)
			return -1;

		if (is_leaf()) {
			return search_leaf(obj);
		}

		key prev = objects.front();
		int prev_position = -1;

		for (auto it = objects.begin(); it != objects.end(); ++it) {
			if (obj.id < it->id) {
				if (prev_position < 0)
					return 0;

				return prev_position;
			}

			prev = *it;
			++prev_position;
		}

		return objects.size() - 1;
	}

	bool insert_and_split(const key &obj, page &other) {
		std::vector<key> copy;
		bool copied = false;

		for (auto it = objects.begin(); it != objects.end(); ++it) {
			if (obj <= *it) {
				copy.push_back(obj);
				total_size += obj.size();
				copied = true;

				if (obj == *it) {
					total_size -= it->size();
					++it;
				}

				copy.insert(copy.end(), it, objects.end());
				break;
			}

			copy.push_back(*it);
		}

		if (!copied) {
			copy.push_back(obj);
			total_size += obj.size();
		}

		if (total_size > max_page_size) {
			size_t split_idx = copy.size() / 2;

			other.flags = flags;
			other.objects = std::vector<key>(std::make_move_iterator(copy.begin() + split_idx),
							 std::make_move_iterator(copy.end()));
			other.recalculate_size();

			copy.erase(copy.begin() + split_idx, copy.end());
			objects.swap(copy);
			recalculate_size();

			dprintf("insert/split: %s: split: %s %s\n", obj.str().c_str(), str().c_str(), other.str().c_str());

			return true;
		}

		objects.swap(copy);

		dprintf("insert/split: %s: %s\n", obj.str().c_str(), str().c_str());
		return false;
	}

	void recalculate_size() {
		total_size = 0;
		for_each(objects.begin(), objects.end(), [&] (const key &obj)
				{
					total_size += obj.size();
				});
	}
};

template <typename T>
class iterator {
public:
	typedef iterator self_type;
	typedef key value_type;
	typedef key& reference;
	typedef key* pointer;
	typedef std::forward_iterator_tag iterator_category;
	typedef std::ptrdiff_t difference_type;

	iterator(T &t, page &p, size_t internal_index) : m_t(t), m_page(p), m_page_internal_index(internal_index) {}
	iterator(const iterator &i) : m_t(i.m_t) {
		m_page = i.m_page;
		m_page_internal_index = i.m_page_internal_index;
		m_page_index = i.m_page_index;
	}

	self_type operator++() {
		++m_page_internal_index;
		try_loading_next_page();

		return *this;
	}

	self_type operator++(int num) {
		m_page_internal_index += num;
		try_loading_next_page();

		return *this;
	}

	reference operator*() {
		return m_page.objects[m_page_internal_index];
	}
	pointer operator->() {
		return &m_page.objects[m_page_internal_index];
	}

	bool operator==(const self_type& rhs) {
		return m_page == rhs.m_page;
	}
	bool operator!=(const self_type& rhs) {
		return m_page != rhs.m_page;
	}
private:
	T &m_t;
	page m_page;
	size_t m_page_index = 0;
	size_t m_page_internal_index = 0;

	void try_loading_next_page() {
		if (m_page_internal_index >= m_page.objects.size()) {
			m_page_internal_index = 0;
			++m_page_index;

			if (m_page.next.empty()) {
				m_page = page();
			} else {
				std::string p = m_t.read(m_page.next);
				m_page.load(p.data(), p.size());
			}
		}
	}
};

struct index_meta {
	int page_index = 0;
	int num_pages = 0;
	uint64_t num_leaf_pages = 0;

	MSGPACK_DEFINE(page_index, num_pages, num_leaf_pages);

	bool operator != (const index_meta &other) const {
		return ((page_index != other.page_index) || (num_pages != other.num_pages) || (num_leaf_pages != other.num_leaf_pages));
	}
};

struct recursion {
	key page_start;
	key split_key;
};

template <typename T>
class index {
public:
	index(T &t, const std::string &sk): m_t(t), m_sk(sk) {
		try {
			std::string meta = m_t.read(meta_key());

			msgpack::unpacked result;
			msgpack::unpack(&result, meta.data(), meta.size());
			msgpack::object obj = result.get();
			m_meta = obj.as<index_meta>();
		} catch (const std::exception &e) {
			fprintf(stderr, "index: could not read index metadata: %s\n", e.what());

			start_page_init();
			meta_write();
		}

		dprintf("index: opened: page_index: %d\n", m_meta.page_index);
	}

	~index() {
		meta_write();
	}

	key search(const key &obj) const {
		auto found = search(m_sk, obj);
		if (found.second < 0)
			return key();

		return found.first.objects[found.second];
	}

	void insert(const key &obj) {
		index_meta old = m_meta;
		recursion tmp;
		insert(m_sk, obj, tmp);

		if (m_meta != old) {
			meta_write();
		}
	}

	iterator<T> begin(const std::string &k) const {
		key zero;
		zero.id = k;

		auto found = search(m_sk, zero);
		if (found.second < 0)
			found.second = 0;

		return iterator<T>(m_t, found.first, found.second);
	}

	iterator<T> begin() const {
		return begin(std::string("\0"));
	}

	iterator<T> end() const {
		page p;
		return iterator<T>(m_t, p, 0);
	}

	std::vector<key> keys(const std::string &start) const {
		std::vector<key> ret;
		for (auto it = begin(start), e = end(); it != e; ++it) {
			ret.push_back(*it);
		}

		return ret;
	}

	std::vector<key> keys() const {
		std::vector<key> ret;
		for (auto it = begin(), e = end(); it != e; ++it) {
			ret.push_back(*it);
		}

		return ret;
	}

private:
	T &m_t;
	std::string m_sk;

	index_meta m_meta;

	std::string meta_key() const {
		return m_sk + ".meta";
	}

	void meta_write() {
		std::stringstream ss;
		msgpack::pack(ss, m_meta);
		m_t.write(meta_key(), ss.str(), true);
	}

	void start_page_init() {
		page start_page;

		try {
			std::string start = m_t.read(m_sk);
		} catch (const std::exception &e) {
			fprintf(stderr, "index: could not read start key: %s\n", e.what());

			m_t.write(m_sk, start_page.save());
		}
	}

	std::pair<page, int> search(const std::string &page_key, const key &obj) const {
		std::string data = m_t.read(page_key);

		page p;
		p.load(data.data(), data.size());

		int found_pos = p.search_node(obj);
		if (found_pos < 0) {
			dprintf("search: %s: page: %s -> %s, found_pos: %d\n",
				obj.str().c_str(),
				page_key.c_str(), p.str().c_str(),
				found_pos);

			return std::make_pair(p, found_pos);
		}

		dprintf("search: %s: page: %s -> %s, found_pos: %d, found_key: %s\n",
			obj.str().c_str(),
			page_key.c_str(), p.str().c_str(),
			found_pos, p.objects[found_pos].str().c_str());

		if (p.is_leaf())
			return std::make_pair(p, found_pos);

		return search(p.objects[found_pos].value, obj);
	}

	// returns true if page at @page_key has been split after insertion
	// key used to store split part has been saved into @obj.value
	void insert(const std::string &page_key, const key &obj, recursion &rec) {
		std::string data = m_t.read(page_key);

		page p;
		p.load(data.data(), data.size());

		page split;

		dprintf("insert: %s: page: %s -> %s\n", obj.str().c_str(), page_key.c_str(), p.str().c_str());

		key tmp_start_key = obj;
		if (!p.is_empty())
			tmp_start_key = p.objects.front();


		if (!p.is_leaf()) {
			int found_pos = p.search_node(obj);
			if (found_pos < 0) {
				dprintf("insert: %s: page: %s -> %s, found_pos: %d\n",
					obj.str().c_str(),
					page_key.c_str(), p.str().c_str(),
					found_pos);

				// this is not a leaf node, but there is no leaf in @objects
				// this is the only reason non-leaf page search failed,
				// thus create new leaf node
				//
				// this path can only be taken once - when new empty index has been created
				key leaf_key;
				leaf_key.id = obj.id;
				leaf_key.value = generate_page_key();

				page leaf(true), unused_split;
				leaf.insert_and_split(obj, unused_split);
				m_t.write(leaf_key.value, leaf.save());

				// no need to perform recursion unwind, since there were no entry for this new leaf
				// which can only happen when page was originally empty
				p.insert_and_split(leaf_key, unused_split);
				m_t.write(page_key, p.save());

				dprintf("insert: %s: page: %s -> %s, leaf: %s -> %s\n",
						obj.str().c_str(),
						page_key.c_str(), p.str().c_str(),
						leaf_key.str().c_str(), leaf.str().c_str());

				m_meta.num_pages++;
				m_meta.num_leaf_pages++;
				return;
			}

			key &found = p.objects[found_pos];

			dprintf("insert: %s: page: %s -> %s, found_pos: %d, found_key: %s\n",
				obj.str().c_str(),
				page_key.c_str(), p.str().c_str(),
				found_pos, found.str().c_str());

			insert(found.value, obj, rec);

			dprintf("insert: %s: returned: %s -> %s, found_pos: %d, found_key: %s, rec: page_start: %s, split_key: %s\n",
					obj.str().c_str(),
					page_key.c_str(), p.str().c_str(),
					found_pos, found.str().c_str(),
					rec.page_start.str().c_str(),
					rec.split_key.str().c_str());

			// true if we should not unwind recursion and just return
			// false if either split page has to be written or page changed and has to be written
			bool want_return = true;

			if (found != rec.page_start) {
				dprintf("p: %s: replace: key: %s: id: %s -> %s\n",
						p.str().c_str(), found.str().c_str(), found.id.c_st(), rec.page_start.c_str());
				p.objects[found_pos].id = rec.page_start;

				// page has changed, it must be written into storage
				want_return = false;
			}

			if (rec.split_key) {
				p.insert_and_split(rec.split_key, split);

				// there is a split page, it was already written into the storage,
				// now its time to insert it into parent and upate parent
				want_return = false;
			}

			if (want_return) {
				rec.page_start = p.objects.front();
				rec.split_key = key();
				return;
			}
		} else {
			p.insert_and_split(obj, split);
		}

		rec.page_start = p.objects.front();
		rec.split_key = key();

		if (!split.is_empty()) {
			// generate key for split page
			rec.split_key.value = generate_page_key();
			rec.split_key.id = split.objects.front().id;

			split.next = p.next;
			p.next = rec.split_key.value;

			dprintf("insert: %s: write split page: %s -> %s, split: key: %s -> %s\n",
					obj.str().c_str(),
					page_key.c_str(), p.str().c_str(),
					rec.split_key.str().c_str(), split.str().c_str());
			m_t.write(rec.split_key.value, split.save());

			m_meta.num_pages++;
			if (p.is_leaf())
				m_meta.num_leaf_pages++;
		}

		if (!split.is_empty() && page_key == m_sk) {
			// if we split root page, put old root data into new key
			// root must always be accessible via start key
			// generate new root, which will host data for 2 new pages:
			// split and old root

			key old_root_key;
			old_root_key.value = generate_page_key();
			old_root_key.id = p.objects.front().id;

			m_t.write(old_root_key.value, p.save());

			// we have written split page and old root page above
			// now its time to create and write the new root
			page new_root, unused_split;
			new_root.insert_and_split(old_root_key, unused_split);
			new_root.insert_and_split(rec.split_key, unused_split);

			m_t.write(m_sk, new_root.save());

			m_meta.num_pages++;

			dprintf("insert: %s: write split page: %s -> %s, old_root_key: %s, new_root: %s\n",
					obj.str().c_str(),
					page_key.c_str(), p.str().c_str(),
					old_root_key.str().c_str(), new_root.str().c_str());
		} else {
			dprintf("insert: %s: write main page: %s -> %s\n", obj.str().c_str(), page_key.c_str(), p.str().c_str());
			m_t.write(page_key, p.save(), true);
		}

		return;
	}

	std::string generate_page_key() {
		char buf[128 + m_sk.size()];

		snprintf(buf, sizeof(buf), "%s.%d", m_sk.c_str(), m_meta.page_index);
		dprintf("generated key: %s\n", buf);
		m_meta.page_index++;
		return std::string(buf);
	}
};

namespace intersect {
struct result {
	bool completed = false;
	std::map<std::string, std::vector<key>> keys;
};

template <typename T>
class intersector {
public:
	intersector(T &t) : m_t(t) {}
	result intersect(const std::vector<std::string> &indexes) const {
		std::string start = std::string("\0");
		return intersect(indexes, start, INT_MAX);
	}

	// search for intersections between all @indexes
	// starting with the key @start, returning at most @num entries
	//
	// after @intersect() completes, it sets @start to the next key to start searching from
	// user should not change that token, otherwise @intersect() may skip some entries or
	// return duplicates.
	//
	// if number of returned entries is less than requested number @num or if @start has been set to empty string
	// after call to this function returns, then intersection is completed.
	//
	// @result.completed will be set to true in this case.
	result intersect(const std::vector<std::string> &indexes, std::string &start, size_t num) const {
		struct iter {
			index<T> idx;
			indexes::iterator<T> begin, end;

			iter(T &t, const std::string &name, const std::string &start) :
				idx(t, name), begin(idx.begin(start)), end(idx.end()) {}
		};
		std::vector<iter> idata;

		for_each(indexes.begin(), indexes.end(), [&] (const std::string &name) {
				iter it(m_t, name, start);

				idata.emplace_back(it);
			});

		result res;

		while (!res.completed) {
			std::vector<int> pos;

			int current = -1;
			for (auto idata_it = idata.begin(); idata_it != idata.end(); ++idata_it) {
				auto &it = idata_it->begin;
				auto &e = idata_it->end;
				++current;

				if (it == e) {
					res.completed = true;
					break;
				}

				if (pos.size() == 0) {
					pos.push_back(current);
					continue;
				}

				auto &min_it = idata[pos[0]].begin;

				if (*it == *min_it) {
					pos.push_back(current);
					continue;
				}

				if (*it < *min_it) {
					pos.clear();
					pos.push_back(current);
				}
			}

			if (res.completed) {
				start = "";
				break;
			}

			if (pos.size() != idata.size()) {
				for (auto it = pos.begin(); it != pos.end(); ++it) {
					auto &min_it = idata[*it].begin;
					++min_it;
				}

				continue;
			}

			start = idata[pos[0]].begin->id;
			if (res.keys.size() == num) {
				break;
			}

			auto find_it = res.keys.find(start);

			for (auto it = pos.begin(); it != pos.end(); ++it) {
				auto &min_it = idata[*it].begin;

				key k = *min_it;
				k.id = indexes[*it];

				if (find_it == res.keys.end()) {
					std::vector<key> kk;
					kk.emplace_back(k);
					auto pair = res.keys.insert(std::make_pair(start, kk));
					find_it = pair.first;
				} else {
					find_it->second.emplace_back(k);
				}

				++min_it;
			}
		}

		return res;
	}
private:
	T &m_t;
};

} // ioremap::indexes::intersect

}} // ioremap::indexes

using namespace ioremap;

class elliptics_transport {
public:
	elliptics_transport(const std::string &log_file, const std::string &log_level):
	m_log(log_file.c_str(), elliptics::file_logger::parse_level(log_level)),
	m_node(elliptics::logger(m_log, blackhole::log::attributes_t())) {
	}

	void add_remotes(const std::vector<std::string> &remotes) {
		std::vector<elliptics::address> a(remotes.begin(), remotes.end());
		m_node.add_remote(a);
	}

	void set_namespace(const std::string &ns) {
		m_ns = ns;
	}

	void set_groups(const std::vector<int> &groups) {
		m_groups = groups;
	}

	std::string read(const std::string &key) {
		dprintf("elliptics read: key: %s\n", key.c_str());
		elliptics::session s = session(true);
		elliptics::read_result_entry e = s.read_data(key, 0, 0).get_one();
		dprintf("elliptics read: key: %s, data-size: %zd\n", key.c_str(), e.file().size());
		return e.file().to_string();
	}

	void write_prepare_commit(const std::string &key, const char *data, size_t size) {
		dprintf("elliptics write: key: %s, data-size: %zd\n", key.c_str(), size);
		elliptics::data_pointer dp = elliptics::data_pointer::from_raw((char *)data, size);

		elliptics::session s = session(false);

		elliptics::key id(key);
		s.transform(id);

		dnet_io_control ctl;

		memset(&ctl, 0, sizeof(ctl));
		dnet_current_time(&ctl.io.timestamp);

		ctl.cflags = s.get_cflags();
		ctl.data = dp.data();

		ctl.io.flags = s.get_ioflags() | DNET_IO_FLAGS_PREPARE | DNET_IO_FLAGS_PLAIN_WRITE | DNET_IO_FLAGS_COMMIT;
		ctl.io.user_flags = s.get_user_flags();
		ctl.io.offset = 0;
		ctl.io.size = dp.size();
		ctl.io.num = indexes::max_page_size * 1.5;
		if (ctl.io.size > ctl.io.num) {
			ctl.io.num = ctl.io.size * 2;
		}

		memcpy(&ctl.id, &id.id(), sizeof(ctl.id));

		ctl.fd = -1;

		elliptics::write_result_entry e = s.write_data(ctl).get_one();
	}

	void write(const std::string &key, const char *data, size_t size, bool cache = false) {
		dprintf("elliptics write: key: %s, data-size: %zd\n", key.c_str(), size);
		elliptics::data_pointer dp = elliptics::data_pointer::from_raw((char *)data, size);
		elliptics::session s = session(cache);
		elliptics::write_result_entry e = s.write_data(key, dp, 0).get_one();
	}

	void write(const std::string &key, const std::string &data, bool cache = false) {
		if (cache)
			return write(key, data.c_str(), data.size(), cache);
		else
			return write_prepare_commit(key, data.c_str(), data.size());
	}

private:
	elliptics::file_logger m_log;
	elliptics::node m_node;
	std::string m_ns;
	std::vector<int> m_groups;

	elliptics::session session(bool cache) {
		elliptics::session s(m_node);
		s.set_namespace(m_ns);
		s.set_groups(m_groups);
		s.set_timeout(60);
		if (cache)
			s.set_ioflags(DNET_IO_FLAGS_CACHE);

		return s;
	}
};

template <typename T>
class test {
#define __stringify_1(x...)     (const char *)#x
#define __stringify(x...)       __stringify_1(x)
#define func(name, args...) __stringify(name), name, ##args
public:
	test(T &t) {
		std::string idx_name0 = "test" + lexical_cast(rand());
		indexes::index<T> idx(t, idx_name0);

		std::vector<indexes::key> keys;
		//test::run(this, func(&test::test_insert_many_keys, idx, keys, 1000000));
		//test::run(this, func(&test::test_iterator_number, idx, keys));
		//test::run(this, func(&test::test_select_many_keys, idx, keys));
		test::run(this, func(&test::test_intersection, t, 3, 5000, 10000));
	}

private:


	template <typename Class, typename Method, typename... Args>
	static inline void run(Class *obj, const char *str, Method method, Args &&...args) {
		try {
			ribosome::timer tm;
			(obj->*method)(std::forward<Args>(args)...);
			printf("%s: %zd ms\n", str, tm.elapsed());
		} catch (const std::exception &e) {
			fprintf(stderr, "%s: failed: %s\n", str, e.what());
			exit(-1);
		}
	}

	void test_insert_many_keys(indexes::index<T> &idx, std::vector<indexes::key> &keys, int max) {
		for (int i = 0; i < max; ++i) {
			indexes::key k;

			char buf[128];

			snprintf(buf, sizeof(buf), "%08x.%08d", rand(), i);
			k.id = std::string(buf);

			snprintf(buf, sizeof(buf), "value.%08d", i);
			k.value = std::string(buf);

			dprintf("inserting: %s\n", k.str().c_str());
			idx.insert(k);
			keys.push_back(k);
			dprintf("inserted: %s\n\n", k.str().c_str());
		}
	}

	void test_select_many_keys(indexes::index<T> &idx, std::vector<indexes::key> &keys) {
		for (auto it = keys.begin(); it != keys.end(); ++it) {
			indexes::key k;

			k.id = it->id;
			indexes::key found = idx.search(k);
			if (!found) {
				std::ostringstream ss;
				ss << "search failed: could not find key: " << it->id.c_str();
				throw std::runtime_error(ss.str());
			}

			if (found.id != it->id) {
				std::ostringstream ss;
				ss << "search failed: ID mismatch: found: " << found << ", must be: " << *it;
				throw std::runtime_error(ss.str());
			}
			if (found.value != it->value) {
				std::ostringstream ss;
				ss << "search failed: value mismatch: found: " << found << ", must be: " << *it;
				throw std::runtime_error(ss.str());
			}

			dprintf("search: key: %s, value: %s\n\n", found.id.c_str(), found.value.c_str());
		}
	}

	void test_iterator_number(indexes::index<T> &idx, std::vector<indexes::key> &keys) {
		size_t num = 0;
		for (auto it = idx.begin(), end = idx.end(); it != end; ++it) {
			dprintf("iterator: %s\n", it->str().c_str());
			num++;
		}

		if (num != keys.size()) {
			std::ostringstream ss;
			ss << "iterated numbed mismatch: keys: " << keys.size() << ", iterated: " << num;
			throw std::runtime_error(ss.str());
		}
	}

	void test_intersection(T &t, int num_indexes, size_t same_num, size_t different_num) {
		std::vector<std::string> indexes;
		std::vector<indexes::key> same;

		for (size_t i = 0; i < same_num; ++i) {
			indexes::key k;
			k.id = lexical_cast(rand()) + ".url-same-key." + lexical_cast(i);
			k.value = "url-same-value." + lexical_cast(i);

			same.emplace_back(k);
		}

		for (int i = 0; i < num_indexes; ++i) {
			std::string name = "intersection-index.rand." + lexical_cast(i) + "." + lexical_cast(rand());

			indexes.emplace_back(name);
			indexes::index<T> idx(t, name);

			for (size_t j = 0; j < different_num; ++j) {
				indexes::key k;

				k.id = lexical_cast(rand()) + ".url-random-key." + lexical_cast(i);
				k.value = "url-random-value." + lexical_cast(i);

				idx.insert(k);
			}

			for (auto it = same.begin(); it != same.end(); ++it) {
				idx.insert(*it);
			}
		}

		ribosome::timer tm;
		indexes::intersect::intersector<T> inter(t);
		indexes::intersect::result res = inter.intersect(indexes);
		for (auto it = res.keys.begin(); it != res.keys.end(); ++it) {
			dprintf("index: %s\n", it->first.c_str());
			for (auto k = it->second.begin(); k != it->second.end(); ++k) {
				dprintf("  %s\n", k->str().c_str());
			}
		}

		printf("intersection: indexes: %d, found keys: %zd, must be: %zd, total keys in each index: %zd, time: %ld ms\n",
				num_indexes, res.keys.size(), same_num, same_num + different_num, tm.restart());
		if (res.keys.size() != same_num) {
			std::ostringstream ss;
			ss << "intersection failed: indexes: " << num_indexes << ", same keys in each index: " << same_num <<
				", found keys: " << res.keys.size() <<
				", total keys in each index: " << different_num + same_num;
			throw std::runtime_error(ss.str());
		}

		indexes::intersect::intersector<T> p(t);
		std::string start("\0");
		size_t num = 100;
		size_t num_found = 0;

		while (true) {
			indexes::intersect::result res = p.intersect(indexes, start, num);

			num_found += res.keys.size();

			for (auto it = res.keys.begin(); it != res.keys.end(); ++it) {
				dprintf("index: %s, keys: %zd, total keys found: %zd\n", it->first.c_str(), res.keys.size(), num_found);
				for (auto k = it->second.begin(); k != it->second.end(); ++k) {
					dprintf("  %s\n", k->str().c_str());
				}
			}

			if (res.keys.size() < num)
				break;

			if (res.completed)
				break;
		}

		printf("paginated intersection: indexes: %d, found keys: %zd, must be: %zd, total keys in each index: %zd, time: %ld ms\n",
				num_indexes, num_found, same_num, same_num + different_num, tm.restart());
		if (num_found != same_num) {
			std::ostringstream ss;
			ss << "paginated intersection failed: indexes: " << num_indexes << ", same keys in each index: " << same_num <<
				", found keys: " << num_found <<
				", total keys in each index: " << different_num + same_num;
			throw std::runtime_error(ss.str());
		}
	}
};

int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	std::vector<std::string> remotes;


	bpo::options_description generic("Index test options");
	generic.add_options()
		("help", "This help message")
		;

	std::string ns, sk, log_file, log_level, groups;
	bpo::options_description ell("Elliptics options");
	ell.add_options()
		("remote", bpo::value<std::vector<std::string>>(&remotes)->required()->composing(), "remote node: addr:port:family")
		("log-file", bpo::value<std::string>(&log_file)->default_value("/dev/stdout"), "log file")
		("log-level", bpo::value<std::string>(&log_level)->default_value("error"), "log level: error, info, notice, debug")
		("groups", bpo::value<std::string>(&groups)->required(), "groups where index tree is stored: 1:2:3")
		("namespace", bpo::value<std::string>(&ns)->default_value(""), "Namespace where index tree is stored")
		("start-key", bpo::value<std::string>(&sk)->default_value("start-key"), "Key where root of the index tree is stored")
		;

	bpo::options_description cmdline_options;
	cmdline_options.add(generic).add(ell);

	bpo::variables_map vm;

	try {
		bpo::store(bpo::command_line_parser(argc, argv).options(cmdline_options).run(), vm);

		if (vm.count("help")) {
			std::cout << cmdline_options << std::endl;
			return 0;
		}

		bpo::notify(vm);
	} catch (const std::exception &e) {
		std::cerr << "Invalid options: " << e.what() << "\n" << cmdline_options << std::endl;
		return -1;
	}

	elliptics_transport t(log_file, log_level);
	t.add_remotes(remotes);
	t.set_namespace(ns);
	t.set_groups(elliptics::parse_groups(groups.c_str()));

	indexes::index<elliptics_transport> idx(t, sk);

	time_t tm = time(NULL);
	srand(tm);

	dprintf("index: init: t: %zd\n", tm);

	test<elliptics_transport> tt(t);
}
