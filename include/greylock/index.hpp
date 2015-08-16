#ifndef __INDEXES_INDEX_HPP
#define __INDEXES_INDEX_HPP

#include "greylock/page.hpp"

#include <blackhole/blackhole.hpp>

#include <atomic>
#include <map>

#define INDEXES_LOG_ERROR blackhole::defaults::severity::error
#define INDEXES_LOG_WARNING blackhole::defaults::severity::warning
#define INDEXES_LOG_INFO blackhole::defaults::severity::info
#define INDEXES_LOG_NOTICE blackhole::defaults::severity::notice
#define INDEXES_LOG_DEBUG blackhole::defaults::severity::debug

namespace ioremap { namespace greylock {

typedef blackhole::defaults::severity log_level;
typedef blackhole::verbose_logger_t<log_level> logger_base;
typedef blackhole::wrapper_t<logger_base> logger;

struct index_meta {
	enum {
		serialization_version_1 = 1,
		serialization_version_max
	};

	index_meta() {
		page_index = 0;
		num_pages = 0;
		num_leaf_pages = 0;
		generation_number = 0;
	}

	index_meta(const index_meta &o) {
		*this = o;
	}

	index_meta &operator=(const index_meta &o) {
		page_index = o.page_index.load();
		num_pages = o.num_pages.load();
		num_leaf_pages = o.num_leaf_pages.load();
		generation_number = o.generation_number.load();

		return *this;
	}

	std::atomic<unsigned long long> page_index;
	std::atomic<unsigned long long> num_pages;
	std::atomic<unsigned long long> num_leaf_pages;
	std::atomic<unsigned long long> generation_number;

	bool operator != (const index_meta &other) const {
		return ((page_index != other.page_index) ||
				(num_pages != other.num_pages) ||
				(num_leaf_pages != other.num_leaf_pages) ||
				(generation_number != other.generation_number)
			);
	}

	std::string str() const {
		std::ostringstream ss;
		ss << "page_index: " << page_index <<
			", num_pages: " << num_pages <<
			", num_leaf_pages: " << num_leaf_pages <<
			", generation_number: " << generation_number;
		return ss.str();
	}
};

struct recursion {
	key page_start;
	key split_key;
};

struct remove_recursion {
	key page_start;
	bool removed = false;
};

template <typename T>
class index {
public:
	index(T &t, const eurl &sk, bool read_only): m_t(t), m_log(t.logger()), m_sk(sk), m_read_only(read_only) {
		std::vector<status> meta = m_t.read_all(meta_key());

		struct separate_index_meta {
			int group = 0;
			index_meta meta;
		};

		std::vector<separate_index_meta> mg;

		for (auto it = meta.begin(), end = meta.end(); it != end; ++it) {
			if (it->error) {
				continue;
			}

			separate_index_meta tmp;

			msgpack::unpacked result;
			msgpack::unpack(&result, (const char *)it->data.data(), it->data.size());
			msgpack::object obj = result.get();

			tmp.meta = obj.as<index_meta>();

			tmp.group = it->group;

			mg.emplace_back(tmp);
		}

		if (mg.empty()) {
			if (!m_read_only) {
				start_page_init();
				return;
			}

			std::ostringstream ss;
			ss << "index: could not read index metadata from '" << sk.str() << "'and not allowed to create new index";
			throw std::runtime_error(ss.str());
		}

		uint64_t highest_generation_number = 0;
		for (auto it = mg.begin(), end = mg.end(); it != end; ++it) {
			if (it->meta.generation_number >= highest_generation_number) {
				highest_generation_number = it->meta.generation_number;
				m_meta = it->meta;
			}
		}

		std::vector<int> recovery_groups;
		std::vector<int> good_groups;
		for (auto it = mg.begin(), end = mg.end(); it != end; ++it) {
			if (it->meta.generation_number == highest_generation_number) {
				good_groups.push_back(it->group);
			} else {
				recovery_groups.push_back(it->group);
			}
		}

		m_t.set_groups(good_groups);

		if (highest_generation_number == 0) {
			if (!m_read_only) {
				start_page_init();
				return;
			}

			std::ostringstream ss;
			ss << "index: metadata for index '" << sk.str() << "' is corrupted (all generation numbers are zero) "
				"and not allowed to create new index";
			throw std::runtime_error(ss.str());
		}

		if (recovery_groups.empty())
			return;

		// do not try to recover read-only index
		if (m_read_only)
			return;

		size_t pages_recovered = 0;
		for (auto it = page_begin(), end = page_end(); it != end; ++it) {
			BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: page: %s: %s -> %s",
				it.url().str().c_str(), it->str().c_str(), print_groups(recovery_groups).c_str());

			std::vector<status> wr = m_t.write(recovery_groups, it.url(), it->save(), default_reserve_size, false);
			
			recovery_groups.clear();
			for (auto r = wr.begin(), end = wr.end(); r != end; ++r) {
				if (!r->error) {
					recovery_groups.push_back(r->group);
				}
			}

			if (recovery_groups.size() == 0)
				break;

			pages_recovered++;
		}

		good_groups.insert(good_groups.end(), recovery_groups.begin(), recovery_groups.end());
		m_t.set_groups(good_groups);

		BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: opened: page_index: %ld, groups: %s, pages recovered: %zd",
				m_meta.page_index, print_groups(good_groups).c_str(), pages_recovered);
	}

	~index() {
		if (!m_read_only) {
			// only sync index metadata at destruction time for performance
			meta_write();
		}
	}

	index_meta meta() const {
		return m_meta;
	}

	key search(const key &obj) const {
		auto found = search(m_sk, obj);
		if (found.second < 0)
			return key();

		return found.first.objects[found.second];
	}

	int insert(const key &obj) const {
		return -EPERM;
	}

	int insert(const key &obj) {
		if (m_read_only)
			return -EPERM;

		recursion tmp;
		int ret = insert(m_sk, obj, tmp);
		if (ret < 0)
			return ret;

		m_meta.generation_number++;
		return 0;
	}

	int remove(const key &obj) const {
		return -EPERM;
	}

	int remove(const key &obj) {
		if (m_read_only)
			return -EPERM;

		remove_recursion tmp;
		int ret = remove(m_sk, obj, tmp);
		if (ret < 0)
			return ret;

		m_meta.generation_number++;
		return 0;
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

	page_iterator<T> page_begin() const {
		return page_iterator<T>(m_t, m_sk);
	}

	page_iterator<T> page_end() const {
		page p;
		return page_iterator<T>(m_t, p);
	}

	std::string print_groups(const std::vector<int> &groups) const {
		std::ostringstream ss;
		for (size_t pos = 0; pos < groups.size(); ++pos) {
			ss << groups[pos];
			if (pos != groups.size() - 1)
				ss << ":";
		}

		return ss.str();
	}

private:
	T &m_t;
	const logger &m_log;
	eurl m_sk;

	// when true, metadata for new index will NOT be created and updated at destruction time
	// should be TRUE for read-only indexes, for example for indexes created to read metadata
	// or for search and indexes intersection
	bool m_read_only;

	index_meta m_meta;

	eurl meta_key() const {
		eurl ret;
		ret.bucket = m_sk.bucket;
		ret.key = m_sk.key + ".meta";
		return ret;
	}

	void meta_write() {
		std::stringstream ss;
		msgpack::pack(ss, m_meta);

		std::string ms = ss.str();
		m_t.write(meta_key(), ms, true);

		BH_LOG(m_log, INDEXES_LOG_INFO, "index: meta updated: %s, size: %d", m_meta.str().c_str(), ms.size());
	}

	void start_page_init() {
		page start_page;

		m_t.write(m_sk, start_page.save());
		m_meta.num_pages++;
	}

	std::pair<page, int> search(const eurl &page_key, const key &obj) const {
		status e = m_t.read(page_key);
		if (e.error) {
			return std::make_pair(page(), e.error);
		}

		page p;
		p.load(e.data.data(), e.data.size());

		int found_pos = p.search_node(obj);
		if (found_pos < 0) {
			BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: search: %s: page: %s -> %s, found_pos: %d",
				obj.str().c_str(),
				page_key.str().c_str(), p.str().c_str(),
				found_pos);

			return std::make_pair(p, found_pos);
		}

		BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: search: %s: page: %s -> %s, found_pos: %d, found_key: %s",
			obj.str().c_str(),
			page_key.str().c_str(), p.str().c_str(),
			found_pos, p.objects[found_pos].str().c_str());

		if (p.is_leaf())
			return std::make_pair(p, found_pos);

		return search(p.objects[found_pos].url, obj);
	}

	// returns true if page at @page_key has been split after insertion
	// key used to store split part has been saved into @obj.url
	int insert(const eurl &page_key, const key &obj, recursion &rec) {
		status e = m_t.read(page_key);
		if (e.error) {
			return e.error;
		}

		int err;
		page p;
		p.load(e.data.data(), e.data.size());

		page split;

		BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: insert: %s: page: %s -> %s",
			obj.str().c_str(), page_key.str().c_str(), p.str().c_str());

		if (!p.is_leaf()) {
			int found_pos = p.search_node(obj);
			if (found_pos < 0) {
				BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: insert: %s: page: %s -> %s, found_pos: %d",
					obj.str().c_str(),
					page_key.str().c_str(), p.str().c_str(),
					found_pos);

				// this is not a leaf node, but there is no leaf in @objects
				// this is the only reason non-leaf page search failed,
				// thus create new leaf node
				//
				// this path can only be taken once - when new empty index has been created
				key leaf_key;
				leaf_key.id = obj.id;
				leaf_key.url = generate_page_url();

				page leaf(true), unused_split;
				leaf.insert_and_split(obj, unused_split);
				err = check(m_t.write(leaf_key.url, leaf.save()));
				if (err)
					return err;

				// no need to perform recursion unwind, since there were no entry for this new leaf
				// which can only happen when page was originally empty
				p.insert_and_split(leaf_key, unused_split);
				p.next = leaf_key.url;
				err = check(m_t.write(page_key, p.save()));
				if (err)
					return err;

				BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: insert: %s: page: %s -> %s, leaf: %s -> %s",
						obj.str().c_str(),
						page_key.str().c_str(), p.str().c_str(),
						leaf_key.str().c_str(), leaf.str().c_str());

				m_meta.num_pages++;
				m_meta.num_leaf_pages++;
				return 0;
			}

			key &found = p.objects[found_pos];

			BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: insert: %s: page: %s -> %s, found_pos: %d, found_key: %s",
				obj.str().c_str(),
				page_key.str().c_str(), p.str().c_str(),
				found_pos, found.str().c_str());

			insert(found.url, obj, rec);

			BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: insert: %s: returned: %s -> %s, "
					"found_pos: %d, found_key: %s, "
					"rec: page_start: %s, split_key: %s",
					obj.str().c_str(), page_key.str().c_str(), p.str().c_str(),
					found_pos, found.str().c_str(),
					rec.page_start.str().c_str(), rec.split_key.str().c_str());

			// true if we should not unwind recursion and just return
			// false if either split page has to be written or page changed and has to be written
			bool want_return = true;

			if (found != rec.page_start) {
				BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: p: %s: replace: key: %s: id: %s -> %s",
					p.str().c_str(), found.str().c_str(), found.id.c_str(), rec.page_start.id.c_str());
				found.id = rec.page_start.id;

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
				return 0;
			}
		} else {
			p.insert_and_split(obj, split);
		}

		rec.page_start = p.objects.front();
		rec.split_key = key();

		if (!split.is_empty()) {
			// generate key for split page
			rec.split_key.url = generate_page_url();
			rec.split_key.id = split.objects.front().id;

			split.next = p.next;
			p.next = rec.split_key.url;

			BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: insert: %s: write split page: %s -> %s, split: key: %s -> %s",
					obj.str().c_str(),
					page_key.str().c_str(), p.str().c_str(),
					rec.split_key.str().c_str(), split.str().c_str());
			err = check(m_t.write(rec.split_key.url, split.save()));
			if (err)
				return err;

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
			old_root_key.url = generate_page_url();
			old_root_key.id = p.objects.front().id;

			err = check(m_t.write(old_root_key.url, p.save()));
			if (err)
				return err;

			// we have written split page and old root page above
			// now its time to create and write the new root
			page new_root, unused_split;
			new_root.insert_and_split(old_root_key, unused_split);
			new_root.insert_and_split(rec.split_key, unused_split);

			new_root.next = new_root.objects.front().url;

			err = check(m_t.write(m_sk, new_root.save()));
			if (err)
				return err;

			BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: insert: %s: write split page: %s -> %s, "
					"old_root_key: %s, new_root: %s",
					obj.str().c_str(),
					page_key.str().c_str(), p.str().c_str(),
					old_root_key.str().c_str(), new_root.str().c_str());

			m_meta.num_pages++;

		} else {
			BH_LOG(m_log, INDEXES_LOG_NOTICE, "insert: %s: write main page: %s -> %s",
				obj.str().c_str(), page_key.str().c_str(), p.str().c_str());
			err = check(m_t.write(page_key, p.save(), true));
		}

		return err;
	}

	// returns true if page at @page_key has been split after insertion
	// key used to store split part has been saved into @obj.url
	int remove(const eurl &page_key, const key &obj, remove_recursion &rec) {
		status e = m_t.read(page_key);
		if (e.error) {
			return e.error;
		}

		int err;
		page p;
		p.load(e.data.data(), e.data.size());

		BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: remove: %s: page: %s -> %s",
				obj.str().c_str(), page_key.str().c_str(), p.str().c_str());

		int found_pos = p.search_node(obj);
		if (found_pos < 0) {
			BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: remove: %s: page: %s -> %s, found_pos: %d",
				obj.str().c_str(),
				page_key.str().c_str(), p.str().c_str(),
				found_pos);

			return -ENOENT;
		}

		key &found = p.objects[found_pos];

		BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: remove: %s: page: %s -> %s, found_pos: %d, found_key: %s",
			obj.str().c_str(),
			page_key.str().c_str(), p.str().c_str(),
			found_pos, found.str().c_str());

		if (p.is_leaf() || rec.removed) {
			p.remove(found_pos);
		} else {
			err = remove(found.url, obj, rec);
			if (err < 0)
				return err;

			// we have removed key from the underlying page, and the first key of that page hasn't been changed
			if (!rec.page_start)
				return 0;

			// the first key of the underlying page has been changed, update appropriate key in the current page
			found.id = rec.page_start.id;
		}

		BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: remove: %s: returned: %s -> %s, found_pos: %d, found_key: %s",
				obj.str().c_str(),
				page_key.str().c_str(), p.str().c_str(),
				found_pos, found.str().c_str());

		rec.page_start.id.clear();
		rec.removed = false;

		if (p.objects.size() != 0) {
			// we have to update higher level page if start of the current page has been changed
			// we can not use @found here, since it could be removed from the current page
			if (found_pos == 0) {
				rec.page_start.id = p.objects.front().id;
			}

			err = check(m_t.write(page_key, p.save()));
			if (err)
				return err;
		} else {
			// if current page is empty, we have to remove appropriate link from the higher page
			rec.removed = true;

			err = check(m_t.remove(page_key));
			if (err)
				return err;

			m_meta.num_pages--;
			if (p.is_leaf())
				m_meta.num_leaf_pages--;
		}

		return 0;
	}

	eurl generate_page_url() {
		status st = m_t.get_bucket(default_reserve_size);
		if (st.error < 0) {
			BH_LOG(m_log, INDEXES_LOG_ERROR, "index: generate_page_url: could not get bucket, "
				"generated page URL will not be valid: %s [%d]",
					st.message, st.error);
		}

		eurl ret;
		ret.bucket = st.data.to_string();

		ret.key = m_sk.key + "." + elliptics::lexical_cast(m_meta.page_index);
		BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: generated key: %s", ret.str().c_str());
		m_meta.page_index++;
		return ret;
	}

	int check(const std::vector<status> &wr) {
		std::vector<int> groups;
		for (auto r = wr.begin(), end = wr.end(); r != end; ++r) {
			if (!r->error) {
				groups.push_back(r->group);
			}
		}

		m_t.set_groups(groups);

		if (groups.empty())
			return -EIO;

		return 0;
	}
};

template<typename T>
class read_only_index: public index<T> {
public:
	read_only_index(T &t, const eurl &start): index<T>(t, start, true) {}
};

template<typename T>
class read_write_index: public index<T> {
public:
	read_write_index(T &t, const eurl &start): index<T>(t, start, false) {}
};

}} // namespace ioremap::greylock

namespace msgpack {
static inline ioremap::greylock::index_meta &operator >>(msgpack::object o, ioremap::greylock::index_meta &meta)
{
	if (o.type != msgpack::type::ARRAY) {
		std::ostringstream ss;
		ss << "page unpack: type: " << o.type <<
			", must be: " << msgpack::type::ARRAY <<
			", size: " << o.via.array.size;
		throw std::runtime_error(ss.str());
	}

	object *p = o.via.array.ptr;
	const uint32_t size = o.via.array.size;
	uint16_t version = 0;
	p[0].convert(&version);
	switch (version) {
	case ioremap::greylock::index_meta::serialization_version_1: {
		if (size != 5) {
			std::ostringstream ss;
			ss << "page unpack: array size mismatch: read: " << size << ", must be: 4";
			throw std::runtime_error(ss.str());
		}

		unsigned long long tmp;

		p[1].convert(&tmp);
		meta.page_index = tmp;

		p[2].convert(&tmp);
		meta.num_pages = tmp;

		p[3].convert(&tmp);
		meta.num_leaf_pages = tmp;

		p[4].convert(&tmp);
		meta.generation_number = tmp;
		break;
	}
	default: {
		std::ostringstream ss;
		ss << "page unpack: version mismatch: read: " << version <<
			", must be: < " << ioremap::greylock::page::serialization_version_max;
		throw std::runtime_error(ss.str());
	}
	}

	return meta;
}

template <typename Stream>
inline msgpack::packer<Stream> &operator <<(msgpack::packer<Stream> &o, const ioremap::greylock::index_meta &meta)
{
	o.pack_array(5);
	o.pack((int)ioremap::greylock::index_meta::serialization_version_1);
	o.pack(meta.page_index.load());
	o.pack(meta.num_pages.load());
	o.pack(meta.num_leaf_pages.load());
	o.pack(meta.generation_number.load());

	return o;
}

} // namespace msgpack


#endif // __INDEXES_INDEX_HPP
