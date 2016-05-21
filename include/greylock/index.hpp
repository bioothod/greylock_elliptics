#ifndef __INDEXES_INDEX_HPP
#define __INDEXES_INDEX_HPP

#include "greylock/io.hpp"
#include "greylock/page.hpp"

#include <atomic>
#include <map>

namespace ioremap { namespace greylock {

typedef blackhole::defaults::severity log_level;
typedef blackhole::verbose_logger_t<log_level> logger_base;
typedef blackhole::wrapper_t<logger_base> logger;

struct index_meta {
	enum {
		serialization_version_6 = 6
	};

	index_meta() {
		page_index = 0;
		num_pages = 0;
		num_leaf_pages = 0;
		generation_number_sec = 0;
		generation_number_nsec = 0;
		num_keys = 0;
	}

	index_meta(const index_meta &o) {
		*this = o;
	}

	index_meta &operator=(const index_meta &o) {
		page_index = o.page_index.load();
		num_pages = o.num_pages.load();
		num_leaf_pages = o.num_leaf_pages.load();
		generation_number_sec = o.generation_number_sec.load();
		generation_number_nsec = o.generation_number_nsec.load();
		num_keys = o.num_keys.load();

		return *this;
	}

	std::atomic<unsigned long long> page_index;
	std::atomic<unsigned long long> num_pages;
	std::atomic<unsigned long long> num_leaf_pages;
	std::atomic<unsigned long long> generation_number_sec;
	std::atomic<unsigned long long> generation_number_nsec;
	std::atomic<unsigned long long> num_keys;

	void update_generation_number() {
		struct timespec ts;
		clock_gettime(CLOCK_REALTIME, &ts);

		generation_number_sec = ts.tv_sec;
		generation_number_nsec = ts.tv_nsec;
	}

	bool operator != (const index_meta &other) const {
		return ((page_index != other.page_index) ||
				(num_pages != other.num_pages) ||
				(num_leaf_pages != other.num_leaf_pages) ||
				(generation_number_sec != other.generation_number_sec) ||
				(generation_number_nsec != other.generation_number_nsec) ||
				(num_keys != other.num_keys)
			);
	}

	std::string str() const {
		std::ostringstream ss;
		ss << "page_index: " << page_index <<
			", num_pages: " << num_pages <<
			", num_leaf_pages: " << num_leaf_pages <<
			", generation_number: " << generation_number_sec << "." << generation_number_nsec <<
			", num_keys: " << num_keys
			;
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

static inline const char *greylock_print_time(const struct dnet_time *t, char *dst, int dsize)
{
	char str[64];
	struct tm tm;

	localtime_r((time_t *)&t->tsec, &tm);
	strftime(str, sizeof(str), "%F %R:%S", &tm);

	snprintf(dst, dsize, "%s.%06llu", str, (long long unsigned) t->tnsec / 1000);
	return dst;
}


class index {
public:
	index(ebucket::bucket_processor &bp, const eurl &sk, bool read_only) :
			m_bp(bp), m_log(bp.logger()), m_index_name(sk), m_read_only(read_only) {
		m_start_key = generate_start_key(m_index_name);
		m_meta_key = generate_meta_key(m_index_name);

		if (!m_read_only) {
			if (need_recovery()) {
				elliptics::error_info err = index_recovery();
				if (err)
					err.throw_error();
			}
		}

		auto async = io::read_data(m_bp, meta_key(), false);
		if (!async.is_valid()) {
			elliptics::throw_error(-EINVAL, "invalid async read result for index %s, meta_key: %s",
					sk.str().c_str(), meta_key().str().c_str());
		}

		if (async.error()) {
			if (async.error().code() == -ENOENT) {
				if (m_read_only) {
					elliptics::throw_error(-EROFS, "index: could not read index metadata for index %s, meta_key: %s, "
							"and not allowed to create new index",
						sk.str().c_str(), meta_key().str().c_str());
				}

				start_page_init();
				return;
			}

			async.error().throw_error();
		}

		elliptics::read_result_entry ent = async.get_one();
		if (ent.error() || !ent.is_valid()) {
			// invalid entry means there are no positive results
			if ((ent.error().code() == -ENOENT) || !ent.is_valid()) {
				if (m_read_only) {
					elliptics::throw_error(-EROFS, "index: could not read index metadata for "
							"index %s, meta_key: %s, is_valid: %d, error_code: %d, "
							"and not allowed to create new index",
						sk.str().c_str(), meta_key().str().c_str(), ent.is_valid(), ent.error().code());
				}

				start_page_init();
				return;
			}

			ent.error().throw_error();
		}

		const auto &file = ent.file();

		try {
			msgpack::unpacked result;
			msgpack::unpack(&result, file.data<char>(), file.size());
			result.get().convert(&m_meta);
		} catch (const std::exception &e) {
			BH_LOG(bp.logger(), INDEXES_LOG_ERROR, "failed to unpack start page: %s, data size: %ld",
					meta_key().str().c_str(), file.size());
			elliptics::throw_error(-EINVAL, "failed to unpack start page: %s, data size: %ld",
					meta_key().str().c_str(), file.size());
		}
	}

	~index() {
		if (!m_read_only && m_modified) {
			// only sync index metadata at destruction time for performance
			meta_write();
		}
	}

	index_meta meta() const {
		return m_meta;
	}

	const eurl &start_key() const {
		return m_start_key;
	}

	key search(const key &obj) const {
		auto found = search(start_key(), obj);
		if (found.second < 0)
			return key();

		return found.first.objects[found.second];
	}

	elliptics::error_info insert(const key &obj) const {
		return elliptics::create_error(-EPERM, "can not insert object '%s' into constant index", obj.str().c_str());
	}

	elliptics::error_info insert(const key &obj) {
		if (m_read_only)
			return elliptics::create_error(-EPERM, "can not insert object '%s' into read-only index", obj.str().c_str());

		m_modified = true;

		recursion tmp;
		BH_LOG(m_log, INDEXES_LOG_NOTICE, "insert: start: sk: %s, key: %s", start_key().str().c_str(), obj.str().c_str());
		elliptics::error_info err = insert(start_key(), obj, tmp);
		BH_LOG(m_log, INDEXES_LOG_NOTICE, "insert: completed: sk: %s, key: %s, err: %s [%d]",
				start_key().str().c_str(), obj.str().c_str(), err.message(), err.code());
		if (err)
			return err;

		m_meta.update_generation_number();
		return err;
	}

	elliptics::error_info remove(const key &obj) const {
		return elliptics::create_error(-EPERM, "can not remove object '%s' from constant index", obj.str().c_str());
	}

	elliptics::error_info remove(const key &obj) {
		if (m_read_only)
			return elliptics::create_error(-EPERM, "can not remove object '%s' from read-only index", obj.str().c_str());

		m_modified = true;

		BH_LOG(m_log, INDEXES_LOG_NOTICE, "remove: start: sk: %s, key: %s", start_key().str().c_str(), obj.str().c_str());
		remove_recursion tmp;
		elliptics::error_info err = remove(start_key(), obj, tmp);
		BH_LOG(m_log, INDEXES_LOG_NOTICE, "remove: completed: sk: %s, key: %s, err: %s [%d]",
				start_key().str().c_str(), obj.str().c_str(), err.message(), err.code());
		if (err)
			return err;

		m_meta.update_generation_number();
		return err;
	}

	iterator begin(const std::string &k) const {
		key zero;
		zero.id = k;

		auto found = search(start_key(), zero);
		if (found.second < 0)
			found.second = 0;

		return iterator(m_bp, found.first, found.second);
	}

	iterator begin() const {
		return begin(std::string("\0"));
	}

	iterator end() const {
		page p;
		return iterator(m_bp, p, 0);
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

	page_iterator page_begin() const {
		return page_iterator(m_bp, false, start_key());
	}

	page_iterator page_begin_latest() const {
		return page_iterator(m_bp, true, start_key());
	}

	page_iterator page_end() const {
		page p;
		return page_iterator(m_bp, p);
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

	static greylock::eurl generate_start_key(const greylock::eurl &index_name) {
		return generate_greylock_key(index_name.bucket, "i", index_name.key);
	}

	static greylock::eurl generate_meta_key(const greylock::eurl &index_name) {
		return generate_greylock_key(index_name.bucket, "m", index_name.key);
	}

	static greylock::eurl generate_page_key(const std::string &bucket, const std::string &key) {
		return generate_greylock_key(bucket, "p", key);
	}

private:
	ebucket::bucket_processor &m_bp;
	const logger &m_log;
	eurl m_index_name;
	eurl m_start_key;
	eurl m_meta_key;

	// when true, there was index modification, update its metadata
	bool m_modified = false;

	// when true, metadata for new index will NOT be created and updated at destruction time
	// should be TRUE for read-only indexes, for example for indexes created to read metadata
	// or for search and indexes intersection
	bool m_read_only;

	index_meta m_meta;

	const eurl &meta_key() const {
		return m_meta_key;
	}

	static greylock::eurl generate_greylock_key(const std::string &bucket, const std::string &prefix, const std::string &key) {
		char tmp[prefix.size() + 1 + key.size() + 1];
		int sz = snprintf(tmp, sizeof(tmp), "%s.%s", prefix.c_str(), key.c_str());
		tmp[prefix.size()] = '\0';

		greylock::eurl ret;
		ret.bucket = bucket;
		ret.key.assign(tmp, sz);

		return ret;
	}

	elliptics::error_info generate_page_url(eurl &url) {
		std::string bucket;
		elliptics::error_info err = m_bp.get_bucket(default_reserve_size, bucket);
		if (err) {
			BH_LOG(m_log, INDEXES_LOG_ERROR, "index: generate_page_url: could not get bucket, "
				"generated page URL will not be valid: %s [%d]",
					err.message(), err.code());
			return err;
		}

		std::ostringstream ss;
		ss << m_index_name.key << "." << m_meta.page_index.fetch_add(1);
		url = generate_page_key(bucket, ss.str());

		BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: generated key url: %s", url.str().c_str());
		return elliptics::error_info();
	}


	void meta_write() {
		std::stringstream ss;
		msgpack::pack(ss, m_meta);

		std::string ms = ss.str();
		io::write(m_bp, meta_key(), ms, 0, true);

		BH_LOG(m_log, INDEXES_LOG_INFO, "index: meta updated: key: %s, meta: %s, size: %d",
				meta_key().str(), m_meta.str().c_str(), ms.size());
	}

	void start_page_init() {
		page start_page;

		io::write(m_bp, start_key(), start_page.save(), 0, true);
		m_meta.num_pages++;
	}

	elliptics::error_info index_recovery() {
		size_t pages_recovered = 0;

		for (auto it = page_begin_latest(), end = page_end(); it != end; ++it) {
			BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: recovering page: url: %s, content: %s",
				it.url().str().c_str(), it->str().c_str());

			int recovered = 0;

			elliptics::async_write_result wr = io::write(m_bp, it.url(), it->save(), default_reserve_size, false);
			for (auto r = wr.begin(), end = wr.end(); r != end; ++r) {
				if (!r->error()) {
					recovered++;
				}
			}

			if (!recovered) {
				BH_LOG(m_log, INDEXES_LOG_ERROR, "index: recovering page: url: %s, content: %s: could not recover page",
						it.url().str().c_str(), it->str().c_str());

				return elliptics::create_error(-ENOEXEC,
						"index: recovering page: url: %s, content: %s: could not recover page",
						it.url().str().c_str(), it->str().c_str());
			}

			pages_recovered++;
		}

		BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: opened: page_index: %ld, pages recovered: %ld",
				m_meta.page_index, pages_recovered);
		return elliptics::error_info();
	}

	bool need_recovery() {
		elliptics::async_lookup_result lookup = io::prepare_latest(m_bp, start_key());
		lookup.wait();

		if (lookup.error()) {
			BH_LOG(m_log, INDEXES_LOG_ERROR, "index: need_recovery: %s requires recovery, prepare_latest error: %s [%d]",
					start_key().str().c_str(), lookup.error().message().c_str(), lookup.error().code());

			// if there is no start key, there is nothing to recover
			if (lookup.error().code() == -ENOENT)
				return false;

			return true;
		}

		struct dnet_time first_time;
		bool empty = true;
		bool need_recovery = false;

		char info_str[128], first_str[128];

		for (auto begin = lookup.begin(), end = lookup.end(), it = begin; it != end; ++it) {
			struct dnet_file_info *info = it->file_info();

			if (empty) {
				first_time = info->mtime;
				empty = false;
			}

			if (dnet_time_cmp(&first_time, &info->mtime)) {
				need_recovery = true;
			}

			BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: need_recovery: %s: group: %d, mtime: %s (%ld.%ld), "
					"first_time: %s (%ld.%ld), need_recovery: %d",
					start_key().str().c_str(), int(it->command()->id.group_id),
					greylock_print_time(&info->mtime, info_str, sizeof(info_str)), info->mtime.tsec, info->mtime.tnsec,
					greylock_print_time(&first_time, first_str, sizeof(first_str)), first_time.tsec, first_time.tnsec,
					need_recovery);
		}

		return need_recovery;
	}


	std::pair<page, int> search(const eurl &page_key, const key &obj) const {
		elliptics::async_read_result async = io::read_data(m_bp, page_key, false);
		elliptics::read_result_entry ent = async.get_one();

		if (async.error() || !async.is_valid()) {
			BH_LOG(m_log, INDEXES_LOG_ERROR, "index: search: %s: page: %s, could not read page, "
					"async: is_valid: %d, error: %s [%d]",
				obj.str().c_str(), page_key.str().c_str(),
				async.is_valid(), async.error().message(), async.error().code());
			return std::make_pair(page(), async.error().code());
		}

		if (ent.error() || !ent.is_valid()) {
			BH_LOG(m_log, INDEXES_LOG_ERROR, "index: search: %s: page: %s, could not read page, "
					"entry: is_valid: %d, error: %s [%d]",
				obj.str().c_str(), page_key.str().c_str(),
				ent.is_valid(), ent.error().message(), ent.error().code());
			return std::make_pair(page(), ent.error().code());
		}

		page p;
		p.load(ent.file().data(), ent.file().size());

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
	elliptics::error_info insert(const eurl &page_key, const key &obj, recursion &rec) {
		elliptics::async_read_result async = io::read_data(m_bp, page_key, false);
		elliptics::read_result_entry ent = async.get_one();

		if (async.error() || !async.is_valid()) {
			BH_LOG(m_log, INDEXES_LOG_ERROR, "index: insert: %s: page: %s, could not read page, "
					"async: is_valid: %d, error: %s [%d]",
				obj.str().c_str(), page_key.str().c_str(),
				async.is_valid(), async.error().message(), async.error().code());
			return async.error();
		}

		if (ent.error() || !ent.is_valid()) {
			BH_LOG(m_log, INDEXES_LOG_ERROR, "index: insert: %s: page: %s, could not read page, "
					"entry: is_valid: %d, error: %s [%d]",
				obj.str().c_str(), page_key.str().c_str(),
				ent.is_valid(), ent.error().message(), ent.error().code());
			return ent.error();
		}

		bool replaced = false;

		elliptics::error_info err;

		page p;
		p.load(ent.file().data(), ent.file().size());

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

				// this is not a leaf node, but there are no leafs in @obj.objects
				// this is the only reason non-leaf page search failed,
				// thus create new leaf node
				//
				// this path can only be taken once - when new empty index is being created
				key leaf_key;
				leaf_key = obj;
				err = generate_page_url(leaf_key.url);
				if (err)
					return err;

				page leaf(true), unused_split;
				leaf.insert_and_split(obj, unused_split, replaced);
				if (!replaced)
					m_meta.num_keys++;
				err = check(io::write(m_bp, leaf_key.url, leaf.save(), default_reserve_size, true));
				if (err)
					return err;

				// no need to perform recursion unwind, since there were no entry for this new leaf
				// which can only happen when page was originally empty
				// do not increment @num_keys since it is not a leaf page
				p.insert_and_split(leaf_key, unused_split, replaced);
				p.next = leaf_key.url;
				err = check(io::write(m_bp, page_key, p.save(), default_reserve_size, true));
				if (err)
					return err;

				BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: insert: %s: page: %s -> %s, leaf: %s -> %s",
						obj.str().c_str(),
						page_key.str().c_str(), p.str().c_str(),
						leaf_key.str().c_str(), leaf.str().c_str());

				m_meta.num_pages++;
				m_meta.num_leaf_pages++;
				return elliptics::error_info();
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
					obj.str(), page_key.str(), p.str(),
					found_pos, found.str(),
					rec.page_start.str(), rec.split_key.str());

			// true if we should not unwind recursion and just return
			// false if either split page has to be written or page changed and has to be written
			bool want_return = true;

			if (found != rec.page_start) {
				BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: p: %s: replace: key: %s: id: %s -> %s",
					p.str().c_str(), found.str().c_str(), found.id.c_str(), rec.page_start.id.c_str());
				found.id = rec.page_start.id;
				found.timestamp = rec.page_start.timestamp;

				// page has been changed, it must be written into storage
				want_return = false;
			}

			if (rec.split_key) {
				// there is a split page, it was already written into the storage,
				// now its time to insert it into parent and upate parent
				//
				// not a leaf page, do not increment @num_keys
				p.insert_and_split(rec.split_key, split, replaced);

				want_return = false;
			}

			if (want_return) {
				rec.page_start = p.objects.front();
				rec.split_key = key();
				return elliptics::error_info();
			}
		} else {
			// this is a leaf page, increment @num_keys if it was not a key replacement
			p.insert_and_split(obj, split, replaced);
			if (!replaced)
				m_meta.num_keys++;
		}

		rec.page_start = p.objects.front();
		rec.split_key = key();

		if (!split.is_empty()) {
			// generate key for split page
			rec.split_key = split.objects.front();
			err = generate_page_url(rec.split_key.url);
			if (err)
				return err;

			split.next = p.next;
			p.next = rec.split_key.url;

			BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: insert: %s: write split page: %s -> %s, split: key: %s -> %s",
					obj.str().c_str(),
					page_key.str().c_str(), p.str().c_str(),
					rec.split_key.str().c_str(), split.str().c_str());
			err = check(io::write(m_bp, rec.split_key.url, split.save(), default_reserve_size, true));
			if (err)
				return err;

			m_meta.num_pages++;
			if (p.is_leaf())
				m_meta.num_leaf_pages++;
		}

		if (!split.is_empty() && page_key == start_key()) {
			// if we split root page, put old root data into new key
			// root must always be accessible via start key
			// generate new root, which will host data for 2 new pages:
			// split and old root

			key old_root_key;
			old_root_key = p.objects.front();
			err = generate_page_url(old_root_key.url);
			if (err)
				return err;

			err = check(io::write(m_bp, old_root_key.url, p.save(), default_reserve_size, true));
			if (err)
				return err;

			// we have written split page and old root page above
			// now its time to create and write the new root
			//
			// root pages are never leaf pages, do not increment @num_keys
			page new_root, unused_split;
			new_root.insert_and_split(old_root_key, unused_split, replaced);
			new_root.insert_and_split(rec.split_key, unused_split, replaced);

			new_root.next = new_root.objects.front().url;

			err = check(io::write(m_bp, start_key(), new_root.save(), default_reserve_size, true));
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
			err = check(io::write(m_bp, page_key, p.save(), default_reserve_size, true));
		}

		return err;
	}

	// returns true if page at @page_key has been split after insertion
	// key used to store split part has been saved into @obj.url
	elliptics::error_info remove(const eurl &page_key, const key &obj, remove_recursion &rec) {
		elliptics::async_read_result async = io::read_data(m_bp, page_key, false);
		elliptics::read_result_entry ent = async.get_one();

		if (async.error() || !async.is_valid()) {
			BH_LOG(m_log, INDEXES_LOG_ERROR, "index: remove: %s: page: %s, could not read page, "
					"async: is_valid: %d, error: %s [%d]",
				obj.str().c_str(), page_key.str().c_str(),
				async.is_valid(), async.error().message(), async.error().code());
			return async.error();
		}

		if (ent.error() || !ent.is_valid()) {
			BH_LOG(m_log, INDEXES_LOG_ERROR, "index: remove: %s: page: %s, could not read page, "
					"entry: is_valid: %d, error: %s [%d]",
				obj.str().c_str(), page_key.str().c_str(),
				ent.is_valid(), ent.error().message(), ent.error().code());
			return ent.error();
		}

		elliptics::error_info err;

		page p;
		p.load(ent.file().data(), ent.file().size());

		BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: remove: %s: page: %s -> %s",
				obj.str().c_str(), page_key.str().c_str(), p.str().c_str());

		int found_pos = p.search_node(obj);
		if (found_pos < 0) {
			BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: remove: %s: page: %s -> %s, found_pos: %d",
				obj.str().c_str(),
				page_key.str().c_str(), p.str().c_str(),
				found_pos);

			return elliptics::create_error(-ENOENT, "index: remove: %s: page: %s -> %s, found_pos: %d: could not find object",
				obj.str().c_str(),
				page_key.str().c_str(), p.str().c_str(),
				found_pos);
		}

		// we must copy key, since if it is leaf page and it is the last key in the page,
		// page will be resized and last key content will be overwritten with STL internal data
		// if it is not the last key, it will be overwritten with the next key after page's remove() method is completed
		key found = p.objects[found_pos];

		BH_LOG(m_log, INDEXES_LOG_NOTICE, "index: remove: %s: page: %s -> %s, found_pos: %d, found_key: %s",
			obj.str().c_str(),
			page_key.str().c_str(), p.str().c_str(),
			found_pos, found.str().c_str());

		if (p.is_leaf() || rec.removed) {
			if (p.is_leaf())
				p.remove(found_pos);
			m_meta.num_keys--;
		} else {
			err = remove(found.url, obj, rec);
			if (err)
				return err;

			// we have removed key from the underlying page, and the first key of that page hasn't been changed
			if (!rec.page_start)
				return elliptics::error_info();

			key found = p.objects[found_pos];

			// the first key of the underlying page has been changed, update appropriate key in the current page
			// @url must be saved, but @id and @timestmap have to be copied, since they have been changed
			found.id = rec.page_start.id;
			found.timestamp = rec.page_start.timestamp;

			p.objects[found_pos] = found;
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
				rec.page_start = p.objects.front();
			}

			err = check(io::write(m_bp, page_key, p.save(), default_reserve_size, 0));
			if (err)
				return err;
		} else {
			// if current page is empty, we have to remove appropriate link from the higher page
			rec.removed = true;

			err = check(io::remove(m_bp, page_key));
			if (err)
				return err;

			m_meta.num_pages--;
			if (p.is_leaf())
				m_meta.num_leaf_pages--;
		}

		return elliptics::error_info();
	}

	elliptics::error_info check(elliptics::async_remove_result &&wr) {
		std::vector<int> groups;
		std::ostringstream st;

		st << "errors: [";
		for (auto r = wr.begin(), end = wr.end(); r != end; ++r) {
			if (!r->error()) {
				groups.push_back(r->command()->id.group_id);
			} else {
				st << "err: " << r->error().message() << ", code: " << r->error().code() << ";";
			}
		}

		if (groups.empty())
			return elliptics::create_error(-ENOENT,
					"remove checker: there are no writeable groups, last error: %s",
					st.str().c_str());

		return elliptics::error_info();
	}

	elliptics::error_info check(elliptics::async_write_result &&wr) {
		std::vector<int> groups;
		std::ostringstream st;

		st << "errors: [";
		for (auto r = wr.begin(), end = wr.end(); r != end; ++r) {
			if (!r->error()) {
				groups.push_back(r->command()->id.group_id);
			} else {
				st << "err: " << r->error().message() << ", code: " << r->error().code() << ";";
			}
		}

		if (groups.empty())
			return elliptics::create_error(-ENOENT,
					"write checker: there are no writeable groups, last error: %s",
					st.str().c_str());

		return elliptics::error_info();
	}
};

class read_only_index: public index {
public:
	read_only_index(ebucket::bucket_processor &bp, const eurl &start): index(bp, start, true) {}
};

class read_write_index: public index {
public:
	read_write_index(ebucket::bucket_processor &bp, const eurl &start): index(bp, start, false) {}
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
	case ioremap::greylock::index_meta::serialization_version_6: {
		if (size != ioremap::greylock::index_meta::serialization_version_6) {
			std::ostringstream ss;
			ss << "page unpack: array size mismatch: read: " << size <<
				", must be: " << ioremap::greylock::index_meta::serialization_version_6;
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
		meta.generation_number_sec = tmp;

		p[5].convert(&tmp);
		meta.generation_number_nsec = tmp;
		break;
	}
	default: {
		std::ostringstream ss;
		ss << "page unpack: version mismatch: read: " << version <<
			", there is no such packing version ";
		throw std::runtime_error(ss.str());
	}
	}

	return meta;
}

template <typename Stream>
inline msgpack::packer<Stream> &operator <<(msgpack::packer<Stream> &o, const ioremap::greylock::index_meta &meta)
{
	o.pack_array(ioremap::greylock::index_meta::serialization_version_6);
	o.pack((int)ioremap::greylock::index_meta::serialization_version_6);
	o.pack(meta.page_index.load());
	o.pack(meta.num_pages.load());
	o.pack(meta.num_leaf_pages.load());
	o.pack(meta.generation_number_sec.load());
	o.pack(meta.generation_number_nsec.load());

	return o;
}

} // namespace msgpack


#endif // __INDEXES_INDEX_HPP
