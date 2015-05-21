#include <iostream>
#include <iterator>
#include <string>
#include <vector>

#include <stdint.h>

#include <elliptics/session.hpp>

#include <msgpack.hpp>

#include <boost/program_options.hpp>

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

	MSGPACK_DEFINE(flags, objects, total_size);

	page(bool leaf = false) {
		if (leaf) {
			flags = PAGE_LEAF;
		}
	}

	bool is_empty() const {
		return objects.size() == 0;
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

	key search_leaf(const key &obj) const {
		if (!is_leaf()) {
			return key();
		}

		for (auto it = objects.begin(); it != objects.end(); ++it) {
			if (obj.id == it->id) {
				return *it;
			}
		}

		return key();
	}

	bool replace(const key &src, const key &dst) {
		for (auto it = objects.begin(); it != objects.end(); ++it) {
			if (src.id == it->id) {
				printf("p: %s: replace: %s -> %s\n", str().c_str(), it->str().c_str(), dst.str().c_str());
				it->id = dst.id;
				return true;
			}
		}

		printf("p: %s: replace failed: %s -> %s\n", str().c_str(), src.str().c_str(), dst.str().c_str());
		return false;
	}

	key search_node(const key &obj) const {
		if (objects.size() == 0)
			return key();

		if (is_leaf()) {
			return search_leaf(obj);
		}

		key prev = objects.front();

		for (auto it = objects.begin(); it != objects.end(); ++it) {
			if (obj.id < it->id) {
				return prev;
			}

			prev = *it;
		}

		return objects.back();
	}

	bool insert_and_split(const key &obj, struct page &other) {
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

struct index_meta {
	int page_index = 0;

	MSGPACK_DEFINE(page_index);
};

struct recursion {
	key page_start;
	key split_key;
};

template <typename T>
class index {
public:
	index(T &t, const std::string &sk): m_t(t), m_sk(sk) {
		page start_page;

		try {
			std::string start = m_t.read(m_sk);
		} catch (const std::exception &e) {
			fprintf(stderr, "index: could not read start key: %s\n", e.what());

			m_t.write(m_sk, start_page.save());
		}

		try {
			std::string meta = m_t.read(meta_key());

			msgpack::unpacked result;
			msgpack::unpack(&result, meta.data(), meta.size());
			msgpack::object obj = result.get();
			m_meta = obj.as<index_meta>();
		} catch (const std::exception &e) {
			fprintf(stderr, "index: could not read index metadata: %s\n", e.what());

			std::stringstream ss;
			msgpack::pack(ss, m_meta);
			std::string meta = ss.str();
			m_t.write(meta_key(), meta.data(), meta.size());
		}

		dprintf("index: opened: page_index: %d\n", m_meta.page_index);
	}

	key search(const key &obj) const {
		return search(m_sk, obj);
	}

	void insert(const key &obj) {
		recursion tmp;
		insert(m_sk, obj, tmp);
	}

private:
	T &m_t;
	std::string m_sk;

	index_meta m_meta;

	std::string meta_key() const {
		return m_sk + ".meta";
	}

	key search(const std::string &page_key, const key &obj) const {
		std::string data = m_t.read(page_key);

		page p;
		p.load(data.data(), data.size());

		key found = p.search_node(obj);

		dprintf("search: %s: page: %s -> %s, found: %s\n", obj.str().c_str(), page_key.c_str(), p.str().c_str(), found.str().c_str());

		if (!found)
			return found;

		if (p.is_leaf())
			return found;

		return search(found.value, obj);
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
			key found = p.search_node(obj);
			dprintf("insert: %s: page: %s -> %s, found: %s\n", obj.str().c_str(), page_key.c_str(), p.str().c_str(), found.str().c_str());
			if (!found) {
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

				return;
			}

			insert(found.value, obj, rec);

			dprintf("insert: %s: returned: %s -> %s, found: %s, rec: page_start: %s, split_key: %s\n",
					obj.str().c_str(),
					page_key.c_str(), p.str().c_str(),
					found.str().c_str(),
					rec.page_start.str().c_str(),
					rec.split_key.str().c_str());

			// true if we should not unwind recursion and just return
			// false if either split page has to be written or page changed and has to be written
			bool want_return = true;

			if (found != rec.page_start) {
				p.replace(found, rec.page_start);

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

			dprintf("insert: %s: write split page: %s -> %s, split: key: %s -> %s\n",
					obj.str().c_str(),
					page_key.c_str(), p.str().c_str(),
					rec.split_key.str().c_str(), split.str().c_str());
			m_t.write(rec.split_key.value, split.save());
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

			dprintf("insert: %s: write split page: %s -> %s, old_root_key: %s, new_root: %s\n",
					obj.str().c_str(), page_key.c_str(), p.str().c_str(), old_root_key.str().c_str(), new_root.str().c_str());
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

	std::vector<indexes::key> keys;
	for (int i = 0; i < 100000; ++i) {
		indexes::key k;

		char buf[128];

		snprintf(buf, sizeof(buf), "%08x.%03d", rand(), i);
		k.id = std::string(buf);

		snprintf(buf, sizeof(buf), "value.%03d", i);
		k.value = std::string(buf);

		dprintf("inserting: %s\n", k.str().c_str());
		idx.insert(k);
		keys.push_back(k);
		dprintf("inserted: %s\n\n", k.str().c_str());

		indexes::key found = idx.search(k);
		if (!found) {
			fprintf(stderr, "search failed: could not find key: %s\n", k.id.c_str());
			return -1;
		}

		if (found.id != k.id) {
			fprintf(stderr, "search failed: ID mismatch: found: %s, must be: %s\n", found.id.c_str(), k.id.c_str());
			return -1;
		}
		if (found.value != k.value) {
			fprintf(stderr, "search failed: value mismatch: found: %s, must be: %s\n", found.value.c_str(), k.value.c_str());
			return -1;
		}

		dprintf("search: key: %s, value: %s\n\n", found.id.c_str(), found.value.c_str());
	}

	for (auto it = keys.begin(); it != keys.end(); ++it) {
		indexes::key k;

		k.id = it->id;
		indexes::key found = idx.search(k);
		if (!found) {
			fprintf(stderr, "search failed: could not find key: %s\n", k.id.c_str());
			return -1;
		}

		if (found.id != it->id) {
			fprintf(stderr, "search failed: ID mismatch: found: %s, must be: %s\n", found.id.c_str(), it->id.c_str());
		}
		if (found.value != it->value) {
			fprintf(stderr, "search failed: value mismatch: found: %s, must be: %s\n", found.value.c_str(), it->value.c_str());
		}

		dprintf("search: key: %s, value: %s\n\n", found.id.c_str(), found.value.c_str());
	}
}
