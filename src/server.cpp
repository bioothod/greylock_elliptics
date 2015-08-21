#include "greylock/bucket.hpp"
#include "greylock/bucket_transport.hpp"
#include "greylock/core.hpp"
#include "greylock/index.hpp"
#include "greylock/intersection.hpp"
#include "greylock/json.hpp"


#include <elliptics/session.hpp>

#include <unistd.h>
#include <signal.h>
#include <time.h>

#include <thevoid/server.hpp>
#include <thevoid/stream.hpp>

#include <thevoid/rapidjson/stringbuffer.h>
#include <thevoid/rapidjson/prettywriter.h>
#include <thevoid/rapidjson/document.h>

#include <ribosome/timer.hpp>

#include <swarm/logger.hpp>

#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

#define ILOG(level, a...) BH_LOG(logger(), level, ##a)
#define ILOG_ERROR(a...) ILOG(SWARM_LOG_ERROR, ##a)
#define ILOG_WARNING(a...) ILOG(SWARM_LOG_WARNING, ##a)
#define ILOG_INFO(a...) ILOG(SWARM_LOG_INFO, ##a)
#define ILOG_NOTICE(a...) ILOG(SWARM_LOG_NOTICE, ##a)
#define ILOG_DEBUG(a...) ILOG(SWARM_LOG_DEBUG, ##a)

using namespace ioremap;

struct lock_entry {
	lock_entry(bool l): locked(l) {}
	std::condition_variable cond;
	int waiting = 0;
	bool locked = false;
};

typedef std::unique_ptr<lock_entry> lock_entry_ptr;
static inline lock_entry_ptr new_lock_entry_ptr(bool l) {
	return std::unique_ptr<lock_entry>(new lock_entry(l));
}

class vector_lock {
public:
	vector_lock() {
	}

	void lock(const std::string &key) {
		std::unique_lock<std::mutex> lock(m_sync_lock);
		auto it = m_locks.find(key);
		if (it == m_locks.end()) {
			m_locks.emplace(std::pair<std::string, lock_entry_ptr>(key, new_lock_entry_ptr(true)));
			return;
		}

		lock_entry_ptr &ptr = it->second;

		ptr->waiting++;
		ptr->cond.wait(lock, [&] { return ptr->locked == false; });
		ptr->locked = true;
		ptr->waiting--;
	}

	bool try_lock(const std::string &key) {
		std::unique_lock<std::mutex> lock(m_sync_lock);
		auto it = m_locks.find(key);
		if (it == m_locks.end()) {
			m_locks.emplace(std::pair<std::string, lock_entry_ptr>(key, new_lock_entry_ptr(true)));
			return true;
		}

		return false;
	}

	void unlock(const std::string &key) {
		std::unique_lock<std::mutex> lock(m_sync_lock);
		auto it = m_locks.find(key);
		if (it == m_locks.end()) {
			throw std::runtime_error(key + ": trying to unlock key which is not locked");
		}

		lock_entry_ptr &ptr = it->second;

		ptr->locked = false;
		if (ptr->waiting != 0) {
			ptr->cond.notify_one();
			return;
		}

		m_locks.erase(it);
	}

private:
	std::mutex m_sync_lock;
	std::map<std::string, lock_entry_ptr> m_locks;
};

template <typename T>
class locker {
public:
	locker(T *t, const std::string &key) : m_t(t), m_key(key) {
	}
	locker(const locker &other) {
		m_key = other.m_key;
		m_t = other.m_t;
	}
	locker(locker &&other) {
		m_key = other.m_key;
		m_t = other.m_t;
	}
	~locker() {
	}

	void lock() {
		m_t->lock(m_key);
	}

	bool try_lock(const std::string &key) {
		return m_t->try_lock(m_key);
	}

	void unlock() {
		m_t->unlock(m_key);
	}

private:
	T *m_t;
	std::string m_key;
};

class JsonValue : public rapidjson::Value
{
public:
	JsonValue() {
		SetObject();
	}

	~JsonValue() {
	}

	static void set_time(rapidjson::Value &obj, rapidjson::Document::AllocatorType &alloc, long tsec, long usec) {
		char str[64];
		struct tm tm;

		localtime_r((time_t *)&tsec, &tm);
		strftime(str, sizeof(str), "%F %Z %R:%S", &tm);

		char time_str[128];
		snprintf(time_str, sizeof(time_str), "%s.%06lu", str, usec);

		obj.SetObject();

		rapidjson::Value tobj(time_str, strlen(time_str), alloc);
		obj.AddMember("time", tobj, alloc);

		std::string raw_time = elliptics::lexical_cast(tsec) + "." + elliptics::lexical_cast(usec);
		rapidjson::Value tobj_raw(raw_time.c_str(), raw_time.size(), alloc);
		obj.AddMember("time-raw", tobj_raw, alloc);
	}

	std::string ToString() const {
		rapidjson::StringBuffer buffer;
		rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);

		Accept(writer);
		buffer.Put('\n');

		return std::string(buffer.GetString(), buffer.Size());
	}

	rapidjson::MemoryPoolAllocator<> &GetAllocator() {
		return m_allocator;
	}

private:
	rapidjson::MemoryPoolAllocator<> m_allocator;
};

class http_server : public thevoid::server<http_server>
{
public:
	virtual bool initialize(const rapidjson::Value &config) {
		if (!elliptics_init(config))
			return false;

		if (!greylock_init(config))
			return false;

		on<on_ping>(
			options::exact_match("/ping"),
			options::methods("GET")
		);

		on<on_index>(
			options::exact_match("/index"),
			options::methods("POST")
		);

		on<on_search>(
			options::exact_match("/search"),
			options::methods("POST")
		);

		return true;
	}

	struct on_ping : public thevoid::simple_request_stream<http_server> {
		virtual void on_request(const thevoid::http_request &req, const boost::asio::const_buffer &buffer) {
			(void) buffer;
			(void) req;

			this->send_reply(thevoid::http_response::ok);
		}
	};

	struct on_search : public thevoid::simple_request_stream<http_server> {
		virtual void on_request(const thevoid::http_request &req, const boost::asio::const_buffer &buffer) {
			ribosome::timer search_tm;
			ILOG_INFO("url: %s: start", req.url().to_human_readable().c_str());

			// this is needed to put ending zero-byte, otherwise rapidjson parser will explode
			std::string data(const_cast<char *>(boost::asio::buffer_cast<const char*>(buffer)), boost::asio::buffer_size(buffer));

			rapidjson::Document doc;
			doc.Parse<0>(data.c_str());

			if (!doc.HasMember("indexes")) {
				ILOG_ERROR("url: %s, error: %d: there is no 'indexes' member", req.url().to_human_readable().c_str(), -EINVAL);
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			const auto &idxs = doc["indexes"];

			if (!idxs.IsArray()) {
				ILOG_ERROR("url: %s, error: %d: 'indexes' must be array", req.url().to_human_readable().c_str(), -EINVAL);
				this->send_reply(swarm::http_response::bad_request);
				return;
			}


			size_t page_num = ~0U;
			std::string page_start("\0");

			if (doc.HasMember("paging")) {
				const auto &pages = doc["paging"];
				if (pages.HasMember("num")) {
					const auto &n = pages["num"];
					if (n.IsNumber())
						page_num = n.GetUint64();
				}

				if (pages.HasMember("start")) {
					const auto &s = pages["start"];
					if (s.IsString())
						page_start = s.GetString();
				}
			}

			std::set<greylock::eurl> raw_greylock_set;
			for (auto idx = idxs.Begin(), idx_end = idxs.End(); idx != idx_end; ++idx) {
				if (!idx->IsString())
					continue;

				greylock::eurl start;
				start.bucket = server()->meta_bucket_name();
				start.key = idx->GetString();

				raw_greylock_set.emplace(start);
			}

			std::vector<greylock::eurl> raw_greylock(raw_greylock_set.begin(), raw_greylock_set.end());

			greylock::intersect::result result;
			result.cookie = page_start;
			result.max_number_of_documents = page_num;

			ILOG_INFO("url: %s: starting intersection, json parsing duration: %d ms",
					req.url().to_human_readable().c_str(), search_tm.elapsed());

			intersect(req, raw_greylock, result);

			const rapidjson::Value &match = greylock::get_object(doc, "match");
			if (match.IsObject()) {
				const char *match_type = greylock::get_string(match, "type");
				if (match_type) {
					if (strcmp(match_type, "and")) {
					} else if (strcmp(match_type, "phrase")) {
					}
				}
			}

			send_search_result(result);

			ILOG_INFO("url: %s: requested indexes: %d, requested number of documents: %d, search start: %s, "
					"found documents: %d, cookie: %s, completed: %d, duration: %d ms",
					req.url().to_human_readable().c_str(), idxs.Size(), page_num, page_start.c_str(),
					result.docs.size(), result.cookie.c_str(), result.completed, search_tm.elapsed());
		}

		void send_search_result(const greylock::intersect::result &result) {
			JsonValue ret;
			auto &allocator = ret.GetAllocator();

			rapidjson::Value ids(rapidjson::kArrayType);
			for (auto it = result.docs.begin(), end = result.docs.end(); it != end; ++it) {
				rapidjson::Value key(rapidjson::kObjectType);

				const greylock::key &doc = it->doc;

				rapidjson::Value kv(doc.url.key.c_str(), doc.url.key.size(), allocator);
				key.AddMember("key", kv, allocator);

				rapidjson::Value bv(doc.url.bucket.c_str(), doc.url.bucket.size(), allocator);
				key.AddMember("bucket", bv, allocator);

				rapidjson::Value idv(doc.id.c_str(), doc.id.size(), allocator);
				key.AddMember("id", idv, allocator);

				ids.PushBack(key, allocator);
			}

			ret.AddMember("ids", ids, allocator);
			ret.AddMember("completed", result.completed, allocator);

			{
				rapidjson::Value page(rapidjson::kObjectType);
				page.AddMember("num", result.docs.size(), allocator);

				rapidjson::Value sv(result.cookie.c_str(), result.cookie.size(), allocator);
				page.AddMember("start", sv, allocator);

				ret.AddMember("paging", page, allocator);
			}

			std::string data = ret.ToString();

			thevoid::http_response reply;
			reply.set_code(swarm::http_response::ok);
			reply.headers().set_content_type("text/json; charset=utf-8");
			reply.headers().set_content_length(data.size());

			this->send_reply(std::move(reply), std::move(data));
		}

		bool intersect(const thevoid::http_request &req, const std::vector<greylock::eurl> &raw_greylock,
				greylock::intersect::result &result) {
			ribosome::timer tm;

			greylock::intersect::intersector<greylock::bucket_transport> p(*(server()->bucket()));

			std::vector<locker<http_server>> lockers;
			// this reserve is absolutely needed, since elements of vector of unique locks
			// use references to this array to grab locker's state
			lockers.reserve(raw_greylock.size());

			std::vector<std::unique_lock<locker<http_server>>> locks;
			locks.reserve(raw_greylock.size());
			for (auto it = raw_greylock.begin(), end = raw_greylock.end(); it != end; ++it) {
				locker<http_server> l(server(), it->str());
				lockers.emplace_back(std::move(l));

				std::unique_lock<locker<http_server>> lk(lockers.back());
				locks.emplace_back(std::move(lk));
			}

			ILOG_INFO("url: %s: locks: %d: intersection locked: duration: %d ms",
					req.url().to_human_readable().c_str(),
					raw_greylock.size(),
					tm.elapsed());

			ribosome::timer intersect_tm;
			result = p.intersect(raw_greylock, result.cookie, result.max_number_of_documents);

			ILOG_INFO("url: %s: locks: %d: completed: %d, result keys: %d, requested num: %d, page start: %s: "
					"intersection completed: duration: %d ms, whole duration: %d ms",
					req.url().to_human_readable().c_str(),
					raw_greylock.size(), result.completed, result.docs.size(),
					result.max_number_of_documents, result.cookie.c_str(),
					intersect_tm.elapsed(), tm.elapsed());

			return result.completed;
		}
	};

	struct on_index : public thevoid::simple_request_stream<http_server> {
		void insert_one_key(const thevoid::http_request &req, greylock::key &doc, const rapidjson::Value &idxs) {
			ribosome::timer all_tm;

			ILOG_INFO("insert_one_key: url: %s, doc: %s, indexes: %d: start insertion",
					req.url().to_human_readable().c_str(),
					doc.str().c_str(), idxs.Size());

			int idx_pos = 0;
			for (auto idx = idxs.Begin(), idx_end = idxs.End(); idx != idx_end; ++idx) {
				ribosome::timer tm;

				if (!idx->IsObject())
					continue;

				// for every index we put vector of positions where given index is located in the document
				// since it is an inverted index, it contains list of document links each of which contains
				// array of the positions, where given index lives in the document
				doc.positions.clear();
				const rapidjson::Value &pos = greylock::get_array(*idx, "positions");
				if (pos.IsArray()) {
					for (auto p = pos.Begin(), pend = pos.End(); p != pend; ++p) {
						if (p->IsInt()) {
							doc.positions.push_back(p->GetInt());
						} else if (p->IsUint()) {
							doc.positions.push_back(p->GetUint());
						} else if (p->IsInt64()) {
							doc.positions.push_back(p->GetInt64());
						} else if (p->IsUint64()) {
							doc.positions.push_back(p->GetUint64());
						}
					}
				}

				const char *name = greylock::get_string(*idx, "name");
				if (!name)
					continue;

				greylock::eurl start;
				start.bucket.assign(server()->meta_bucket_name());
				start.key.assign(name);

				locker<http_server> l(server(), start.str());
				std::unique_lock<locker<http_server>> lk(l);

				greylock::read_write_index<greylock::bucket_transport> index(*(server()->bucket()), start);

				int err = index.insert(doc);
				if (err < 0) {
					ILOG_ERROR("insert_one_key: url: %s, doc: %s, index: %s error: %d: could not insert new key",
						req.url().to_human_readable().c_str(),
						doc.str().c_str(),
						start.str().c_str(),
						err);
					this->send_reply(swarm::http_response::internal_server_error);
					return;
				}

				ILOG_INFO("insert_one_key: url: %s, doc: %s, index: %s, index position: %d/%d, elapsed time: %d ms",
					req.url().to_human_readable().c_str(),
					doc.str().c_str(),
					start.str().c_str(),
					idx_pos, idxs.Size(),
					tm.elapsed());

				idx_pos++;
			}

			ILOG_INFO("insert_one_key: url: %s, doc: %s, indexes: %d: successfully inserded, elapsed time: %d ms",
					req.url().to_human_readable().c_str(),
					doc.str().c_str(), idxs.Size(), all_tm.elapsed());
		}

		void parse_ids(const thevoid::http_request &req, const rapidjson::Value &ids) {
			for (auto it = ids.Begin(), id_end = ids.End(); it != id_end; ++it) {
				if (it->IsObject()) {
					greylock::key doc;

					const char *bucket = greylock::get_string(*it, "bucket");
					const char *key = greylock::get_string(*it, "key");
					const char *id = greylock::get_string(*it, "id");
					if (!bucket || !key || !id) {
						ILOG_ERROR("parse_ids: url: %s, error: %d: 'ids/{bucket,ket,id} must be strings",
								req.url().to_human_readable().c_str(), -EINVAL);
						continue;
					}

					doc.url.bucket.assign(bucket);
					doc.url.key.assign(key);
					doc.id.assign(id);

					const rapidjson::Value &idxs = greylock::get_array(*it, "indexes");
					if (!idxs.IsArray()) {
						ILOG_ERROR("parse_ids: url: %s, error: %d: 'ids/indexes' must be array",
								req.url().to_human_readable().c_str(), -EINVAL);
						continue;
					}

					insert_one_key(req, doc, idxs);
				}
			}
		}

		virtual void on_request(const thevoid::http_request &req, const boost::asio::const_buffer &buffer) {
			ribosome::timer index_tm;
			ILOG_INFO("url: %s: start", req.url().to_human_readable().c_str());

			// this is needed to put ending zero-byte, otherwise rapidjson parser will explode
			std::string data(const_cast<char *>(boost::asio::buffer_cast<const char*>(buffer)), boost::asio::buffer_size(buffer));

			rapidjson::Document doc;
			doc.Parse<0>(data.c_str());

			const rapidjson::Value &ids = greylock::get_array(doc, "ids");
			if (!ids.IsArray()) {
				ILOG_ERROR("on_request: url: %s, error: %d: 'ids' must be array",
						req.url().to_human_readable().c_str(), -EINVAL);
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			parse_ids(req, ids);

			ILOG_INFO("on_request: url: %s, keys: %d: insertion completed, index duration: %d ms",
					req.url().to_human_readable().c_str(),
					ids.Size(), index_tm.elapsed());
			this->send_reply(thevoid::http_response::ok);
		}
	};

	std::shared_ptr<greylock::bucket_transport> bucket() {
		return m_bucket;
	}

	void lock(const std::string &key) {
		m_lock.lock(key);
	}

	void unlock(const std::string &key) {
		m_lock.unlock(key);
	}

	const std::string &meta_bucket_name() const {
		return m_meta_bucket;
	}

private:
	vector_lock m_lock;

	std::shared_ptr<elliptics::node> m_node;

	std::string m_meta_bucket;
	std::shared_ptr<greylock::bucket_transport> m_bucket;

	long m_read_timeout = 60;
	long m_write_timeout = 60;

	bool elliptics_init(const rapidjson::Value &config) {
		dnet_config node_config;
		memset(&node_config, 0, sizeof(node_config));

		if (!prepare_config(config, node_config)) {
			return false;
		}

		m_node.reset(new elliptics::node(std::move(swarm::logger(this->logger(), blackhole::log::attributes_t())), node_config));

		if (!prepare_node(config, *m_node)) {
			return false;
		}

		m_bucket.reset(new greylock::bucket_transport(m_node));

		if (!prepare_session(config)) {
			return false;
		}

		if (!prepare_buckets(config))
			return false;

		return true;
	}

	bool prepare_config(const rapidjson::Value &config, dnet_config &node_config) {
		if (config.HasMember("io-thread-num")) {
			node_config.io_thread_num = config["io-thread-num"].GetInt();
		}
		if (config.HasMember("nonblocking-io-thread-num")) {
			node_config.nonblocking_io_thread_num = config["nonblocking-io-thread-num"].GetInt();
		}
		if (config.HasMember("net-thread-num")) {
			node_config.net_thread_num = config["net-thread-num"].GetInt();
		}

		return true;
	}

	bool prepare_node(const rapidjson::Value &config, elliptics::node &node) {
		if (!config.HasMember("remotes")) {
			ILOG_ERROR("\"application.remotes\" field is missed");
			return false;
		}

		std::vector<elliptics::address> remotes;

		auto &remotesArray = config["remotes"];
		for (auto it = remotesArray.Begin(), end = remotesArray.End(); it != end; ++it) {
				if (it->IsString()) {
					remotes.push_back(it->GetString());
				}
		}

		try {
			node.add_remote(remotes);
			elliptics::session session(node);

			if (!session.get_routes().size()) {
				ILOG_ERROR("Didn't add any remote node, exiting.");
				return false;
			}
		} catch (const std::exception &e) {
			ILOG_ERROR("Could not add any out of %d nodes.", remotes.size());
			return false;
		}

		return true;
	}

	bool prepare_session(const rapidjson::Value &config) {
		if (config.HasMember("read-timeout")) {
			auto &tm = config["read-timeout"];
			if (tm.IsInt())
				m_read_timeout = tm.GetInt();
		}

		if (config.HasMember("write-timeout")) {
			auto &tm = config["write-timeout"];
			if (tm.IsInt())
				m_write_timeout = tm.GetInt();
		}

		return true;
	}

	bool prepare_buckets(const rapidjson::Value &config) {
		if (!config.HasMember("buckets")) {
			ILOG_ERROR("\"application.buckets\" field is missed");
			return false;
		}

		auto &buckets = config["buckets"];

		std::set<std::string> bnames;
		for (auto it = buckets.Begin(), end = buckets.End(); it != end; ++it) {
			if (it->IsString()) {
				bnames.insert(it->GetString());
			}
		}

		if (!config.HasMember("meta-bucket")) {
			ILOG_ERROR("\"application.meta-bucket\" field is missed");
			return false;
		}

		// XXX there should be no meta bucket.
		// XXX meta bucket was created to host all index starts (i.e. index metadata and the first page)
		// XXX but it should be dynamically allocated just like all other pages
		// XXX instead of using @m_meta_bucket/@meta_bucket_name() server should grab
		// XXX new bucket via @get_bucket(), and client has to provide bucket name in the index methods.
		auto &meta_bucket = config["meta-bucket"];
		if (!meta_bucket.IsString()) {
			ILOG_ERROR("\"application.meta-bucket\" must be string");
			return false;
		}
		m_meta_bucket = meta_bucket.GetString();
		bnames.insert(m_meta_bucket);


		if (!config.HasMember("metadata-groups")) {
			ILOG_ERROR("\"application.metadata-groups\" field is missed");
			return false;
		}

		std::vector<int> mgroups;
		auto &groups_meta_array = config["metadata-groups"];
		for (auto it = groups_meta_array.Begin(), end = groups_meta_array.End(); it != end; ++it) {
			if (it->IsInt())
				mgroups.push_back(it->GetInt());
		}

		if (!m_bucket->init(mgroups, std::vector<std::string>(bnames.begin(), bnames.end())))
			return false;

		return true;
	}

	bool greylock_init(const rapidjson::Value &config) {
		if (config.HasMember("max-page-size")) {
			auto &ps = config["max-page-size"];
			if (ps.IsNumber())
				ioremap::greylock::max_page_size = ps.GetInt();
		}

		if (config.HasMember("reserve-size")) {
			auto &ps = config["reserve-size"];
			if (ps.IsNumber())
				ioremap::greylock::default_reserve_size = ps.GetInt();
		}

		return true;
	}
};

int main(int argc, char **argv)
{
	ioremap::thevoid::register_signal_handler(SIGINT, ioremap::thevoid::handle_stop_signal);
	ioremap::thevoid::register_signal_handler(SIGTERM, ioremap::thevoid::handle_stop_signal);
	ioremap::thevoid::register_signal_handler(SIGHUP, ioremap::thevoid::handle_reload_signal);
	ioremap::thevoid::register_signal_handler(SIGUSR1, ioremap::thevoid::handle_ignore_signal);
	ioremap::thevoid::register_signal_handler(SIGUSR2, ioremap::thevoid::handle_ignore_signal);

	ioremap::thevoid::run_signal_thread();

	auto server = ioremap::thevoid::create_server<http_server>();
	int err = server->run(argc, argv);

	ioremap::thevoid::stop_signal_thread();

	return err;
}

