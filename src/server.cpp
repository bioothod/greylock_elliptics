#include "greylock/bucket.hpp"
#include "greylock/bucket_transport.hpp"
#include "greylock/core.hpp"
#include "greylock/index.hpp"
#include "greylock/intersection.hpp"


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

class vector_lock {
public:
	vector_lock(int num = 1024) {
		resize(num);
	}

	void resize(int num) {
		std::unique_lock<std::mutex> lock(m_sync_lock);

		if (m_lock_num != num) {
			// must wait for every lock to unlock
			// otherwise different thread will unlock something we have freed
			m_sync.wait(lock, [&] {return m_lockers == 0;});

			if (num > m_lock_num) {
				std::vector<std::mutex> locks(num);
				m_locks.swap(locks);
			}

			m_lock_num = num;
		}
	}

	void lock(const std::string &key) {
		size_t h = std::hash<std::string>()(key);
		std::unique_lock<std::mutex> lock(m_sync_lock);
		m_locks[h % m_lock_num].lock();
		m_lockers++;
	}

	bool try_lock(const std::string &key) {
		size_t h = std::hash<std::string>()(key);
		std::unique_lock<std::mutex> lock(m_sync_lock);
		if (m_locks[h % m_lock_num].try_lock()) {
			m_lockers++;
			return true;
		}

		return false;
	}

	void unlock(const std::string &key) {
		size_t h = std::hash<std::string>()(key);
		{
			std::unique_lock<std::mutex> lock(m_sync_lock);
			m_locks[h % m_lock_num].unlock();
			m_lockers--;
		}

		m_sync.notify_all();
	}

private:
	std::mutex m_sync_lock;
	std::condition_variable m_sync;

	int m_lock_num = 0;
	int m_lockers = 0;
	std::vector<std::mutex> m_locks;
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

			std::vector<greylock::key> result;
			bool completed = true;


			ILOG_INFO("url: %s: starting intersection, json parsing duration: %d ms",
					req.url().to_human_readable().c_str(), search_tm.elapsed());

			completed = intersect(req, raw_greylock, page_start, page_num, result);

			JsonValue ret;
			auto &allocator = ret.GetAllocator();

			rapidjson::Value ids(rapidjson::kArrayType);
			for (auto it = result.begin(), end = result.end(); it != end; ++it) {
				rapidjson::Value key(rapidjson::kObjectType);

				rapidjson::Value kv(it->url.key.c_str(), it->url.key.size(), allocator);
				key.AddMember("key", kv, allocator);

				rapidjson::Value bv(it->url.bucket.c_str(), it->url.bucket.size(), allocator);
				key.AddMember("bucket", bv, allocator);

				rapidjson::Value idv(it->id.c_str(), it->id.size(), allocator);
				key.AddMember("id", idv, allocator);

				ids.PushBack(key, allocator);
			}

			ret.AddMember("ids", ids, allocator);
			ret.AddMember("completed", completed, allocator);

			rapidjson::Value page(rapidjson::kObjectType);
			page.AddMember("num", result.size(), allocator);

			rapidjson::Value sv(page_start.c_str(), page_start.size(), allocator);
			page.AddMember("start", sv, allocator);

			ret.AddMember("paging", page, allocator);

			data = ret.ToString();

			thevoid::http_response reply;
			reply.set_code(swarm::http_response::ok);
			reply.headers().set_content_type("text/json; charset=utf-8");
			reply.headers().set_content_length(data.size());

			ILOG_INFO("url: %s: found ids: %d, requested ids: %d, start: %s, completed: %d, duration: %d ms",
					req.url().to_human_readable().c_str(),
					ids.Size(), page_num, page_start.c_str(), completed,
					search_tm.elapsed());

			this->send_reply(std::move(reply), std::move(data));
		}

		bool intersect(const thevoid::http_request &req, const std::vector<greylock::eurl> &raw_greylock,
				std::string &page_start, int page_num, std::vector<greylock::key> &result) {
			ribosome::timer tm;

			greylock::intersect::intersector<greylock::bucket_transport> p(*(server()->bucket()));

			std::vector<locker<http_server>> lockers;
			// this reserve is absolutely needed, since elements of vector of unique locks
			// use references to this array to grab locker's state
			lockers.reserve(raw_greylock.size());

			std::vector<std::unique_lock<locker<http_server>>> locks;
			locks.reserve(raw_greylock.size());
			for (auto it = raw_greylock.begin(), end = raw_greylock.end(); it != end; ++it) {
				locker<http_server> l(server(), it->key);
				lockers.emplace_back(std::move(l));

				std::unique_lock<locker<http_server>> lk(lockers.back());
				locks.emplace_back(std::move(lk));
			}

			ILOG_INFO("url: %s: locks: %d: intersection locked: duration: %d ms",
					req.url().to_human_readable().c_str(),
					raw_greylock.size(),
					tm.elapsed());

			ribosome::timer intersect_tm;
			greylock::intersect::result res = p.intersect(raw_greylock, page_start, page_num);

			if (res.keys.size()) {
				auto &v = res.keys.begin()->second;
				result.swap(v);
			}

			ILOG_INFO("url: %s: locks: %d: completed: %d, result keys: %d, requested num: %d, page start: %s: "
					"intersection completed: duration: %d ms, whole duration: %d ms",
					req.url().to_human_readable().c_str(),
					raw_greylock.size(), res.completed, result.size(),
					page_num, page_start.c_str(),
					intersect_tm.elapsed(), tm.elapsed());

			return res.completed;
		}
	};

	struct on_index : public thevoid::simple_request_stream<http_server> {
		virtual void on_request(const thevoid::http_request &req, const boost::asio::const_buffer &buffer) {
			ribosome::timer index_tm;
			ILOG_INFO("url: %s: start", req.url().to_human_readable().c_str());

			// this is needed to put ending zero-byte, otherwise rapidjson parser will explode
			std::string data(const_cast<char *>(boost::asio::buffer_cast<const char*>(buffer)), boost::asio::buffer_size(buffer));

			rapidjson::Document doc;
			doc.Parse<0>(data.c_str());

			if (!doc.HasMember("ids")) {
				ILOG_ERROR("url: %s, error: %d: there is no 'ids' member", req.url().to_human_readable().c_str(), -EINVAL);
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			if (!doc.HasMember("indexes")) {
				ILOG_ERROR("url: %s, error: %d: there is no 'indexes' member", req.url().to_human_readable().c_str(), -EINVAL);
				this->send_reply(swarm::http_response::bad_request);
				return;
			}


			const auto &ids = doc["ids"];
			const auto &idxs = doc["indexes"];

			if (!ids.IsArray()) {
				ILOG_ERROR("url: %s, error: %d: 'ids' must be array", req.url().to_human_readable().c_str(), -EINVAL);
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			if (!idxs.IsArray()) {
				ILOG_ERROR("url: %s, error: %d: 'indexes' must be array", req.url().to_human_readable().c_str(), -EINVAL);
				this->send_reply(swarm::http_response::bad_request);
				return;
			}


			std::vector<greylock::key> keys;
			for (auto it = ids.Begin(), id_end = ids.End(); it != id_end; ++it) {
				if (it->IsObject()) {
					greylock::key k;

					if (!it->HasMember("key"))
						continue;
					if (!it->HasMember("bucket"))
						continue;
					if (!it->HasMember("id"))
						continue;

					const auto &jid = (*it)["id"];
					const auto &jb = (*it)["bucket"];
					const auto &jk = (*it)["key"];

					if (!jid.IsString() || !jb.IsString() || !jk.IsString())
						continue;

					k.url.bucket = jb.GetString();
					k.url.key = jk.GetString();
					k.id = jid.GetString();

					keys.emplace_back(k);
				}
			}

			ILOG_INFO("url: %s, keys: %d, indexes: %d: start insertion",
					req.url().to_human_readable().c_str(),
					keys.size(), idxs.Size());

			ribosome::timer all_tm;
			int idx_pos = 0;
			for (auto idx = idxs.Begin(), idx_end = idxs.End(); idx != idx_end; ++idx) {
				all_tm.restart();

				if (!idx->IsString())
					continue;

				greylock::eurl start;
				start.bucket = server()->meta_bucket_name();
				start.key = idx->GetString();

				locker<http_server> l(server(), start.key);
				std::unique_lock<locker<http_server>> lk(l);

				greylock::index<greylock::bucket_transport> index(*(server()->bucket()), start);

				ribosome::timer tm;

				int pos = 0;
				for (auto it = keys.begin(), end = keys.end(); it != end; ++it) {
					tm.restart();

					int err = index.insert(*it);
					if (err < 0) {
						ILOG_ERROR("url: %s, index: %s, key: %s, error: %d: could not insert new key",
							req.url().to_human_readable().c_str(), start.str().c_str(),
							it->str().c_str(), err);
						this->send_reply(swarm::http_response::internal_server_error);
						return;
					}

					ILOG_INFO("url: %s, index: %s, keys: %d/%d, key: %s: inserted new key, duration: %d ms",
						req.url().to_human_readable().c_str(), start.str().c_str(),
						pos, keys.size(),
						it->str().c_str(), tm.elapsed());

					pos++;
				}

				ILOG_INFO("url: %s, index: %s, keys: %d, idx: %d/%d: inserted all keys, duration: %d ms",
					req.url().to_human_readable().c_str(), start.str().c_str(),
					keys.size(), idx_pos, idxs.Size(), all_tm.elapsed());

				idx_pos++;
			}

			ILOG_INFO("url: %s, keys: %d, indexes: %d: completed, index duration: %d ms",
					req.url().to_human_readable().c_str(),
					keys.size(), idxs.Size(), index_tm.elapsed());
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
			ILOG_ERROR("Could not add any out of %zd nodes.", remotes.size());
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
		if (config.HasMember("lock-num")) {
			auto &ln = config["lock-num"];
			if (ln.IsNumber()) {
				m_lock.resize(ln.GetInt());
			}
		}

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

