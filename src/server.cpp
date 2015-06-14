#include "indexes/bucket.hpp"
#include "indexes/core.hpp"
#include "indexes/index.hpp"

#include <elliptics/session.hpp>

#include <unistd.h>
#include <signal.h>

#include <thevoid/server.hpp>
#include <thevoid/stream.hpp>

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

class bucket_transport : public indexes::bucket_processor {
public:
	bucket_transport(std::shared_ptr<elliptics::node> node) : bucket_processor(node) {}

	indexes::status read(const indexes::eurl &key) {
		return indexes::bucket_processor::read(key.bucket, key.key);
	}

	std::vector<indexes::status> read_all(const indexes::eurl &key) {
		return indexes::bucket_processor::read_all(key.bucket, key.key);
	}

	std::vector<indexes::status> write(const std::vector<int> groups, const indexes::eurl &key,
			const std::string &data, size_t reserve_size, bool cache) {
		return indexes::bucket_processor::write(groups, key.bucket, key.key, data, reserve_size, cache);
	}

	std::vector<indexes::status> write(const indexes::eurl &key, const std::string &data, bool cache = false) {
		return indexes::bucket_processor::write(key.bucket, key.key, data, indexes::default_reserve_size, cache);
	}

	std::vector<indexes::status> remove(const indexes::eurl &key) {
		return indexes::bucket_processor::remove(key.bucket, key.key);
	}

	// this method is useless for bucket processing,
	// each bucket will continue to work with all its groups,
	// even if some of them are currently unavailable and
	// index class reports this by setting new groups
	void set_groups(const std::vector<int> &groups) {
		(void) groups;
	}

private:
};

class http_server : public thevoid::server<http_server>
{
public:
	virtual bool initialize(const rapidjson::Value &config) {
		if (!elliptics_init(config))
			return false;

		if (!indexes_init(config))
			return false;

		on<on_ping>(
			options::exact_match("/ping"),
			options::methods("GET")
		);

		on<on_index>(
			options::exact_match("/index"),
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

	struct on_index : public thevoid::simple_request_stream<http_server> {
		virtual void on_request(const thevoid::http_request &req, const boost::asio::const_buffer &buffer) {
			(void) req;

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


			std::vector<std::string> raw_indexes;
			std::vector<std::string> raw_ids;

			for (auto it = idxs.Begin(), idx_end = idxs.End(); it != idx_end; ++it) {
				if (it->IsString())
					raw_indexes.push_back(it->GetString());
			}

			for (auto it = ids.Begin(), id_end = ids.End(); it != id_end; ++it) {
				if (it->IsString())
					raw_ids.push_back(it->GetString());
			}


			std::string bucket = "";

			for (auto idx = raw_indexes.begin(), idx_end = raw_indexes.end(); idx != idx_end; ++idx) {
				indexes::eurl start;
				start.bucket = bucket;
				start.key = *idx;

				indexes::index<bucket_transport> index(*(server()->bucket()), start);

				for (auto id = raw_ids.begin(), id_end = raw_ids.end(); id != id_end; ++id) {
					indexes::key k;
					k.url.bucket = bucket;
					k.url.key = *id;
					k.id = *id;

					int err = index.insert(k);
					if (err < 0) {
						ILOG_ERROR("url: %s, index: %s, id: %s, error: %d: could not insert new key",
							req.url().to_human_readable().c_str(), idx->c_str(), id->c_str(), err);
						this->send_reply(swarm::http_response::internal_server_error);
						return;
					}
				}
			}

			this->send_reply(thevoid::http_response::ok);
		}
	};

	std::shared_ptr<bucket_transport> bucket() {
		return m_bucket;
	}

private:
	vector_lock m_lock;

	std::shared_ptr<elliptics::node> m_node;

	std::shared_ptr<bucket_transport> m_bucket;

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

		m_bucket.reset(new bucket_transport(m_node));

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

		std::vector<std::string> bnames;
		for (auto it = buckets.Begin(), end = buckets.End(); it != end; ++it) {
			if (it->IsString())
				bnames.push_back(it->GetString());
		}

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

		if (!m_bucket->init(mgroups, bnames))
			return false;

		return true;
	}

	bool indexes_init(const rapidjson::Value &config) {
		if (config.HasMember("lock-num")) {
			auto &ln = config["lock-num"];
			if (ln.IsNumber()) {
				m_lock.resize(ln.GetInt());
			}
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

