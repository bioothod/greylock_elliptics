#ifndef __INDEXES_STAT_HPP
#define __INDEXES_STAT_HPP

#include "indexes/error.hpp"

#include <elliptics/session.hpp>

#include <thevoid/rapidjson/document.h>

namespace ioremap { namespace indexes {
static inline const char *get_string(const rapidjson::Value &entry, const char *name, const char *def = NULL) {
	if (entry.HasMember(name)) {
		const rapidjson::Value &v = entry[name];
		if (v.IsString()) {
			return v.GetString();
		}
	}

	return def;
}

static inline int64_t get_int64(const rapidjson::Value &entry, const char *name, int64_t def = -1) {
	if (entry.HasMember(name)) {
		const rapidjson::Value &v = entry[name];
		if (v.IsInt()) {
			return v.GetInt();
		}
		if (v.IsUint()) {
			return v.GetUint();
		}
		if (v.IsInt64()) {
			return v.GetInt64();
		}
		if (v.IsUint()) {
			return v.GetUint64();
		}
	}

	return def;
}

static inline const rapidjson::Value &get_object(const rapidjson::Value &entry, const char *name,
		const rapidjson::Value &def = rapidjson::Value()) {
	if (entry.HasMember(name)) {
		const rapidjson::Value &v = entry[name];

		if (v.IsObject())
			return v;
	}

	return def;
}

static inline const rapidjson::Value &get_array(const rapidjson::Value &entry, const char *name,
		const rapidjson::Value &def = rapidjson::Value()) {
	if (entry.HasMember(name)) {
		const rapidjson::Value &v = entry[name];

		if (v.IsArray())
			return v;
	}

	return def;
}

static inline bool get_bool(const rapidjson::Value &entry, const char *name, bool def = true) {
	if (entry.HasMember(name)) {
		const rapidjson::Value &v = entry[name];

		if (v.IsBool())
			return v.GetBool();
	}

	return def;
}

// weight calculation limits
//
// Metric in question is not allowed to be less than @hard limit and
// preferably should not be less than @soft limit
struct limits {
	struct {
		float soft = 0.2;
		float hard = 0.1;
	} size;
};

struct backend_stat {
	backend_stat() {}
	backend_stat(struct dnet_addr *_addr) : addr(_addr) {}

	struct dnet_addr *addr = NULL;

	int		backend_id = -1;
	int		group = -1;

	int 		state = 0;
	bool		ro = true;
	int		start_error = 0;
	int		defrag_state = 0;

	struct {
		uint64_t	limit = 0;
		uint64_t	used = 0;
		uint64_t	removed = 0;
	} size;

	struct {
		uint64_t	avail = 0;
		uint64_t	total = 0;
	} vfs;

	struct {
		uint64_t	total = 0;
		uint64_t	removed = 0;
		uint64_t	corrupted = 0;
	} records;

	std::string str() const {
		char tmp[1024];
		snprintf(tmp, sizeof(tmp), "addr: %s, backend_id: %d, group: %d, "
			"state: %d, defrag_state: %d, ro: %d, start_error: %d"
			"size: limit: %llu, used: %llu, removed: %llu, "
			"can_be_written: %llu, can_be_written_plus_removed: %llu, "
			"records: total: %llu, removed: %llu, corrupted: %llu",
				dnet_addr_string(addr), backend_id, group,
				state, defrag_state, ro, start_error,
				(unsigned long long)size.limit, (unsigned long long)size.used, (unsigned long long)size.removed,
				(unsigned long long)(size.limit - size.used),
					(unsigned long long)(size.limit - size.used + size.removed),
				(unsigned long long)records.total, (unsigned long long)records.removed,
					(unsigned long long)records.corrupted);

		return std::string(tmp);
	}

	void fill_status(elliptics::logger &log, const rapidjson::Value &status) {
		state = get_int64(status, "state");
		ro = get_bool(status, "read_only");
		start_error = get_int64(status, "last_start_err");
		defrag_state = get_int64(status, "defrag_state");

		BH_LOG(log, DNET_LOG_NOTICE,
			"stat: fill_status: addr: %s, backend_id: %d, "
			"state: %d, defrag_state: %d, ro: %d, start_error: %d",
				dnet_addr_string(addr), backend_id, state, defrag_state, ro, start_error);

	}

	void fill_vfs_stats(const rapidjson::Value &vstat) {
		uint64_t blocks = get_int64(vstat, "blocks");
		uint64_t bsize = get_int64(vstat, "bsize");

		vfs.total = get_int64(vstat, "frsize") * blocks;
		vfs.avail = get_int64(vstat, "bfree") * bsize;
	}

	void fill_raw_stats(elliptics::logger &log, const rapidjson::Value &backend) {
		const rapidjson::Value &summary = get_object(backend, "summary_stats");
		if (!summary.IsObject()) {
			BH_LOG(log, DNET_LOG_ERROR,
				"stat: fill_raw_stats: addr: %s, backend_id: %d, json logic error: invalid 'summary_stats' object",
					dnet_addr_string(addr), backend_id);
			return;
		}

		const rapidjson::Value &config = get_object(backend, "config");
		if (!config.IsObject()) {
			BH_LOG(log, DNET_LOG_ERROR,
				"stat: fill_raw_stats: addr: %s, backend_id: %d, json logic error: invalid 'config' object",
					dnet_addr_string(addr), backend_id);
			return;
		}
		uint64_t config_flags = get_int64(config, "blob_flags");

		group = get_int64(config, "group");
		if (group < 0) {
			BH_LOG(log, DNET_LOG_ERROR,
				"stat: fill_raw_stats: addr: %s, backend_id: %d, json logic error: invalid 'group' field",
					dnet_addr_string(addr), backend_id);
			return;
		}

		const rapidjson::Value &vstat = get_object(backend, "config");
		if (!vstat.IsObject()) {
			BH_LOG(log, DNET_LOG_ERROR,
				"stat: fill_raw_stats: addr: %s, backend_id: %d, json logic error: invalid 'vfs' object",
					dnet_addr_string(addr), backend_id);
			return;
		}

		fill_vfs_stats(vstat);

		size.limit = get_int64(config, "blob_size_limit", 0);

		// if there is eblob flag 'no size check' or there is no @blob_size_limit option,
		// use total available disk space as size limit
		if ((size.limit == 0) || (config_flags & (1<<4))) {
			size.limit = vfs.total;
		}

		size.used = get_int64(summary, "base_size", 0);
		size.removed = get_int64(summary, "records_removed_size", 0);

		records.total = get_int64(summary, "records_total", 0);
		records.removed = get_int64(summary, "records_removed", 0);
		records.corrupted = get_int64(summary, "records_corrupted", 0);

		auto level = DNET_LOG_INFO;
		if (records.corrupted != 0) {
			level = DNET_LOG_ERROR;
		}

		BH_LOG(log, level, "stat: fill_raw_stats: %s", str().c_str());
	}
};

class elliptics_stat {
public:
	elliptics_stat(std::shared_ptr<elliptics::node> &node) : m_node(node) {}

	void schedule_update_and_wait() {
		elliptics::session s(*m_node);
		s.set_exceptions_policy(elliptics::session::no_exceptions);

		uint64_t cat = DNET_MONITOR_BACKEND;

		elliptics::logger &log = m_node->get_log();

		BH_LOG(log, DNET_LOG_INFO, "stat: schedule_update: going to request global backend statistics");
		auto st = s.monitor_stat(cat);
		st.connect(std::bind(&elliptics_stat::update_completion, this, std::placeholders::_1, std::placeholders::_2));
		st.wait();
	}

	backend_stat stat(int group) {
		std::lock_guard<std::mutex> guard(m_group_lock);
		auto it = m_group_stat.find(group);
		if (it == m_group_stat.end()) {
			return backend_stat();
		}

		return it->second;
	}

private:
	std::shared_ptr<elliptics::node> m_node;

	std::mutex m_group_lock;
	std::map<int, backend_stat> m_group_stat;

	void update_completion(const elliptics::sync_monitor_stat_result &result, const elliptics::error_info &error) {
		elliptics::logger &log = m_node->get_log();

		if (error) {
			BH_LOG(log, DNET_LOG_ERROR, "stat: update_completion: error: %s [%d]", error.message().c_str(), error.code());
			return;
		}

		std::map<int, backend_stat> gstat;

		for (auto res_it = result.begin(), res_end = result.end(); res_it != res_end; ++res_it) {
			const elliptics::monitor_stat_result_entry &ent = *res_it;
			std::string statistics = ent.statistics();

			rapidjson::Document doc;
			doc.Parse<0>(statistics.c_str());

			if (doc.HasParseError()) {
				BH_LOG(log, DNET_LOG_ERROR, "stat: update_completion: json parser error: %s, offset: %zd",
						doc.GetParseError(), doc.GetErrorOffset());
				continue;
			}

			struct dnet_addr *addr = ent.address();
			const rapidjson::Value &backends = get_object(doc, "backends");
			if (!backends.IsObject()) {
				BH_LOG(log, DNET_LOG_ERROR,
					"stat: update_completion: addr: %s, json logic error: no 'backends' object",
						dnet_addr_string(addr));
				continue;
			}

			for (rapidjson::Value::ConstMemberIterator backend_it = backends.MemberBegin(),
					backend_end = backends.MemberEnd();
					backend_it != backend_end; ++backend_it) {
				const rapidjson::Value &backend = backend_it->value;
				if (!backend.IsObject()) {
					BH_LOG(log, DNET_LOG_ERROR,
						"stat: update_completion: addr: %s, json logic error: "
						"'backends' map does not contain objects",
							dnet_addr_string(addr));
					break;
				}

				backend_stat b(addr);

				b.backend_id = get_int64(backend, "backend_id");
				if (b.backend_id < 0) {
					BH_LOG(log, DNET_LOG_ERROR,
						"stat: update_completion: addr: %s, json logic error: "
						"invalid 'backends/%s/backend_id' object",
							dnet_addr_string(addr), backend_it->name.GetString());
					continue;
				}

				const rapidjson::Value &status = get_object(backend, "status");
				if (!status.IsObject()) {
					BH_LOG(log, DNET_LOG_ERROR,
						"stat: update_completion: addr: %s, backend_id: %d: json logic error: "
						"invalid 'status' object",
							dnet_addr_string(addr), b.backend_id);
					continue;
				}

				b.fill_status(log, status);

				if (b.state != DNET_BACKEND_ENABLED)
					continue;

				const rapidjson::Value &raw_backend = get_object(backend, "backend");
				if (!raw_backend.IsObject()) {
					BH_LOG(log, DNET_LOG_ERROR,
						"stat: update_completion: addr: %s, backend_id: %d: json logic error: "
						"invalid 'backend' object",
							dnet_addr_string(addr), b.backend_id);
					continue;
				}

				b.fill_raw_stats(log, raw_backend);

				if (b.group > 0)
					gstat[b.group] = std::move(b);
			}
		}

		std::lock_guard<std::mutex> guard(m_group_lock);
		m_group_stat.swap(gstat);
	}
};

}} // namespace ioremap::indexes

#endif // __INDEXES_STAT_HPP
