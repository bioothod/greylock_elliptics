#ifndef __INDEXES_BUCKET_HPP
#define __INDEXES_BUCKET_HPP

#include "greylock/core.hpp"
#include "greylock/elliptics_stat.hpp"

#include <elliptics/session.hpp>

#include <msgpack.hpp>

#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>

namespace ioremap { namespace greylock {

struct bucket_acl {
	enum {
		serialization_version = 2,
	};

	// This enum describes per-user authorization flags
	enum auth_flags {
		auth_no_token = 0x01, // this user is able to perform requests without the authorization
		auth_write = 0x02, // this user is able to write to this bucket
		auth_admin = 0x04, // this user is able to change this bucket,
		auth_all = auth_write | auth_admin,
	};

	// This enum describes per-handler authorization flags
	enum handler_flags {
		handler_read = 0x01, // user must have read rights to access this handler
		handler_write = 0x02, // user must have write rights to access this handler
		handler_bucket = 0x04, // user must have admin rights to access this handler
		handler_not_found_is_ok = 0x08 // user is able to access this handler even if bucket doesn't exist
	};

	bool has_no_token() const {
		return flags & auth_no_token;
	}

	bool can_read() const {
		return true;
	}

	bool can_write() const {
		return flags & auth_write;
	}

	bool can_admin() const {
		return flags & auth_admin;
	}

	std::string to_string(void) const {
		std::ostringstream acl_ss;
		if (!user.empty())
			acl_ss << user << ":" << token << ":0x" << std::hex << flags;

		return acl_ss.str();
	}

	std::string user;
	std::string token;
	uint64_t flags = 0;
};

struct bucket_meta {
	enum {
		serialization_version = 1,
	};

	std::string name;
	std::map<std::string, bucket_acl> acl;
	std::vector<int> groups;
	uint64_t flags = 0;
	uint64_t max_size = 0;
	uint64_t max_key_num = 0;
	uint64_t reserved[3];

	bucket_meta() {
		memset(reserved, 0, sizeof(reserved));
	}
};

struct bucket_stat {
	std::map<int, backend_stat>	backends;

	std::string str() {
		std::ostringstream ss;
		ss << "{";
		for (auto it = backends.begin(), end = backends.end(); it != end; ++it) {
			if (it != backends.begin())
				ss << ", ";
			ss << it->second.str();
		}
		ss << "}";

		return ss.str();
	}
};

class raw_bucket {
public:
	raw_bucket(std::shared_ptr<elliptics::node> &node, const std::vector<int> mgroups, const std::string &name) :
	m_node(node),
	m_meta_groups(mgroups),
	m_valid(false),
	m_reloaded(false)
	{
		m_meta.name = name;
		reload();
	}

	void reload() {
		elliptics::session s(*m_node);
		s.set_exceptions_policy(elliptics::session::no_exceptions);
		s.set_groups(m_meta_groups);

		std::string ns = "bucket";
		s.set_namespace(ns.c_str(), ns.size());

		m_reloaded = false;

		elliptics::logger &log = m_node->get_log();

		BH_LOG(log, DNET_LOG_INFO, "reload: going to reload bucket: %s", m_meta.name.c_str());
		s.read_data(m_meta.name, 0, 0).connect(
			std::bind(&raw_bucket::reload_completed, this, std::placeholders::_1, std::placeholders::_2));
	}

	bool wait_for_reload() {
		std::unique_lock<std::mutex> guard(m_lock);
		m_wait.wait(guard, [&] {return m_reloaded;});

		return m_valid;
	}

	bool valid() const {
		return m_valid && m_stat.backends.size() != 0;
	}

	std::string name() const {
		return m_meta.name;
	}

	std::string stat_str() {
		std::unique_lock<std::mutex> guard(m_lock);
		return m_stat.str();
	}

	elliptics::async_read_result read(const std::vector<int> &groups, const std::string &key) {
		elliptics::session s = session(true);
		if (!m_valid) {
			elliptics::async_read_result result(s);
			elliptics::async_result_handler<elliptics::read_result_entry> handler(result);
			handler.complete(elliptics::create_error(-EIO, "bucket: %s: valid: %d, reloaded: %d",
						m_meta.name.c_str(), m_valid, m_reloaded));
			return result;
		}

		s.set_groups(groups);
		return s.read_data(key, 0, 0);
	}
	
	elliptics::async_read_result read(const std::string &key) {
		return read(m_meta.groups, key);
	}

	elliptics::async_read_result read_latest(const std::string &key) {
		elliptics::session s = session(true);

		if (!m_valid) {
			elliptics::async_read_result result(s);
			elliptics::async_result_handler<elliptics::read_result_entry> handler(result);
			handler.complete(elliptics::create_error(-EIO, "bucket: %s: valid: %d, reloaded: %d",
						m_meta.name.c_str(), m_valid, m_reloaded));
			return result;
		}

		return s.read_latest(key, 0, 0);
	}

	elliptics::async_lookup_result prepare_latest(const std::string &key) {
		elliptics::session s = session(true);

		if (!m_valid) {
			elliptics::async_lookup_result result(s);
			elliptics::async_result_handler<elliptics::lookup_result_entry> handler(result);
			handler.complete(elliptics::create_error(-EIO, "bucket: %s: valid: %d, reloaded: %d",
						m_meta.name.c_str(), m_valid, m_reloaded));
			return result;
		}

		return s.prepare_latest(key, m_meta.groups);
	}

	elliptics::async_write_result write(const std::vector<int> groups, const std::string &key,
			const std::string &data, size_t reserve_size, bool cache = false) {
		elliptics::session s = session(cache);

		if (!m_valid) {
			elliptics::async_write_result result(s);
			elliptics::async_result_handler<elliptics::write_result_entry> handler(result);
			handler.complete(elliptics::create_error(-EIO, "bucket: %s: valid: %d, reloaded: %d",
						m_meta.name.c_str(), m_valid, m_reloaded));
			return result;
		}

		elliptics::data_pointer dp = elliptics::data_pointer::from_raw((char *)data.data(), data.size());

		s.set_filter(elliptics::filters::all);
		s.set_groups(groups);

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
		ctl.io.num = reserve_size;
		if (ctl.io.size > ctl.io.num) {
			ctl.io.num = ctl.io.size * 1.5;
		}

		memcpy(&ctl.id, &id.id(), sizeof(ctl.id));

		ctl.fd = -1;

		BH_LOG(m_node->get_log(), DNET_LOG_NOTICE,
				"%s: bucket write: bucket: %s, key: %s, data-size: %d, reserve-size: %d, cache: %d, ts: %s (%ld.%ld)\n",
				dnet_dump_id(&id.id()),
				m_meta.name.c_str(), key.c_str(), data.size(), reserve_size, cache,
				dnet_print_time(&ctl.io.timestamp), (long)ctl.io.timestamp.tsec, (long)ctl.io.timestamp.tnsec);

		return s.write_data(ctl);
	}

	elliptics::async_write_result write(const std::string &key, const std::string &data, size_t reserve_size, bool cache = false) {
		return write(m_meta.groups, key, data, reserve_size, cache);
	}

	elliptics::async_remove_result remove(const std::string &key) {
		elliptics::session s = session(false);
		if (!m_valid) {
			elliptics::async_remove_result result(s);
			elliptics::async_result_handler<elliptics::remove_result_entry> handler(result);
			handler.complete(elliptics::create_error(-EIO, "bucket: %s: valid: %d, reloaded: %d",
						m_meta.name.c_str(), m_valid, m_reloaded));
			return result;
		}

		return s.remove(key);
	}

	bucket_meta meta() {
		std::lock_guard<std::mutex> guard(m_lock);
		return m_meta;
	}

	void set_backend_stat(int group, const backend_stat &bs) {
		std::lock_guard<std::mutex> guard(m_lock);
		m_stat.backends[group] = bs;
	}

	// weight is a value in (0,1) range,
	// the closer to 1, the more likely this bucket will be selected
	float weight(uint64_t size, const limits &l) {
		std::lock_guard<std::mutex> guard(m_lock);

		// we select backend with the smallest amount of space available
		// any other space metric may end up with the situation when we will
		// write data to backend where there is no space
		float size_weight = 0;
		for (auto st = m_stat.backends.begin(), end = m_stat.backends.end(); st != end; ++st) {
			const backend_stat &bs = st->second;
			float tmp = bs.size.limit - bs.size.used;

			// there is no space at least in one backend for given size in this bucket
			if (tmp < size) {
				return 0;
			}

			tmp /= (float)(bs.size.limit);

			// size is less than hard limit - this backend doesn't have enough space for given request
			if (tmp < l.size.hard) {
				return 0;
			}

			// heavily deacrease weight of this backend (and thus bucket)
			// if amount of free space is less than soft limit
			if (tmp < l.size.soft) {
				tmp /= 10;
			}

			if (tmp < size_weight || size_weight == 0)
				size_weight = tmp;
		}

		// so far only size metric is supported
		// TODO next step is to add network/disk performance metric
		// TODO we have to measure upload time and modify weight
		// TODO accordingly to the time it took to write data
		return size_weight;
	}

private:
	std::shared_ptr<elliptics::node> m_node;
	std::vector<int> m_meta_groups;

	bool m_valid = false;

	bool m_reloaded = false;
	std::condition_variable m_wait;

	std::mutex m_lock;
	bucket_meta m_meta;

	bucket_stat m_stat;

	elliptics::session session(bool cache) {
		elliptics::session s(*m_node);
		s.set_namespace(m_meta.name);
		s.set_groups(m_meta.groups);
		s.set_filter(elliptics::filters::all_with_ack);
		s.set_timeout(60);
		s.set_exceptions_policy(elliptics::session::no_exceptions);
		if (cache)
			s.set_ioflags(DNET_IO_FLAGS_CACHE);

		return s;
	}

	void reload_completed(const elliptics::sync_read_result &result, const elliptics::error_info &error) {
		elliptics::logger &log = m_node->get_log();

		if (error) {
			BH_LOG(log, DNET_LOG_ERROR, "reload_completed: bucket: %s: could not reload: %s, error: %d",
					m_meta.name.c_str(), error.message().c_str(), error.code());
		} else {
			meta_unpack(result);
		}

		m_reloaded = true;
		m_wait.notify_all();
	}

	void meta_unpack(const elliptics::sync_read_result &result) {
		elliptics::logger &log = m_node->get_log();

		for (auto ent = result.begin(), end = result.end(); ent != end; ++ent) {
			if (ent->error()) {
				BH_LOG(log, DNET_LOG_ERROR, "meta_unpack: bucket: %s, error result: %s [%d]",
						m_meta.name.c_str(), ent->error().message(), ent->error().code());
				continue;
			}

			try {
				auto file = ent->file();

				msgpack::unpacked msg;
				msgpack::unpack(&msg, file.data<char>(), file.size());

				bucket_meta tmp;

				msg.get().convert(&tmp);

				std::ostringstream ss;
				std::copy(tmp.groups.begin(), tmp.groups.end(), std::ostream_iterator<int>(ss, ":"));

				BH_LOG(log, DNET_LOG_INFO, "meta_unpack: bucket: %s, acls: %ld, flags: 0x%lx, groups: %s",
						m_meta.name.c_str(), m_meta.acl.size(), m_meta.flags, ss.str().c_str());

				std::unique_lock<std::mutex> guard(m_lock);
				std::swap(m_meta, tmp);
				m_valid = true;
			} catch (const std::exception &e) {
				BH_LOG(log, DNET_LOG_ERROR, "meta_unpack: bucket: %s, exception: %s",
						m_meta.name.c_str(), e.what());
			}
		}
	}
};

typedef std::shared_ptr<raw_bucket> bucket;
static inline bucket make_bucket(std::shared_ptr<elliptics::node> &node, const std::vector<int> mgroups, const std::string &name) {
	return std::make_shared<raw_bucket>(node, mgroups, name);
}

}} // namespace ioremap::greylock

namespace msgpack
{
static inline ioremap::greylock::bucket_acl &operator >>(msgpack::object o, ioremap::greylock::bucket_acl &acl)
{
	if (o.type != msgpack::type::ARRAY) {
		std::ostringstream ss;
		ss << "bucket-acl unpack: type: " << o.type <<
			", must be: " << msgpack::type::ARRAY <<
			", size: " << o.via.array.size;
		throw std::runtime_error(ss.str());
	}

	object *p = o.via.array.ptr;
	const uint32_t size = o.via.array.size;
	uint16_t version = 0;
	p[0].convert(&version);
	switch (version) {
	case 1:
	case 2: {
		if (size != 4) {
			std::ostringstream ss;
			ss << "bucket acl unpack: array size mismatch: read: " << size << ", must be: 4";
			throw std::runtime_error(ss.str());
		}

		p[1].convert(&acl.user);
		p[2].convert(&acl.token);
		p[3].convert(&acl.flags);

		if (version == 1) {
			using namespace ioremap::greylock;
			// Convert flags from old version to new one
			const bool noauth_read = acl.flags & (1 << 0);
			const bool noauth_all = acl.flags & (1 << 1);

			acl.flags = 0;

			// If there was any noauth - we shouldn't check token
			if (noauth_all || noauth_read) {
				acl.flags |= bucket_acl::auth_no_token;
			}

			// If there wasn't 'noauth_read' flag - user is permitted to do everything he want
			if (!noauth_read) {
				acl.flags |= bucket_acl::auth_admin | bucket_acl::auth_write;
			}
		}
		break;
	}
	default: {
		std::ostringstream ss;
		ss << "bucket acl unpack: version mismatch: read: " << version <<
			", must be: <= " << ioremap::greylock::bucket_acl::serialization_version;
		throw std::runtime_error(ss.str());
	}
	}

	return acl;
}

template <typename Stream>
inline msgpack::packer<Stream> &operator <<(msgpack::packer<Stream> &o, const ioremap::greylock::bucket_acl &acl)
{
	o.pack_array(4);
	o.pack((int)ioremap::greylock::bucket_acl::serialization_version);
	o.pack(acl.user);
	o.pack(acl.token);
	o.pack(acl.flags);

	return o;
}

template <typename Stream>
static inline msgpack::packer<Stream> &operator <<(msgpack::packer<Stream> &o, const ioremap::greylock::bucket_meta &m)
{
	o.pack_array(10);
	o.pack((int)ioremap::greylock::bucket_meta::serialization_version);
	o.pack(m.name);
	o.pack(m.acl);
	o.pack(m.groups);
	o.pack(m.flags);
	o.pack(m.max_size);
	o.pack(m.max_key_num);
	for (size_t i = 0; i < ARRAY_SIZE(m.reserved); ++i)
		o.pack(m.reserved[i]);

	return o;
}

static inline ioremap::greylock::bucket_meta &operator >>(msgpack::object o, ioremap::greylock::bucket_meta &m)
{
	if (o.type != msgpack::type::ARRAY || o.via.array.size < 10) {
		std::ostringstream ss;
		ss << "bucket unpack: type: " << o.type <<
			", must be: " << msgpack::type::ARRAY <<
			", size: " << o.via.array.size;
		throw std::runtime_error(ss.str());
	}

	object *p = o.via.array.ptr;
	const uint32_t size = o.via.array.size;
	uint16_t version = 0;
	p[0].convert(&version);
	switch (version) {
	case 1: {
		if (size != 10) {
			std::ostringstream ss;
			ss << "bucket unpack: array size mismatch: read: " << size << ", must be: 10";
			throw std::runtime_error(ss.str());
		}

		p[1].convert(&m.name);
		p[2].convert(&m.acl);
		p[3].convert(&m.groups);
		p[4].convert(&m.flags);
		p[5].convert(&m.max_size);
		p[6].convert(&m.max_key_num);
		for (size_t i = 0; i < ARRAY_SIZE(m.reserved); ++i)
			p[7 + i].convert(&m.reserved[i]);
		break;
	}
	default: {
		std::ostringstream ss;
		ss << "bucket unpack: version mismatch: read: " << version <<
			", must be: <= " << ioremap::greylock::bucket_meta::serialization_version;
		throw std::runtime_error(ss.str());
	}
	}

	return m;
}
} // namespace msgpack

#endif // __INDEXES_BUCKET_HPP
