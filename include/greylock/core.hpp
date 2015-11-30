#ifndef __INDEXES_CORE_HPP
#define __INDEXES_CORE_HPP

#include <string>
#include <msgpack.hpp>

#include <blackhole/blackhole.hpp>

namespace ioremap { namespace greylock {

static size_t max_page_size = 6144;
static size_t default_reserve_size = max_page_size/4;

#define dprintf(fmt, a...) do {} while (0)
//#define dprintf(fmt, a...) printf(fmt, ##a)

#define INDEXES_LOG_ERROR blackhole::defaults::severity::error
#define INDEXES_LOG_WARNING blackhole::defaults::severity::warning
#define INDEXES_LOG_INFO blackhole::defaults::severity::info
#define INDEXES_LOG_NOTICE blackhole::defaults::severity::notice
#define INDEXES_LOG_DEBUG blackhole::defaults::severity::debug


struct eurl {
	std::string bucket;
	std::string key;

	MSGPACK_DEFINE(bucket, key);


	size_t size() const {
		return bucket.size() + key.size();
	}

	std::string str() const {
		return bucket + "/" + key;
	}

	bool operator!=(const eurl &other) const {
		return bucket != other.bucket || key != other.key;
	}

	bool operator==(const eurl &other) const {
		return bucket == other.bucket && key == other.key;
	}

	bool operator<(const eurl &other) const {
		return bucket < other.bucket || key < other.key;
	}
	bool operator<=(const eurl &other) const {
		return bucket <= other.bucket || key <= other.key;
	}

	bool empty() const {
		return key.empty();
	}
};

}} // namespace ioremap::greylock

#endif // __INDEXES_CORE_HPP
