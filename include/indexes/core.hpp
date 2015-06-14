#ifndef __INDEXES_CORE_HPP
#define __INDEXES_CORE_HPP

#include <string>
#include <msgpack.hpp>

namespace ioremap { namespace indexes {

static size_t max_page_size = 4096;
static size_t default_reserve_size = max_page_size * 1.5;

#define dprintf(fmt, a...) do {} while (0)
//#define dprintf(fmt, a...) printf(fmt, ##a)

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

}} // namespace ioremap::indexes

#endif // __INDEXES_CORE_HPP
