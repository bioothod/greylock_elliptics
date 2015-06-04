#ifndef __INDEXES_KEY_HPP
#define __INDEXES_KEY_HPP

#include "indexes/core.hpp"

#include <string>

namespace ioremap { namespace indexes {

struct key {
	std::string id;
	eurl url;

	size_t size() const {
		return id.size() + url.size();
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
		return id + ":" + url.str();
	}

	MSGPACK_DEFINE(id, url);
};


}} // namespace ioremap::indexes

#endif // __INDEXES_KEY_HPP
