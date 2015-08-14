#ifndef __INDEXES_KEY_HPP
#define __INDEXES_KEY_HPP

#include "greylock/core.hpp"

#include <string>

namespace ioremap { namespace greylock {

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


}} // namespace ioremap::greylock

#endif // __INDEXES_KEY_HPP
