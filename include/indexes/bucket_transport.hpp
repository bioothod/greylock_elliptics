#ifndef __INDEXES_BUCKET_TRANSPORT_HPP
#define __INDEXES_BUCKET_TRANSPORT_HPP

#include "indexes/bucket.hpp"
#include "indexes/core.hpp"
#include "indexes/error.hpp"

#include <elliptics/session.hpp>

namespace ioremap { namespace indexes {

// this transport spreads all write load among set of buckets available in bucket_processor
// main function which 'shards' data among internal buckets is @get_bucket() - it returns name of the bucket to write data into
class bucket_transport : public indexes::bucket_processor {
public:
	bucket_transport(std::shared_ptr<elliptics::node> node) : bucket_processor(node) {}


	std::vector<int> get_groups() {
		return std::vector<int>();
	}

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

}} // namespace ioremap::indexes

#endif //__INDEXES_BUCKET_TRANSPORT_HPP
