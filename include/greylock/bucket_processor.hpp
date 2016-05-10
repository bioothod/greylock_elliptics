#ifndef __INDEXES_BUCKET_PROCESSOR_HPP
#define __INDEXES_BUCKET_PROCESSOR_HPP

#include "greylock/bucket.hpp"
#include "greylock/elliptics_stat.hpp"

#include <elliptics/session.hpp>

#include <chrono>
#include <functional>
#include <thread>

namespace ioremap { namespace greylock {

// This class implements main distribution logic.
// There are multiple @bucket objects in the processor,
// each bucket corresponds to logical entity which handles replication
// and optionally additional internal write load balancing.
//
// Basically saying, @bucket is an entity which holds your data and
// checks whether it is good or not.
//
// Thus, after you data has been written into given bucket, you can only
// read it from that bucket and update it only in that bucket. If you write
// data with the same key into different bucket, that will be a completely different object,
// objects with the same name in different buckets will not be related in any way.
//
// When you do not have (do not even know) about which bucket to use, there is a helper method @get_bucket()
// It will select bucket which suits your needs better than others. Selection logic taks into consideration
// space measurements and various performance metrics (implicit disk and network for instance).
// Selection is probabilistic in nature, thus it is possible that you will not get the best match,
// this probability obeys normal distribution law and decays quickly.
//
// It is only possible to read data from some bucket (where you had written it), not from the storage.
// There is internal per-bucket load balancing for read requests - elliptics maintains internal states
// for all connections and groups (replicas within bucket) and their weights, which correspond to how
// quickly one can read object from given destination point. When reading data from bucket, it selects
// the fastest desistnation point among its replicas and reads data from that point. If data is not available,
// elliptics will automatically fetch (data recover) data from other copies.
class bucket_processor {
public:
	bucket_processor(std::shared_ptr<elliptics::node> &node) :
	m_node(node),
	m_stat(node),
	m_error_session(*m_node)
	{
		m_error_session.set_exceptions_policy(elliptics::session::no_exceptions);
		m_error_session.set_filter(elliptics::filters::all_with_ack);
	}

	virtual ~bucket_processor() {
		m_need_exit = true;
		m_wait.notify_all();
		if (m_buckets_update.joinable())
			m_buckets_update.join();
	}

	bool init(const std::vector<int> &mgroups, const std::vector<std::string> &bnames) {
		std::map<std::string, bucket> buckets = read_buckets(mgroups, bnames);

		m_buckets_update =  std::thread(std::bind(&bucket_processor::buckets_update, this));

		std::unique_lock<std::mutex> lock(m_lock);

		m_buckets = buckets;
		m_bnames = bnames;
		m_meta_groups = mgroups;

		if (buckets.empty())
			return false;

		return true;
	}

	const elliptics::logger &logger() const {
		return m_node->get_log();
	}

	// run self test, raise exception if there are problems
	//
	// Tests:
	// 1. select bucket for upload multiple times,
	// 	check that distribution is biased towards buckets with more free space available
	void test() {
		elliptics::logger &log = m_node->get_log();
		BH_LOG(log, DNET_LOG_INFO, "test: start: buckets: %d", m_buckets.size());

		std::unique_lock<std::mutex> guard(m_lock);
		if (m_buckets.size() == 0) {
			throw std::runtime_error("there are no buckets at all");
		}

		limits l;

		std::map<float, bucket> good_buckets;
		std::map<float, bucket> really_good_buckets;

		float sum = 0;
		float really_good_sum = 0;
		for (auto it = m_buckets.begin(), end = m_buckets.end(); it != end; ++it) {
			if (it->second->valid()) {
				float w = it->second->weight(1, l);

				BH_LOG(log, DNET_LOG_NOTICE, "test: bucket: %s, weight: %f", it->second->name(), w);

				// skip buckets with zero weights
				// usually this means that there is no free space for this request
				// or stats are broken (timed out)
				if (w <= 0)
					continue;

				good_buckets[w] = it->second;
				sum += w;

				if (w > 0.5) {
					really_good_buckets[w] = it->second;
					really_good_sum += w;
				}
			}
		}

		guard.unlock();

		// use really good buckets if we have them
		if (really_good_sum > 0) {
			sum = really_good_sum;
			good_buckets.swap(really_good_buckets);
		}

		if (good_buckets.size() == 0) {
			throw std::runtime_error("there are buckets, but they are not suitable for size " +
					elliptics::lexical_cast(1));
		}


		// first test - call @get_bucket() many times, check that distribution
		// of the buckets looks similar to the initial weights
		int num = 10000;
		std::map<std::string, int> bucket_counters;
		for (int i = 0; i < num; ++i) {
			std::string bname;
			elliptics::error_info err = get_bucket(1, bname);
			if (err) {
				throw std::runtime_error("get_bucket() failed: " + err.message());
			}

			int cnt = bucket_counters[bname];
			cnt++;
			bucket_counters[bname] = cnt;
		}

		for (auto it = good_buckets.begin(), end = good_buckets.end(); it != end; ++it) {
			float w = it->first;
			bucket b = it->second;

			int counter = bucket_counters[b->name()];
			float ratio = (float)counter/(float)num;
			float wratio = w / sum;

			// @ratio is a number of this bucket selection related to total number of runs
			// it should rougly correspond to the ratio of free space in given bucket, or weight


			BH_LOG(log, DNET_LOG_INFO, "test: bucket: %s, weight: %f, weight ratio: %f, selection ratio: %f",
					b->name(), w, wratio, ratio);

			float eq = ratio / wratio;
			if (eq > 1.2 || eq < 0.8) {
				std::ostringstream ss;
				ss << "bucket: " << b->name() <<
					", weight: " << w <<
					", weight ratio: " << wratio <<
					", selection ratio: " << ratio <<
					": parameters mismatch, weight and selection ratios should be close to each other";
				throw std::runtime_error(ss.str());
			}
		}

		BH_LOG(log, DNET_LOG_INFO, "test: weight comparison of %d buckets has been completed", m_buckets.size());
	}

	// returns bucket name in @data or negative error code in @error
	elliptics::error_info get_bucket(size_t size, std::string &bname) {
		elliptics::logger &log = m_node->get_log();

		std::unique_lock<std::mutex> guard(m_lock);
		if (m_buckets.size() == 0) {
			return elliptics::create_error(-ENODEV, "there are no buckets at all");
		}

		struct bw {
			bucket		b;
			float		w = 0;
		};

		std::vector<bw> good_buckets;
		good_buckets.reserve(m_buckets.size());

		for (auto it = m_buckets.begin(), end = m_buckets.end(); it != end; ++it) {
			if (it->second->valid()) {
				bw b;
				b.b = it->second;
				good_buckets.push_back(b);
			}
		}

		guard.unlock();

		if (good_buckets.size() == 0) {
			return elliptics::create_error(-ENODEV, "there are buckets, but they are not suitable for size %zd", size);
		}

		limits l;
		float sum = 0;
		for (auto it = good_buckets.rbegin(), end = good_buckets.rend(); it != end; ++it) {
			// weight calculation is a rather heavy task, cache this value
			it->w = it->b->weight(1, l);
			sum += it->w;
		}

		struct {
			// reverse sort - from higher to lower weights
			bool operator()(const bw &b1, const bw &b2) {
				return b1.w > b2.w;
			}
		} cmp;
		std::sort(good_buckets.begin(), good_buckets.end(), cmp);

		// randomly select value in a range [0, sum+1)
		// then iterate over all good buckets starting from the one with the highest weight
		//
		// the higher the weight, the more likely this bucket will be selected
		float rnd = (0 + (rand() % (int)(sum * 10 - 0 + 1))) / 10.0;

		BH_LOG(log, DNET_LOG_NOTICE, "test: weight selection: good-buckets: %d, rnd: %f, sum: %f",
				good_buckets.size(), rnd, sum);

		for (auto it = good_buckets.rbegin(), end = good_buckets.rend(); it != end; ++it) {
			BH_LOG(log, DNET_LOG_NOTICE, "test: weight comparison: bucket: %s, sum: %f, rnd: %f, weight: %f",
					it->b->name().c_str(), sum, rnd, it->w);
			rnd -= it->w;
			if (rnd <= 0) {
				bname = it->b->name();
				break;
			}
		}

		return elliptics::error_info();
	}


	elliptics::async_read_result read(const eurl &url) {
		bucket b;
		elliptics::error_info err = find_bucket(url.bucket, b);
		if (err) {
			elliptics::async_read_result result(m_error_session);
			elliptics::async_result_handler<elliptics::read_result_entry> handler(result);
			handler.complete(err);
			return result;
		}

		return b->read(url.key);
	}

	elliptics::async_read_result read(const std::vector<int> &groups, const eurl &url) {
		bucket b;
		elliptics::error_info err = find_bucket(url.bucket, b);
		if (err) {
			elliptics::async_read_result result(m_error_session);
			elliptics::async_result_handler<elliptics::read_result_entry> handler(result);
			handler.complete(err);
			return result;
		}

		return b->read(groups, url.key);
	}

	elliptics::async_read_result read_latest(const eurl &url) {
		bucket b;

		elliptics::error_info err = find_bucket(url.bucket, b);
		if (err) {
			elliptics::async_read_result result(m_error_session);
			elliptics::async_result_handler<elliptics::read_result_entry> handler(result);
			handler.complete(err);

			return result;
		}

		return b->read_latest(url.key);
	}

	elliptics::async_lookup_result prepare_latest(const eurl &url) {
		bucket b;

		elliptics::error_info err = find_bucket(url.bucket, b);
		if (err) {
			elliptics::async_lookup_result result(m_error_session);
			elliptics::async_result_handler<elliptics::lookup_result_entry> handler(result);
			handler.complete(err);

			return result;
		}

		return b->prepare_latest(url.key);
	}

	elliptics::async_write_result write(const std::vector<int> groups, const eurl &url,
			const std::string &data, size_t reserve_size, bool cache) {
		bucket b;
		elliptics::error_info err = find_bucket(url.bucket, b);
		if (err) {
			elliptics::async_write_result result(m_error_session);
			elliptics::async_result_handler<elliptics::write_result_entry> handler(result);
			handler.complete(err);
			return result;
		}

		return b->write(groups, url.key, data, reserve_size, cache);
	}

	elliptics::async_write_result write(const eurl &url, const std::string &data, size_t reserve_size, bool cache) {
		bucket b;
		elliptics::error_info err = find_bucket(url.bucket, b);
		if (err) {
			elliptics::async_write_result result(m_error_session);
			elliptics::async_result_handler<elliptics::write_result_entry> handler(result);
			handler.complete(err);
			return result;
		}

		return b->write(url.key, data, reserve_size, cache);
	}

	elliptics::async_remove_result remove(const eurl &url) {
		bucket b;
		elliptics::error_info err = find_bucket(url.bucket, b);
		if (err) {
			elliptics::async_remove_result result(m_error_session);
			elliptics::async_result_handler<elliptics::remove_result_entry> handler(result);
			handler.complete(err);
			return result;
		}

		return b->remove(url.key);
	}

	std::string generate(const std::string &bname, const std::string &key) const {
		elliptics::session s(*m_node);
		s.set_namespace(bname.data(), bname.size());
		elliptics::key k(key);
		s.transform(k);

		DNET_DUMP_ID_LEN(name, &k.id(), DNET_ID_SIZE);
		return key + "." + std::string(name);
	}

private:
	std::shared_ptr<elliptics::node> m_node;

	std::mutex m_lock;
	std::vector<int> m_meta_groups;

	std::vector<std::string> m_bnames;
	std::map<std::string, bucket> m_buckets;

	bool m_need_exit = false;
	std::condition_variable m_wait;
	std::thread m_buckets_update;

	elliptics_stat m_stat;

	elliptics::session m_error_session;

	elliptics::error_info find_bucket(const std::string &bname, bucket &b) {
		std::lock_guard<std::mutex> lock(m_lock);
		auto it = m_buckets.find(bname);
		if (it == m_buckets.end()) {
			return elliptics::create_error(-ENOENT, "could not find bucket '%s' in bucket list", bname.c_str());
		}

		if (!it->second->valid()) {
			return elliptics::create_error(-EINVAL, "bucket '%s' is not valid", bname.c_str());
		}

		b = it->second;
		return elliptics::error_info();
	}

	std::map<std::string, bucket> read_buckets(const std::vector<int> mgroups, const std::vector<std::string> &bnames) {
		std::map<std::string, bucket> buckets;

		for (auto it = bnames.begin(), end = bnames.end(); it != end; ++it) {
			buckets[*it] = make_bucket(m_node, mgroups, *it);
		}
		m_stat.schedule_update_and_wait();

		limits l;
		elliptics::logger &log = m_node->get_log();
		for (auto it = buckets.begin(), end = buckets.end(); it != end; ++it) {
			it->second->wait_for_reload();

			bucket_meta meta = it->second->meta();
			for (auto g = meta.groups.begin(), gend = meta.groups.end(); g != gend; ++g) {
				backend_stat bs = m_stat.stat(*g);
				if (bs.group == *g) {
					it->second->set_backend_stat(*g, bs);
				}
			}

			BH_LOG(log, DNET_LOG_INFO, "read_buckets: bucket: %s: reloaded, valid: %d, "
					"stats: %s, weight: %f",
					it->first.c_str(), it->second->valid(),
					it->second->stat_str().c_str(), it->second->weight(1, l));
		}

		return buckets;
	}

	void buckets_update() {
		while (!m_need_exit) {
			std::unique_lock<std::mutex> guard(m_lock);
			if (m_wait.wait_for(guard, std::chrono::seconds(30), [&] {return m_need_exit;}))
				break;
			guard.unlock();

			std::map<std::string, bucket> buckets = read_buckets(m_meta_groups, m_bnames);

			guard.lock();
			m_buckets = buckets;
		}
	}
};

}} // namespace ioremap::greylock

#endif // __INDEXES_BUCKET_PROCESSOR_HPP
