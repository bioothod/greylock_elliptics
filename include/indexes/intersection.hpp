#ifndef __INDEXES_INTERSECTION_HPP
#define __INDEXES_INTERSECTION_HPP

#include "indexes/index.hpp"

#include <map>

namespace ioremap { namespace indexes { namespace intersect {
struct result {
	bool completed = false;

	// index name (eurl) -> set of keys from that index which match other indexes
	// key IDs will be the same, but key data (url) can be different
	std::map<eurl, std::vector<key>> keys;
};

template <typename T>
class intersector {
public:
	intersector(T &t) : m_t(t) {}
	result intersect(const std::vector<eurl> &indexes) const {
		std::string start = std::string("\0");
		return intersect(indexes, start, INT_MAX);
	}

	// search for intersections between all @indexes
	// starting with the key @start, returning at most @num entries
	//
	// after @intersect() completes, it sets @start to the next key to start searching from
	// user should not change that token, otherwise @intersect() may skip some entries or
	// return duplicates.
	//
	// if number of returned entries is less than requested number @num or if @start has been set to empty string
	// after call to this function returns, then intersection is completed.
	//
	// @result.completed will be set to true in this case.
	result intersect(const std::vector<eurl> &indexes, std::string &start, size_t num) const {
		struct iter {
			index<T> idx;
			indexes::iterator<T> begin, end;

			iter(T &t, const eurl &name, const std::string &start) :
				idx(t, name), begin(idx.begin(start)), end(idx.end()) {}
		};
		std::vector<iter> idata;

		for_each(indexes.begin(), indexes.end(), [&] (const eurl &name) {
				iter it(m_t, name, start);

				idata.emplace_back(it);
			});

		result res;

		while (!res.completed) {
			std::vector<int> pos;

			int current = -1;
			for (auto idata_it = idata.begin(); idata_it != idata.end(); ++idata_it) {
				auto &it = idata_it->begin;
				auto &e = idata_it->end;
				++current;

				if (it == e) {
					res.completed = true;
					break;
				}

				if (pos.size() == 0) {
					pos.push_back(current);
					continue;
				}

				auto &min_it = idata[pos[0]].begin;

				if (*it == *min_it) {
					pos.push_back(current);
					continue;
				}

				if (*it < *min_it) {
					pos.clear();
					pos.push_back(current);
				}
			}

			if (res.completed) {
				start = "";
				break;
			}

			if (pos.size() != idata.size()) {
				for (auto it = pos.begin(); it != pos.end(); ++it) {
					auto &min_it = idata[*it].begin;
					++min_it;
				}

				continue;
			}

			start = idata[pos[0]].begin->id;
			if (res.keys.begin()->second.size() == num) {
				break;
			}

			for (auto it = pos.begin(); it != pos.end(); ++it) {
				auto &min_it = idata[*it].begin;
				key k = *min_it;

				auto find_it = res.keys.find(indexes[*it]);
				if (find_it == res.keys.end()) {
					std::vector<key> kk;
					kk.emplace_back(k);
					auto pair = res.keys.insert(std::make_pair(indexes[*it], kk));
					find_it = pair.first;
				} else {
					find_it->second.emplace_back(k);
				}

				++min_it;
			}
		}

		return res;
	}
private:
	T &m_t;
};

}}} // namespace ioremap::indexes::intersect

#endif // __INDEXES_INTERSECTION_HPP
