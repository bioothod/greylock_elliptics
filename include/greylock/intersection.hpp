#ifndef __INDEXES_INTERSECTION_HPP
#define __INDEXES_INTERSECTION_HPP

#include "greylock/index.hpp"

#include <map>

namespace ioremap { namespace greylock { namespace intersect {

struct single_doc_result {
	// how to get the document
	// document name lives in @id
	// @bucket/@key pair contains elliptics credentials to read the doc (if was specified at insertion time)
	key doc;

	// every entry in this array corresponds to one of the requested index name,
	// array size will always be equal to requested number of indexes
	//
	// each key contains index name and how to find it (index's @bucket/@key, @id contains index name)
	// as well as vector of positions where given index name is being located in the document
	std::vector<key> indexes;
};

struct result {
	bool completed = false;

	// This will contain a cookie which must be used for the next intersection request,
	// if current request is not complete. This may happen when client has requested limited
	// maximum number of keys in reply and there are more keys.
	std::string cookie;
	long max_number_of_documents = ~0UL;

	// array of documents which contain all requested indexes
	std::vector<single_doc_result> docs;
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
			read_only_index<T> idx;
			greylock::iterator<T> begin, end;

			iter(T &t, const eurl &name, const std::string &start) :
				idx(t, name), begin(idx.begin(start)), end(idx.end()) {}
		};

		// contains vector of iterators pointing to the requested indexes
		// iterator always points to the smallest document ID not yet pushed into resulting structure (or to client)
		// or discarded (if other index iterators point to larger document IDs)
		std::vector<iter> idata;
		idata.reserve(indexes.size());

		for_each(indexes.begin(), indexes.end(), [&] (const eurl &name) {
				iter it(m_t, name, start);

				idata.emplace_back(std::move(it));
			});

		result res;

		while (!res.completed) {
			// contains indexes within @idata array of iterators,
			// each iterator contains the same and smallest to the known moment reference to the document (i.e. document ID)
			//
			// if checking @idata array yelds smaller document ID than that in iterators referenced in @pos,
			// then we clear @pos and starts pushing the new smallest iterator indexes
			//
			// we could break out of the @idata processing, increase the smallest pointing iterator and start over,
			// but we optimize @idata processing - if there are other iterators in @idata which equal to the smallest
			// iterator value (document ID), we put them into @pos
			// Since @pos doesn't contain all indexes (its size doesn't equal to the size of @idata), we will increase
			// all iterators where we have found the smallest document ID, hopefully they will point to the new document ID,
			// which might be the same for all iterator among @idata and thus we will push this document ID to the result
			// structure returned to the client
			//
			// Here is an example:
			//
			// 1. @idata iterators	0	1	2	3
			//                      -------------------------
			// document ids		d0	d2	d3	d3
			// 			d2	d3	d4	d4
			// 			d3	d4	d5	d5
			// 			d4	-	-	-
			// 			d5	-	-	-
			//
			// We start from the top of this table, i.e. row after 'document ids' string
			// @pos will contain following values during iteration over @idata iterators
			// 	0 - select the first value
			// 	0 - skip iterator 1 (d2 document id) since its value is greater than that 0'th iterator value (d0)
			// 	0 - skip iterator 2
			// 	0 - skip iterator 3
			//
			// @pos contains only 0 index, it is not equal to the size of @idata (4), thus we have to increase 0'th iterator
			// discarding its first value
			//
			// 2. @idata iterators	0	1	2	3
			//                      -------------------------
			// document ids		d2	d2	d3	d3
			// 			d3	d3	d4	d4
			// 			d4	d4	d5	d5
			// 			d5	-	-	-
			// @pos:
			// 	0 - select the first iterator
			// 	0 1 - 1'th iterator value equals to the value of the 0'th iterator, append it to the array
			// 	0 1 - 2'th iterator value (d3) is greater than that of the 0'th iterator (d2)
			// 	0 1 - the same as above
			// since size of the @pos is not equal to the size of @idata we increment all iterators which are indexed in @pos
			//
			// 3. @idata iterators	0	1	2	3
			//                      -------------------------
			// document ids		d3	d3	d3	d3
			// 			d4	d4	d4	d4
			// 			d5	-	d5	d5
			// @pos will contain all 4 indexes, since all iterator's value are the same (d3)
			// We will increment all iterators and push d3 into resulting array which will be returned to the client,
			// since size of the @pos array equals to the @idata size
			//
			// 4. @idata iterators	0	1	2	3
			//                      -------------------------
			// document ids		d4	d4	d4	d4
			// 			d5	-	d5	d5
			// We put d4 into resulting array and increment all iterators as above
			//
			// 5. @idata iterators	0	1	2	3
			//                      -------------------------
			// document ids		d5	-	d5	d5
			//
			// @pos:
			// 	0 - select the first iterator
			// 	Stop processing, since 1'th iterator is empty. This means no further iteration checks can contain all 4 the same
			// 	value, thus it is not possible to find any other document with higher ID which will contain all 4 requested
			// 	indexes.
			//
			// 6. Return [d3, d4] values to the client
			std::vector<int> pos;

			int current = -1;
			for (auto idata_it = idata.begin(), idata_end = idata.end(); idata_it != idata_end; ++idata_it) {
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
				start.clear();
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
			if (res.docs.size() == num) {
				break;
			}

			single_doc_result rs;
			for (auto it = pos.begin(); it != pos.end(); ++it) {
				auto &min_it = idata[*it].begin;
				key k = *min_it;

				if (it == pos.begin()) {
					rs.doc.id = k.id;
					rs.doc.url = k.url;
				}

				key idx;
				idx.url = indexes[*it];
				idx.positions = k.positions;

				rs.indexes.push_back(idx);

				++min_it;
			}

			res.docs.emplace_back(rs);
		}

		return res;
	}
private:
	T &m_t;
};

}}} // namespace ioremap::greylock::intersect

#endif // __INDEXES_INTERSECTION_HPP
