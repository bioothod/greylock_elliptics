#include <algorithm>
#include <iostream>

#include "greylock/bucket_processor.hpp"
#include "greylock/intersection.hpp"

#include <boost/program_options.hpp>

#include <ribosome/timer.hpp>

using namespace ioremap;

template <typename T>
class test {
#define __stringify_1(x...)     (const char *)#x
#define __stringify(x...)       __stringify_1(x)
#define func(name, args...) __stringify(name), name, ##args
public:
	test(T &t, const std::string &bname) : m_bucket(bname) {
		greylock::eurl start;
		start.key = "test" + elliptics::lexical_cast(rand());
		start.bucket = m_bucket;

		greylock::read_write_index<T> idx(t, start);

		test::run(this, func(&test::test_remove_some_keys, t, 10000));

		std::vector<greylock::key> keys;
		test::run(this, func(&test::test_index_recovery, t, 10000));
		test::run(this, func(&test::test_insert_many_keys, idx, keys, 10000));
		test::run(this, func(&test::test_page_iterator, idx));
		test::run(this, func(&test::test_iterator_number, idx, keys));
		test::run(this, func(&test::test_select_many_keys, idx, keys));
		test::run(this, func(&test::test_intersection, t, 3, 5000, 10000));
	}

private:
	std::string m_bucket = "";

	template <typename Class, typename Method, typename... Args>
	static inline void run(Class *obj, const char *str, Method method, Args &&...args) {
		try {
			ribosome::timer tm;
			(obj->*method)(std::forward<Args>(args)...);
			printf("%s: %zd ms\n", str, tm.elapsed());
		} catch (const std::exception &e) {
			fprintf(stderr, "%s: failed: %s\n", str, e.what());
			exit(-1);
		}
	}

	void test_insert_many_keys(greylock::read_write_index<T> &idx, std::vector<greylock::key> &keys, int max) {
		for (int i = 0; i < max; ++i) {
			greylock::key k;

			char buf[128];

			snprintf(buf, sizeof(buf), "%08x.%08d", rand(), i);
			k.id = std::string(buf);

			snprintf(buf, sizeof(buf), "some-data.%08d", i);
			k.url.key = std::string(buf);
			k.url.bucket = m_bucket;

			dprintf("inserting: %s\n", k.str().c_str());
			idx.insert(k);
			keys.push_back(k);
			dprintf("inserted: %s\n\n", k.str().c_str());
		}
	}

	void test_remove_some_keys(T &t, int max) {
		greylock::eurl start;
		start.key = "remove-test-index." + elliptics::lexical_cast(rand());
		start.bucket = m_bucket;

		greylock::read_write_index<T> idx(t, start);
		std::vector<greylock::key> keys;

		for (int i = 0; i < max; ++i) {
			greylock::key k;

			char buf[128];

			snprintf(buf, sizeof(buf), "%08x.remove-test.%08d", rand(), i);
			k.id = std::string(buf);

			snprintf(buf, sizeof(buf), "some-data.%08d", i);
			k.url.key = std::string(buf);
			k.url.bucket = m_bucket;

			elliptics::error_info err = idx.insert(k);
			if (err) {
				printf("remote-test: insert key: %s, error: %s [%d]\n", k.str().c_str(), err.message().c_str(), err.code());
				std::ostringstream ss;
				ss << "failed to insert key: " << k.str() << ": " << err.message();
				throw std::runtime_error(ss.str());
			}
			keys.push_back(k);
		}

		ribosome::timer tm;
		printf("remove-test: meta before remove: %s\n", idx.meta().str().c_str());
		int del_num = keys.size() / 2;
		for (auto it = keys.begin(), end = keys.begin() + del_num; it != end; ++it) {
			elliptics::error_info err = idx.remove(*it);
			if (err) {
				printf("remote-test: remove key: %s, error: %s [%d]\n", it->str().c_str(), err.message().c_str(), err.code());
				std::ostringstream ss;
				ss << "failed to remove key: " << it->str() << ": " << err.message();
				throw std::runtime_error(ss.str());
			}
		}
		printf("remove-test: meta after remove: %s, removed entries: %zd, time: %ld ms\n",
				idx.meta().str().c_str(), keys.size() / 2, tm.elapsed());

		if (idx.meta().num_keys != keys.size() - del_num) {
			printf("remove-test: number of keys mismatch: meta: %s, inserted keys: %zd, removed entries: %d, "
				"meta.num_keys must be: %ld, but it is: %lld\n",
				idx.meta().str().c_str(), keys.size(), del_num, keys.size() - del_num, idx.meta().num_keys.load());

			std::ostringstream ss;
			ss << "remove-test: number of keys mismatch" <<
				": meta: " << idx.meta().str() <<
				", inserted keys: " << keys.size() <<
				", removed keys: " << del_num <<
				", meta.num_keys must be: " << keys.size() - del_num <<
				", but it is: " << idx.meta().num_keys.load();
			throw std::runtime_error(ss.str());
		}

		int pos = 0;
		for (auto it = keys.begin(), end = keys.end(); it != end; ++it) {
			greylock::key found = idx.search(*it);
			if (pos < del_num) {
				if (found) {
					std::ostringstream ss;
					ss << "key: " << it->str() << " has been found, but it was removed";
					throw std::runtime_error(ss.str());
				}
			} else {
				if (!found) {
					std::ostringstream ss;
					ss << "key: " << it->str() << " has not been found, but it was not removed";
					throw std::runtime_error(ss.str());
				}
			}

			++pos;
		}
	}

	void test_index_recovery(T &t, int max) {
#if 0
		greylock::eurl name;
		name.key = "recovery-test." + elliptics::lexical_cast(rand());
		name.bucket = m_bucket;

		greylock::read_write_index<T> idx(t, name);
		std::vector<int> groups = idx.get_groups();

		std::vector<greylock::key> keys;

		for (int i = 0; i < max; ++i) {
			greylock::key k;
			k.id = elliptics::lexical_cast(rand()) + ".recovery-key." + elliptics::lexical_cast(i);
			k.url.key = "recovery-value." + elliptics::lexical_cast(i);
			k.url.bucket = m_bucket;

			elliptics::error_info err = idx.insert(k);
			if (!err) {
				keys.push_back(k);
			}

			if (i == max / 2) {
				groups = idx.get_groups();
				std::vector<int> tmp;
				tmp.insert(tmp.end(), groups.begin(), groups.begin() + groups.size() / 2);
				idx.set_groups(tmp);
			}
		}

		idx.set_groups(groups);
		ribosome::timer tm;
		// index constructor self-heals itself
		greylock::read_write_index<T> rec(t, name);

		groups = rec.get_groups();
		std::vector<int> tmp;
		tmp.insert(tmp.end(), groups.begin() + groups.size() / 2, groups.end());
		rec.set_groups(tmp);

		printf("recovery: index has been self-healed, records: %d, time: %ld ms, meta: %s, reading from groups: %s\n",
				max, tm.elapsed(), rec.meta().str().c_str(), rec.print_groups(rec.get_groups()).c_str());


		for (auto it = keys.begin(); it != keys.end(); ++it) {
			greylock::key s;
			s.id = it->id;

			greylock::key found = rec.search(s);
			if (!found) {
				std::ostringstream ss;
				ss << "search failed: could not find key: " << it->id.c_str();
				throw std::runtime_error(ss.str());
			}

			if (found.id != it->id) {
				std::ostringstream ss;
				ss << "search failed: ID mismatch: found: " << found << ", must be: " << *it;
				throw std::runtime_error(ss.str());
			}

			if (found.url != it->url) {
				std::ostringstream ss;
				ss << "search failed: url/value mismatch: found: " << found << ", must be: " << *it;
				throw std::runtime_error(ss.str());
			}

			dprintf("search: key: %s, url/value: %s\n", found.id.c_str(), found.url.str().c_str());
		}

		rec.set_groups(groups);
#endif
	}

	void test_select_many_keys(greylock::read_write_index<T> &idx, std::vector<greylock::key> &keys) {
		for (auto it = keys.begin(); it != keys.end(); ++it) {
			greylock::key k;

			k.id = it->id;
			greylock::key found = idx.search(k);
			if (!found) {
				std::ostringstream ss;
				ss << "search failed: could not find key: " << it->id.c_str();
				throw std::runtime_error(ss.str());
			}

			if (found.id != it->id) {
				std::ostringstream ss;
				ss << "search failed: ID mismatch: found: " << found << ", must be: " << *it;
				throw std::runtime_error(ss.str());
			}
			if (found.url != it->url) {
				std::ostringstream ss;
				ss << "search failed: url/value mismatch: found: " << found << ", must be: " << *it;
				throw std::runtime_error(ss.str());
			}

			dprintf("search: key: %s, url/value: %s\n\n", found.id.c_str(), found.value.str().c_str());
		}
	}

	void test_iterator_number(greylock::read_write_index<T> &idx, std::vector<greylock::key> &keys) {
		size_t num = 0;
		for (auto it = idx.begin(), end = idx.end(); it != end; ++it) {
			dprintf("iterator: %s\n", it->str().c_str());
			num++;
		}

		if (num != keys.size()) {
			std::ostringstream ss;
			ss << "iterated number mismatch: keys: " << keys.size() << ", iterated: " << num;
			throw std::runtime_error(ss.str());
		}
	}

	void test_page_iterator(greylock::read_write_index<T> &idx) {
		size_t page_num = 0;
		size_t leaf_num = 0;
		for (auto it = idx.page_begin(), end = idx.page_end(); it != end; ++it) {
			dprintf("page: %s: %s\n", it.path().c_str(), it->str().c_str());
			page_num++;
			if (it->is_leaf())
				leaf_num++;
		}
		greylock::index_meta meta = idx.meta();
		printf("meta: %s\n", meta.str().c_str());

		if (page_num != meta.num_pages) {
			std::ostringstream ss;
			ss << "page iterator: number of pages mismatch: "
				"meta: " << meta.str() <<
				"iterated: number of pages: " << page_num <<
				", number of leaf pages: " << leaf_num <<
				std::endl;
			throw std::runtime_error(ss.str());
		}

		if (leaf_num != meta.num_leaf_pages) {
			std::ostringstream ss;
			ss << "page iterator: number of leaf pages mismatch: "
				"meta: " << meta.str() <<
				"iterated: number of pages: " << page_num <<
				", number of leaf pages: " << leaf_num <<
				std::endl;
			throw std::runtime_error(ss.str());
		}
	}

	void test_intersection(T &t, int num_indexes, size_t same_num, size_t different_num) {
		std::vector<greylock::eurl> indexes;
		std::vector<greylock::key> same; // documents which are present in every index

		for (size_t i = 0; i < same_num; ++i) {
			greylock::key k;
			k.id = elliptics::lexical_cast(rand()) + ".url-same-key." + elliptics::lexical_cast(i);
			k.url.key = "url-same-data." + elliptics::lexical_cast(i);
			k.url.bucket = m_bucket;

			same.emplace_back(k);
		}

		for (int i = 0; i < num_indexes; ++i) {
			greylock::eurl url;
			url.bucket = m_bucket;
			url.key = "intersection-index.rand." + elliptics::lexical_cast(i) + "." + elliptics::lexical_cast(rand());
			indexes.push_back(url);

			greylock::read_write_index<T> idx(t, url);

			for (size_t j = 0; j < different_num; ++j) {
				greylock::key k;

				k.id = elliptics::lexical_cast(rand()) + ".url-random-key." + elliptics::lexical_cast(i);
				k.url.key = "url-random-data." + elliptics::lexical_cast(i);
				k.url.bucket = m_bucket;

				idx.insert(k);
			}

			for (auto it = same.begin(); it != same.end(); ++it) {
				idx.insert(*it);
			}
		}

		struct index_checker {
			index_checker(const greylock::intersect::result &res, const std::vector<greylock::eurl> &requested_indexes, size_t same_num) {
				if (res.docs.size() != same_num) {
					std::ostringstream ss;
					ss << "intersection failed: total number of documents found: " << res.docs.size() <<
						", number of documents must be: " << same_num;
					throw std::runtime_error(ss.str());
				}

				for (auto doc = res.docs.begin(), end = res.docs.end(); doc != end; ++doc) {
					if (doc->indexes.size() != requested_indexes.size()) {
						std::ostringstream ss;
						ss << "intersection failed: number of requested indexes: " << requested_indexes.size() <<
							", current document: " << doc->doc.str() <<
							", number of indexes in the document: " << doc->indexes.size();
						throw std::runtime_error(ss.str());
					}

					for (auto req = requested_indexes.begin(), req_end = requested_indexes.end(); req != req_end; ++req) {
						auto f = std::find_if(doc->indexes.begin(), doc->indexes.end(),
								[&] (const greylock::key &k) { return *req == k.url; });
						if (f == doc->indexes.end()) {
							std::ostringstream ss;

							ss << "intersection failed: could not find requested index: " << req->str() <<
								", in the document indexes" <<
								": document: " << doc->doc.str() <<
								", document indexes: [";
							for (auto doc_idx = doc->indexes.begin(), doc_idx_end = doc->indexes.end();
									doc_idx != doc_idx_end; ++doc_idx) {
								ss << doc_idx->str();
								if (doc_idx == doc->indexes.begin()) {
									ss << ", ";
								}
							}
							ss << "]";

							throw std::runtime_error(ss.str());
						}
					}
				}
			}
		};

		ribosome::timer tm;
		greylock::intersect::intersector<T> inter(t);
		greylock::intersect::result res = inter.intersect(indexes);

		auto check_intersection = [&] () {
			for (auto it = res.docs.begin(); it != res.docs.end(); ++it) {
				dprintf("document: %s, indexes: ", it->doc.str().c_str());
				for (auto k = it->indexes.begin(); k != it->indexes.end(); ++k) {
					dprintf("%s ", k->str().c_str());
				}
				dprintf("\n");
			}

			printf("intersection: requested number of indexes: %d, found documents: %zd, must be: %zd, total number of documents: %zd, "
					"total indexes in each document: %zd, time: %ld ms\n",
					num_indexes, res.docs.size(), same_num, same_num + different_num,
					res.docs[0].indexes.size(), tm.restart());

			index_checker c(res, indexes, same_num);
		};

		check_intersection();

		greylock::intersect::intersector<T> p(t);
		std::string start("\0");
		size_t num = same_num / 10;
		size_t num_found = 0;

		while (true) {
			greylock::intersect::result res = p.intersect(indexes, start, num);

			if (!res.docs.size())
				break;

			size_t cur_size = res.docs.size();
			num_found += cur_size;

			check_intersection();

			if (cur_size < num)
				break;

			if (res.completed)
				break;
		}

		printf("paginated intersection: requested number of indexes: %d, found documents: %zd, must be: %zd, total number of documents: %zd, "
				"total indexes in each document: %zd, time: %ld ms\n",
				num_indexes, num_found, same_num, same_num + different_num,
				res.docs[0].indexes.size(), tm.restart());

		if (num_found != same_num) {
			std::ostringstream ss;
			ss << "paginated intersection failed: indexes: " << num_indexes << ", same keys in each index: " << same_num <<
				", found keys: " << num_found <<
				", total keys in each index: " << different_num + same_num;
			throw std::runtime_error(ss.str());
		}
	}
};

int main(int argc, char *argv[])
{
	namespace bpo = boost::program_options;

	std::vector<std::string> remotes;


	bpo::options_description generic("Index test options");
	generic.add_options()
		("help", "this help message")
		;

	std::vector<std::string> bnames;
	std::string log_file, log_level, groups;
	bpo::options_description ell("Elliptics options");
	ell.add_options()
		("remote", bpo::value<std::vector<std::string>>(&remotes)->required()->composing(), "remote node: addr:port:family")
		("log-file", bpo::value<std::string>(&log_file)->default_value("/dev/stdout"), "log file")
		("log-level", bpo::value<std::string>(&log_level)->default_value("error"), "log level: error, info, notice, debug")
		("groups", bpo::value<std::string>(&groups)->required(), "groups where index tree is stored: 1:2:3")
		("bucket", bpo::value<std::vector<std::string>>(&bnames)->composing(), "use this bucket in tests")
		;

	bpo::options_description cmdline_options;
	cmdline_options.add(generic).add(ell);

	bpo::variables_map vm;

	try {
		bpo::store(bpo::command_line_parser(argc, argv).options(cmdline_options).run(), vm);

		if (vm.count("help")) {
			std::cout << cmdline_options << std::endl;
			return 0;
		}

		bpo::notify(vm);
	} catch (const std::exception &e) {
		std::cerr << "Invalid options: " << e.what() << "\n" << cmdline_options << std::endl;
		return -1;
	}

	time_t tm = time(NULL);
	srand(tm);

	dprintf("index: init: t: %zd\n", tm);

	elliptics::file_logger log(log_file.c_str(), elliptics::file_logger::parse_level(log_level));
	std::shared_ptr<elliptics::node> node(new elliptics::node(elliptics::logger(log, blackhole::log::attributes_t())));

	std::vector<elliptics::address> rem(remotes.begin(), remotes.end());
	node->add_remote(rem);

	greylock::bucket_processor bt(node);

	if (!bt.init(elliptics::parse_groups(groups.c_str()), bnames)) {
		std::cerr << "Could not initialize bucket transport, exiting";
		return -1;
	}

	bt.test();

	test<greylock::bucket_processor> tt(bt, bnames[0]);
	return 0;
}
