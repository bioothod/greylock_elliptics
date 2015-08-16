#include <iostream>

#include "greylock/bucket_transport.hpp"
#include "greylock/elliptics.hpp"
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
		if (t.get_groups().size() > 1)
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

			idx.insert(k);
			keys.push_back(k);
		}

		ribosome::timer tm;
		printf("remove-test: meta before remove: %s\n", idx.meta().str().c_str());
		for (auto it = keys.begin(), end = keys.begin() + keys.size() / 2; it != end; ++it) {
			idx.remove(*it);
		}
		printf("remove-test: meta after remove: %s, removed entries: %zd, time: %ld ms\n",
				idx.meta().str().c_str(), keys.size() / 2, tm.elapsed());

		for (auto it = keys.begin(), end = keys.end(); it != end; ++it) {
			greylock::key found = idx.search(*it);
			if (it < keys.begin() + keys.size() / 2) {
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
		}
	}

	void test_index_recovery(T &t, int max) {
		std::vector<int> groups = t.get_groups();

		greylock::eurl name;
		name.key = "recovery-test." + elliptics::lexical_cast(rand());
		name.bucket = m_bucket;

		greylock::read_write_index<T> idx(t, name);

		std::vector<greylock::key> keys;

		for (int i = 0; i < max; ++i) {
			greylock::key k;
			k.id = elliptics::lexical_cast(rand()) + ".recovery-key." + elliptics::lexical_cast(i);
			k.url.key = "recovery-value." + elliptics::lexical_cast(i);
			k.url.bucket = m_bucket;

			int err = idx.insert(k);
			if (!err) {
				keys.push_back(k);
			}

			if (i == max / 2) {
				groups = t.get_groups();
				std::vector<int> tmp;
				tmp.insert(tmp.end(), groups.begin(), groups.begin() + groups.size() / 2);
				t.set_groups(tmp);
			}
		}

		t.set_groups(groups);
		ribosome::timer tm;
		// index constructor self-heals itself
		greylock::read_write_index<T> rec(t, name);

		groups = t.get_groups();
		std::vector<int> tmp;
		tmp.insert(tmp.end(), groups.begin() + groups.size() / 2, groups.end());
		t.set_groups(tmp);

		printf("recovery: index has been self-healed, records: %d, time: %ld ms, meta: %s, reading from groups: %s\n",
				max, tm.elapsed(), rec.meta().str().c_str(), rec.print_groups(t.get_groups()).c_str());


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

		t.set_groups(groups);
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

	void test_intersection(T &t, int num_greylock, size_t same_num, size_t different_num) {
		std::vector<greylock::eurl> greylock;
		std::vector<greylock::key> same;

		for (size_t i = 0; i < same_num; ++i) {
			greylock::key k;
			k.id = elliptics::lexical_cast(rand()) + ".url-same-key." + elliptics::lexical_cast(i);
			k.url.key = "url-same-data." + elliptics::lexical_cast(i);
			k.url.bucket = m_bucket;

			same.emplace_back(k);
		}

		for (int i = 0; i < num_greylock; ++i) {
			greylock::eurl url;
			url.bucket = m_bucket;
			url.key = "intersection-index.rand." + elliptics::lexical_cast(i) + "." + elliptics::lexical_cast(rand());
			greylock.push_back(url);

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
			index_checker(const greylock::intersect::result &res, size_t same_num) {
				size_t size = 0;
				for (auto it = res.keys.begin(), end = res.keys.end(); it != end; ++it) {
					if (!size) {
						size = it->second.size();
						continue;
					}

					if (size != it->second.size() || size != same_num) {
						std::ostringstream ss;
						ss << "intersection failed: greylock: " << res.keys.size() <<
							", same keys in each index: " << same_num <<
							", current-index: " << it->first.str() <<
							", found keys (must be equal to the same jeys in each index): " << it->second.size();
						throw std::runtime_error(ss.str());
					}
				}

				for (size_t i = 0; i < size; ++i) {
					greylock::key k;
					for (auto it = res.keys.begin(), end = res.keys.end(); it != end; ++it) {
						if (!k) {
							k = it->second[i];
							continue;
						}

						if (k != it->second[i]) {
							std::ostringstream ss;
							ss << "intersection failed: greylock: " << res.keys.size() <<
								", same keys in each index: " << same_num <<
								", current-index: " << it->first.str() <<
								", mismatch position: " << i <<
								", found key: " << it->second[i].str() <<
								", must be: " << k.str();
							throw std::runtime_error(ss.str());
						}
					}
				}
			}
		};

		ribosome::timer tm;
		greylock::intersect::intersector<T> inter(t);
		greylock::intersect::result res = inter.intersect(greylock);
		for (auto it = res.keys.begin(); it != res.keys.end(); ++it) {
			printf("index: %s, keys: %zd\n", it->first.str().c_str(), it->second.size());
			for (auto k = it->second.begin(); k != it->second.end(); ++k) {
				dprintf("  %s\n", k->str().c_str());
			}
		}

		printf("intersection: greylock: %d, found keys: %zd, must be: %zd, total keys in each index: %zd, time: %ld ms\n",
				num_greylock, res.keys.size(), same_num, same_num + different_num, tm.restart());

		index_checker c(res, same_num);

		greylock::intersect::intersector<T> p(t);
		std::string start("\0");
		size_t num = same_num / 10;
		size_t num_found = 0;

		while (true) {
			greylock::intersect::result res = p.intersect(greylock, start, num);

			if (!res.keys.size())
				break;

			size_t cur_size = res.keys.begin()->second.size();
			num_found += cur_size;

			for (auto it = res.keys.begin(); it != res.keys.end(); ++it) {
				printf("index: %s, keys: %zd, total keys found: %zd\n", it->first.str().c_str(), cur_size, num_found);
				for (auto k = it->second.begin(); k != it->second.end(); ++k) {
					dprintf("  %s\n", k->str().c_str());
				}
			}

			index_checker c(res, cur_size);

			if (cur_size < num)
				break;

			if (res.completed)
				break;
		}

		printf("paginated intersection: greylock: %d, found keys: %zd, must be: %zd, total keys in each index: %zd, time: %ld ms\n",
				num_greylock, num_found, same_num, same_num + different_num, tm.restart());
		if (num_found != same_num) {
			std::ostringstream ss;
			ss << "paginated intersection failed: greylock: " << num_greylock << ", same keys in each index: " << same_num <<
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

	greylock::elliptics_transport t(log_file, log_level);
	t.add_remotes(remotes);

	if (bnames.size() != 0) {
		greylock::bucket_transport bt(t.get_node());
		if (!bt.init(elliptics::parse_groups(groups.c_str()), bnames)) {
			std::cerr << "Could not initialize bucket transport, exiting";
			return -1;
		}

		bt.test();

		test<greylock::bucket_transport> tt(bt, bnames[0]);
	} else {
		t.set_groups(elliptics::parse_groups(groups.c_str()));
		test<greylock::elliptics_transport> tt(t, "");
	}
}
