#include "greylock/core.hpp"
#include "greylock/index.hpp"
#include "greylock/intersection.hpp"
#include "greylock/json.hpp"


#include <ebucket/bucket_processor.hpp>
#include <elliptics/session.hpp>

#include <unistd.h>
#include <signal.h>
#include <time.h>

#include <thevoid/server.hpp>
#include <thevoid/stream.hpp>

#include <thevoid/rapidjson/stringbuffer.h>
#include <thevoid/rapidjson/prettywriter.h>
#include <thevoid/rapidjson/document.h>

#include <ribosome/split.hpp>
#include <ribosome/timer.hpp>
#include <ribosome/distance.hpp>
#include <ribosome/vector_lock.hpp>

#include <swarm/logger.hpp>

#include <functional>
#include <string>
#include <thread>

#define ILOG(level, a...) BH_LOG(logger(), level, ##a)
#define ILOG_ERROR(a...) ILOG(SWARM_LOG_ERROR, ##a)
#define ILOG_WARNING(a...) ILOG(SWARM_LOG_WARNING, ##a)
#define ILOG_INFO(a...) ILOG(SWARM_LOG_INFO, ##a)
#define ILOG_NOTICE(a...) ILOG(SWARM_LOG_NOTICE, ##a)
#define ILOG_DEBUG(a...) ILOG(SWARM_LOG_DEBUG, ##a)

using namespace ioremap;

class JsonValue : public rapidjson::Value
{
public:
	JsonValue() {
		SetObject();
	}

	~JsonValue() {
	}

	static void set_time(rapidjson::Value &obj, rapidjson::Document::AllocatorType &alloc, long tsec, long usec) {
		char str[64];
		struct tm tm;

		localtime_r((time_t *)&tsec, &tm);
		strftime(str, sizeof(str), "%F %Z %R:%S", &tm);

		char time_str[128];
		snprintf(time_str, sizeof(time_str), "%s.%06lu", str, usec);

		obj.SetObject();

		rapidjson::Value tobj(time_str, strlen(time_str), alloc);
		obj.AddMember("time", tobj, alloc);

		std::string raw_time = elliptics::lexical_cast(tsec) + "." + elliptics::lexical_cast(usec);
		rapidjson::Value tobj_raw(raw_time.c_str(), raw_time.size(), alloc);
		obj.AddMember("time-raw", tobj_raw, alloc);
	}

	std::string ToString() const {
		rapidjson::StringBuffer buffer;
		rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);

		Accept(writer);
		buffer.Put('\n');

		return std::string(buffer.GetString(), buffer.Size());
	}

	rapidjson::MemoryPoolAllocator<> &GetAllocator() {
		return m_allocator;
	}

private:
	rapidjson::MemoryPoolAllocator<> m_allocator;
};

struct single_attribute {
	typedef std::vector<size_t> pos_t;

	std::string aname;
	pos_t ivec;

	// greylock operates with raw index names, it doesn't know whether they were organized into attributes or not
	// It returns array of raw index names and positions where those indexes live in each returned document.
	//
	// We have to split array of raw index names into per-attribute indexes
	// This @apos vector contains positions within raw index name vector of the indexes which belong to @aname attribute
	pos_t apos;
};

struct indexes_request {
	typedef std::vector<size_t> pos_t;

	indexes_request(indexes_request&& o) :
		inames(o.inames.str()),
		indexes(std::move(o.indexes)),
		positions(std::move(o.positions)),
		attributes(std::move(o.attributes)) {}

	indexes_request() {}

	std::ostringstream inames;

	std::vector<greylock::eurl> indexes;
	std::vector<pos_t> positions;

	std::vector<single_attribute> attributes;

	bool distance_sort(const std::vector<greylock::eurl> &indexes_unused, greylock::intersect::result &res) {
		(void) indexes_unused;

//#define STDOUT_DEBUG
#ifdef STDOUT_DEBUG
		auto print_vector = [] (const std::vector<size_t> &v) {
			std::ostringstream ss;
			ss << "[";
			for (auto it = v.begin(), end = v.end(); it != end;) {
				ss << (ssize_t)(*it);
				++it;

				if (it != end)
					ss << ", ";
			}
			ss << "]";

			return ss.str();
		};
		auto print_vectors = [&] (const std::vector<pos_t> &v) {
			std::ostringstream ss;
			for (auto it = v.begin(), end = v.end(); it != end;) {
				ss << print_vector(*it);
				++it;

				if (it != end)
					ss << ", ";
			}

			return ss.str();
		};
#endif

		for (size_t i = 0; i < indexes.size(); ++i) {
			const std::string &iname = indexes[i].key;

			for (auto &sa : attributes) {
				if (iname.find(sa.aname) == 0) {
					sa.apos.push_back(i);
					break;
				}
			}
		}

		for (auto &doc: res.docs) {
			for (const auto &sa: attributes) {
				std::vector<pos_t> positions;
				for (size_t i = 0; i < sa.apos.size(); ++i) {
					size_t ipos = sa.apos[i];

					positions.push_back(doc.indexes[ipos].positions);
#ifdef STDOUT_DEBUG
					printf("doc: %s, sa: %s, index: %s, positions: %s\n",
							doc.doc.str().c_str(), sa.aname.c_str(),
							doc.indexes[ipos].str().c_str(), print_vector(doc.indexes[ipos].positions).c_str());
#endif
				}

				std::vector<size_t> pos_idx;
				// fill in with zeroes, every entry in this array is an offset within corresponding entry in @positions array
				pos_idx.resize(positions.size());

				// This code converts index positions for given attribute in given document into index vector.
				//
				// Given document @doc, let's assume, there are following index positions for @sa attribute:
				//
				// idx0 positions: 0, 1, 4, 12
				// idx1 positions: 3, 7, 15
				// idx2 positions: 2, 13
				//
				//              : 0, 1, 2, 3, 4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15
				// index vectors: 0, 0, 2, 1, 0
				//                                        1
				//                                                            0,  2
				//                                                                        1

				std::vector<pos_t> dvecs;
				pos_t current_dvec;

				size_t prev;

				while (true) {
					size_t min = INT_MAX;
					int min_pos = -1;

					for (size_t i = 0; i < pos_idx.size(); ++i) {
						const pos_t &pos_vector = positions[i];
						size_t pos_offset = pos_idx[i];

						if (pos_offset < pos_vector.size()) {
							if (pos_vector[pos_offset] < min) {
								min = pos_vector[pos_offset];
								min_pos = i;
							}
						}
					}

					if (min_pos == -1) {
						dvecs.push_back(current_dvec);
						break;
					}

					if (current_dvec.size() == 0) {
						current_dvec.push_back(min_pos);
						pos_idx[min_pos]++;

						prev = min;
						continue;
					}

					if (min != prev + 1) {
						dvecs.push_back(current_dvec);
						current_dvec.clear();
					} else {
						current_dvec.push_back(min_pos);
						pos_idx[min_pos]++;
						prev = min;
					}
				}
#ifdef STDOUT_DEBUG
				printf("doc: %s, sa: %s, index vectors: %s\n",
						doc.doc.str().c_str(), sa.aname.c_str(),
						print_vectors(dvecs).c_str());
#endif
				int min_dist = INT_MAX;
				int min_num = 0;
				size_t total_length = 0;
				for (const auto &dvec: dvecs) {
					// Using sliding window with size equal to @ivec size for given attribute,
					// compare every subset of index vector with @ivec using Levenstein distance
					size_t start = 0;
					while (true) {
						std::vector<size_t> sub;

						size_t num = dvec.size() - start;
						if (num > sa.ivec.size())
							num = sa.ivec.size();

						sub.insert(sub.begin(), dvec.begin() + start, dvec.begin() + start + num);

						int dist = ribosome::distance::levenstein(sa.ivec, sub, INT_MAX);
						if (dist < min_dist) {
							min_dist = dist;
							min_num = 1;
						} else if (dist == min_dist) {
							min_num++;
						}
#ifdef STDOUT_DEBUG						
						printf("doc: %s, sa: %s, start: %zd, index vector: %s, dist: %d, min_dist: %d, min_num: %d\n",
								doc.doc.str().c_str(),
								sa.aname.c_str(),
								start,
								print_vector(sub).c_str(),
								dist,
								min_dist,
								min_num);
#endif

						if (num < sa.ivec.size())
							break;

						++start;
					}

					total_length += dvec.size();
				}

				doc.relevance = (1.0 - (float)min_dist / (float)sa.ivec.size()) * ((float)min_num / (float)total_length);
			}
		}

		std::sort(res.docs.begin(), res.docs.end(), [&]
				(const greylock::intersect::single_doc_result &d1, const greylock::intersect::single_doc_result &d2) {
					return d1.relevance > d2.relevance;
			});

		return true;
	}
};

class http_server : public thevoid::server<http_server>
{
public:
	virtual bool initialize(const rapidjson::Value &config) {
		if (!elliptics_init(config))
			return false;

		if (!greylock_init(config))
			return false;

		on<on_ping>(
			options::exact_match("/ping"),
			options::methods("GET")
		);

		on<on_index>(
			options::exact_match("/index"),
			options::methods("POST")
		);

		on<on_search>(
			options::exact_match("/search"),
			options::methods("POST")
		);

		return true;
	}

	struct on_ping : public thevoid::simple_request_stream<http_server> {
		virtual void on_request(const thevoid::http_request &req, const boost::asio::const_buffer &buffer) {
			(void) buffer;
			(void) req;

			this->send_reply(thevoid::http_response::ok);
		}
	};

	struct on_search : public thevoid::simple_request_stream<http_server> {
		virtual void on_request(const thevoid::http_request &req, const boost::asio::const_buffer &buffer) {
			ribosome::timer search_tm;
			ILOG_INFO("url: %s: start", req.url().to_human_readable().c_str());

			// this is needed to put ending zero-byte, otherwise rapidjson parser will explode
			std::string data(const_cast<char *>(boost::asio::buffer_cast<const char*>(buffer)), boost::asio::buffer_size(buffer));

			rapidjson::Document doc;
			doc.Parse<0>(data.c_str());

			if (doc.HasParseError()) {
				ILOG_ERROR("on_request: url: %s, error: %d: could not parse document: %s, error offset: %d",
						req.url().to_human_readable().c_str(), -EINVAL, doc.GetParseError(), doc.GetErrorOffset());
				this->send_reply(swarm::http_response::bad_request);
				return;
			}


			if (!doc.IsObject()) {
				ILOG_ERROR("on_request: url: %s, error: %d: document must be object",
						req.url().to_human_readable().c_str(), -EINVAL);
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			const char *mbox = greylock::get_string(doc, "mailbox");
			if (!mbox) {
				ILOG_ERROR("on_request: url: %s, error: %d: 'mailbox' must be a string",
						req.url().to_human_readable().c_str(), -EINVAL);
				this->send_reply(swarm::http_response::bad_request);
				return;
			}


			const rapidjson::Value &query = greylock::get_object(doc, "query");
			if (!query.IsObject()) {
				ILOG_ERROR("on_request: url: %s, mailbox: %s, error: %d: 'query' must be object",
						req.url().to_human_readable().c_str(), mbox, -EINVAL);
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			size_t page_num = ~0U;
			std::string page_start("\0");

			if (doc.HasMember("paging")) {
				const auto &pages = doc["paging"];
				page_num = greylock::get_int64(pages, "num", ~0U);
				page_start = greylock::get_string(pages, "start", "\0");
			}

			auto ireq = server()->get_indexes(mbox, query);

			greylock::intersect::result result;
			result.cookie = page_start;
			result.max_number_of_documents = page_num;

			ILOG_INFO("url: %s: indexes: %s: starting intersection, json parsing duration: %d ms",
					req.url().to_human_readable(), ireq.inames.str(), search_tm.elapsed());

			try {
				intersect(req, ireq, result);
			} catch (const std::exception &e) {
				// likely this exception tells that there are no requested indexes
				// FIXME exception mechanism has to be reworked
				ILOG_ERROR("url: %s: indexes: %s: could not run intersection for %d indexes: %s",
					req.url().to_human_readable(), ireq.inames.str(), ireq.indexes.size(), e.what());
				send_search_result(result);
				return;
			}

			const rapidjson::Value &match = greylock::get_object(doc, "match");
			if (match.IsObject()) {
				const char *match_type = greylock::get_string(match, "type");
				if (match_type) {
					if (strcmp(match_type, "and")) {
					} else if (strcmp(match_type, "phrase")) {
					}
				}
			}

			send_search_result(result);

			ILOG_INFO("url: %s: indexes: %s: requested indexes: %d, requested number of documents: %d, search start: %s, "
					"found documents: %d, cookie: %s, completed: %d, duration: %d ms",
					req.url().to_human_readable(),
					ireq.inames.str(), ireq.indexes.size(),
					page_num, page_start,
					result.docs.size(), result.cookie.c_str(), result.completed,
					search_tm.elapsed());
		}

		void send_search_result(const greylock::intersect::result &result) {
			JsonValue ret;
			auto &allocator = ret.GetAllocator();

			rapidjson::Value ids(rapidjson::kArrayType);
			for (auto it = result.docs.begin(), end = result.docs.end(); it != end; ++it) {
				rapidjson::Value key(rapidjson::kObjectType);

				const greylock::key &doc = it->doc;

				rapidjson::Value kv(doc.url.key.c_str(), doc.url.key.size(), allocator);
				key.AddMember("key", kv, allocator);

				rapidjson::Value bv(doc.url.bucket.c_str(), doc.url.bucket.size(), allocator);
				key.AddMember("bucket", bv, allocator);

				rapidjson::Value idv(doc.id.c_str(), doc.id.size(), allocator);
				key.AddMember("id", idv, allocator);

				key.AddMember("relevance", it->relevance, allocator);

				rapidjson::Value ts(rapidjson::kObjectType);
				long tsec, tnsec;
				doc.get_timestamp(tsec, tnsec);
				ts.AddMember("tsec", tsec, allocator);
				ts.AddMember("tnsec", tnsec, allocator);
				key.AddMember("timestamp", ts, allocator);

				ids.PushBack(key, allocator);
			}

			ret.AddMember("ids", ids, allocator);
			ret.AddMember("completed", result.completed, allocator);

			{
				rapidjson::Value page(rapidjson::kObjectType);
				page.AddMember("num", result.docs.size(), allocator);

				rapidjson::Value sv(result.cookie.c_str(), result.cookie.size(), allocator);
				page.AddMember("start", sv, allocator);

				ret.AddMember("paging", page, allocator);
			}

			std::string data = ret.ToString();

			thevoid::http_response reply;
			reply.set_code(swarm::http_response::ok);
			reply.headers().set_content_type("text/json; charset=utf-8");
			reply.headers().set_content_length(data.size());

			this->send_reply(std::move(reply), std::move(data));
		}

		bool intersect(const thevoid::http_request &req, indexes_request &ireq, greylock::intersect::result &result) {
			ribosome::timer tm;

			greylock::intersect::intersector p(*(server()->bucket()));

			std::vector<ribosome::locker<http_server>> lockers;
			lockers.reserve(ireq.indexes.size());

			std::vector<std::unique_lock<ribosome::locker<http_server>>> locks;
			locks.reserve(ireq.indexes.size());

			for (auto it = ireq.indexes.begin(), end = ireq.indexes.end(); it != end; ++it) {
				ribosome::locker<http_server> l(server(), it->str());
				lockers.emplace_back(std::move(l));

				std::unique_lock<ribosome::locker<http_server>> lk(lockers.back());
				locks.emplace_back(std::move(lk));
			}

			ILOG_INFO("url: %s: indexes: %s: intersection locked: duration: %d ms",
					req.url().to_human_readable(), ireq.inames.str(), tm.elapsed());

			ribosome::timer intersect_tm;
			result = p.intersect(ireq.indexes, result.cookie, result.max_number_of_documents,
					std::bind(&indexes_request::distance_sort, &ireq, std::placeholders::_1, std::placeholders::_2));

			ILOG_INFO("url: %s: indexes: %s: completed: %d, result keys: %d, requested num: %d, page start: %s: "
					"intersection completed: duration: %d ms, whole duration: %d ms",
					req.url().to_human_readable(), ireq.inames.str(),
					result.completed, result.docs.size(),
					result.max_number_of_documents, result.cookie,
					intersect_tm.elapsed(), tm.elapsed());

			return result.completed;
		}
	};

	struct on_index : public thevoid::simple_request_stream<http_server> {
		elliptics::error_info process_one_document(const thevoid::http_request &req, const std::string &mbox,
				greylock::key &doc, const rapidjson::Value &idxs) {
			ribosome::timer all_tm;
			ribosome::timer tm;

			ILOG_INFO("process_one_document: url: %s, mailbox: %s, doc: %s: start insertion",
					req.url().to_human_readable().c_str(), mbox.c_str(),
					doc.str().c_str());

			auto ireq = server()->get_indexes(mbox, idxs);

			if (ireq.indexes.size() == 0) {
				return elliptics::create_error(-EINVAL,
						"process_one_document: url: %s, mailbox: %s, doc: %s: no valid indexes",
						req.url().to_human_readable().c_str(), mbox.c_str(),
						doc.str().c_str());
			}

			for (size_t i = 0; i < ireq.indexes.size(); ++i) {
				greylock::eurl &iname = ireq.indexes[i];
				std::vector<size_t> &positions = ireq.positions[i];

				// for every index we put vector of positions where given index is located in the document
				// since it is an inverted index, it contains list of document links each of which contains
				// array of the positions, where given index lives in the document
				doc.positions.swap(positions);

				ribosome::locker<http_server> l(server(), iname.str());
				std::unique_lock<ribosome::locker<http_server>> lk(l);

				try {
					greylock::read_write_index index(*(server()->bucket()), iname);

					elliptics::error_info err = index.insert(doc);
					if (err) {
						return elliptics::create_error(err.code(), "process_one_document: url: %s, mailbox: %s, "
								"doc: %s, index: %s: could not insert new key: %s [%d]",
							req.url().to_human_readable().c_str(), mbox.c_str(),
							doc.str().c_str(),
							iname.str().c_str(),
							err.message().c_str(),
							err.code());
					}
				} catch (const std::exception &e) {
					return elliptics::create_error(-EINVAL, "process_one_document: url: %s, mailbox: %s, "
							"doc: %s, index: %s, exception: %s",
							req.url().to_human_readable().c_str(), mbox.c_str(),
							doc.str().c_str(),
							iname.str().c_str(),
							e.what());
				}

				ILOG_INFO("process_one_document: url: %s, mailbox: %s, "
						"doc: %s, index: %s, elapsed time: %d ms",
					req.url().to_human_readable().c_str(), mbox,
					doc.str().c_str(),
					iname.str().c_str(),
					tm.restart());
			}

			ILOG_INFO("process_one_document: url: %s, mailbox: %s, doc: %s, total number of indexes: %d, elapsed time: %d ms",
					req.url().to_human_readable().c_str(), mbox,
					doc.str().c_str(), ireq.indexes.size(), all_tm.elapsed());

			return elliptics::error_info();
		}

		elliptics::error_info parse_docs(const thevoid::http_request &req, const std::string &mbox, const rapidjson::Value &docs) {
			elliptics::error_info err = elliptics::create_error(-EINVAL,
					"url: %s, mbox: %s: could not parse document, there are no valid index entries",
					req.url().to_human_readable().c_str(), mbox.c_str());

			for (auto it = docs.Begin(), id_end = docs.End(); it != id_end; ++it) {
				if (it->IsObject()) {
					greylock::key doc;

					const char *id = greylock::get_string(*it, "id");
					const char *bucket = greylock::get_string(*it, "bucket");
					const char *key = greylock::get_string(*it, "key");
					if (!bucket || !key || !id) {
						ILOG_ERROR("parse_docs: url: %s, mailbox: %s, error: %d: 'docs/{bucket,ket,id} must be strings",
								req.url().to_human_readable().c_str(), mbox, -EINVAL);
						continue;
					}

					struct timespec ts;
					clock_gettime(CLOCK_REALTIME, &ts);

					const rapidjson::Value &timestamp = greylock::get_object(*it, "timestamp");
					if (timestamp.IsObject()) {
						long tsec, tnsec;

						tsec = greylock::get_int64(timestamp, "tsec", ts.tv_sec);
						tnsec = greylock::get_int64(timestamp, "tnsec", ts.tv_nsec);

						doc.set_timestamp(tsec, tnsec);
					} else {
						doc.set_timestamp(ts.tv_sec, ts.tv_nsec);
					}

					doc.url.bucket.assign(bucket);
					doc.url.key.assign(key);
					doc.id.assign(id);

					const rapidjson::Value &idxs = greylock::get_object(*it, "index");
					if (!idxs.IsObject()) {
						ILOG_ERROR("parse_docs: url: %s, mailbox: %s, doc: %s, error: %d: 'docs/index' must be object",
								req.url().to_human_readable().c_str(), mbox, doc.str().c_str(), -EINVAL);
						continue;
					}

					err = process_one_document(req, mbox, doc, idxs);
					if (err)
						return err;
				}
			}

			return err;
		}

		virtual void on_request(const thevoid::http_request &req, const boost::asio::const_buffer &buffer) {
			ribosome::timer index_tm;
			ILOG_INFO("url: %s: start", req.url().to_human_readable().c_str());

			// this is needed to put ending zero-byte, otherwise rapidjson parser will explode
			std::string data(const_cast<char *>(boost::asio::buffer_cast<const char*>(buffer)), boost::asio::buffer_size(buffer));

			rapidjson::Document doc;
			doc.Parse<0>(data.c_str());

			if (doc.HasParseError()) {
				ILOG_ERROR("on_request: url: %s, error: %d: could not parse document: %s, error offset: %d",
						req.url().to_human_readable().c_str(), -EINVAL, doc.GetParseError(), doc.GetErrorOffset());
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			if (!doc.IsObject()) {
				ILOG_ERROR("on_request: url: %s, error: %d: document must be object, its type: %d",
						req.url().to_human_readable().c_str(), -EINVAL, doc.GetType());
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			const char *mbox = greylock::get_string(doc, "mailbox");
			if (!mbox) {
				ILOG_ERROR("on_request: url: %s, error: %d: 'mailbox' must be a string",
						req.url().to_human_readable().c_str(), -EINVAL);
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			const rapidjson::Value &docs = greylock::get_array(doc, "docs");
			if (!docs.IsArray()) {
				ILOG_ERROR("on_request: url: %s, mailbox: %s, error: %d: 'docs' must be array",
						req.url().to_human_readable().c_str(), mbox, -EINVAL);
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			elliptics::error_info err = parse_docs(req, mbox, docs);
			if (err) {
				ILOG_ERROR("on_request: url: %s, mailbox: %s, keys: %d: insertion error: %s [%d]",
					req.url().to_human_readable().c_str(), mbox, docs.Size(), err.message(), err.code());
				this->send_reply(swarm::http_response::bad_request);
				return;
			}

			ILOG_INFO("on_request: url: %s, mailbox: %s, keys: %d: insertion completed, index duration: %d ms",
					req.url().to_human_readable().c_str(), mbox, docs.Size(), index_tm.elapsed());
			this->send_reply(thevoid::http_response::ok);
		}
	};

	std::shared_ptr<ebucket::bucket_processor> bucket() {
		return m_bucket;
	}

	void lock(const std::string &key) {
		m_lock.lock(key);
	}

	void unlock(const std::string &key) {
		m_lock.unlock(key);
	}

	const std::string &meta_bucket_name() const {
		return m_meta_bucket;
	}

	indexes_request get_indexes(const std::string &mbox, const rapidjson::Value &idxs) {
		indexes_request ireq;

		if (!idxs.IsObject())
			return ireq;

		ribosome::split spl;
		for (rapidjson::Value::ConstMemberIterator it = idxs.MemberBegin(), idxs_end = idxs.MemberEnd(); it != idxs_end; ++it) {
			const char *aname = it->name.GetString();
			const rapidjson::Value &avalue = it->value;

			if (!avalue.IsString())
				continue;

			single_attribute sa;
			sa.aname = index_name(mbox, aname, "");

			std::vector<ribosome::lstring> indexes = spl.convert_split_words(avalue.GetString(), avalue.GetStringLength());
			for (size_t pos = 0; pos < indexes.size(); ++pos) {
				greylock::eurl url;
				url.bucket = meta_bucket_name();
				url.key = index_name(mbox, aname, ribosome::lconvert::to_string(indexes[pos]));

				auto f = std::find(ireq.indexes.begin(), ireq.indexes.end(), url);
				if (f == ireq.indexes.end()) {
					std::vector<size_t> v;
					v.push_back(pos);
					ireq.positions.push_back(v);
					ireq.indexes.push_back(url);

					sa.ivec.push_back(pos);
				} else {
					size_t idx = std::distance(ireq.indexes.begin(), f);
					ireq.positions[idx].push_back(pos);
					sa.ivec.push_back(idx);
				}
			}

			ireq.attributes.push_back(sa);
		}

		for (auto it = ireq.indexes.begin(), end = ireq.indexes.end(); it != end;) {
			ireq.inames << it->str();
			++it;

			if (it != end)
				ireq.inames << " ";
		}

		return ireq;
	}


private:
	ribosome::vector_lock m_lock;

	std::shared_ptr<elliptics::node> m_node;

	std::string m_meta_bucket;
	std::shared_ptr<ebucket::bucket_processor> m_bucket;

	long m_read_timeout = 60;
	long m_write_timeout = 60;

	bool elliptics_init(const rapidjson::Value &config) {
		dnet_config node_config;
		memset(&node_config, 0, sizeof(node_config));

		if (!prepare_config(config, node_config)) {
			return false;
		}

		m_node.reset(new elliptics::node(std::move(swarm::logger(this->logger(), blackhole::log::attributes_t())), node_config));

		if (!prepare_node(config, *m_node)) {
			return false;
		}

		m_bucket.reset(new ebucket::bucket_processor(m_node));

		if (!prepare_session(config)) {
			return false;
		}

		if (!prepare_buckets(config))
			return false;

		return true;
	}

	bool prepare_config(const rapidjson::Value &config, dnet_config &node_config) {
		if (config.HasMember("io-thread-num")) {
			node_config.io_thread_num = config["io-thread-num"].GetInt();
		}
		if (config.HasMember("nonblocking-io-thread-num")) {
			node_config.nonblocking_io_thread_num = config["nonblocking-io-thread-num"].GetInt();
		}
		if (config.HasMember("net-thread-num")) {
			node_config.net_thread_num = config["net-thread-num"].GetInt();
		}

		return true;
	}

	bool prepare_node(const rapidjson::Value &config, elliptics::node &node) {
		if (!config.HasMember("remotes")) {
			ILOG_ERROR("\"application.remotes\" field is missed");
			return false;
		}

		std::vector<elliptics::address> remotes;

		auto &remotesArray = config["remotes"];
		for (auto it = remotesArray.Begin(), end = remotesArray.End(); it != end; ++it) {
				if (it->IsString()) {
					remotes.push_back(it->GetString());
				}
		}

		try {
			node.add_remote(remotes);
			elliptics::session session(node);

			if (!session.get_routes().size()) {
				ILOG_ERROR("Didn't add any remote node, exiting.");
				return false;
			}
		} catch (const std::exception &e) {
			ILOG_ERROR("Could not add any out of %d nodes.", remotes.size());
			return false;
		}

		return true;
	}

	bool prepare_session(const rapidjson::Value &config) {
		if (config.HasMember("read-timeout")) {
			auto &tm = config["read-timeout"];
			if (tm.IsInt())
				m_read_timeout = tm.GetInt();
		}

		if (config.HasMember("write-timeout")) {
			auto &tm = config["write-timeout"];
			if (tm.IsInt())
				m_write_timeout = tm.GetInt();
		}

		return true;
	}

	bool prepare_buckets(const rapidjson::Value &config) {
		if (!config.HasMember("buckets")) {
			ILOG_ERROR("\"application.buckets\" field is missed");
			return false;
		}

		auto &buckets = config["buckets"];

		std::set<std::string> bnames;
		for (auto it = buckets.Begin(), end = buckets.End(); it != end; ++it) {
			if (it->IsString()) {
				bnames.insert(it->GetString());
			}
		}

		if (!config.HasMember("meta-bucket")) {
			ILOG_ERROR("\"application.meta-bucket\" field is missed");
			return false;
		}

		// XXX there should be no meta bucket.
		// XXX meta bucket was created to host all index starts (i.e. index metadata and the first page)
		// XXX but it should be dynamically allocated just like all other pages
		// XXX instead of using @m_meta_bucket/@meta_bucket_name() server should grab
		// XXX new bucket via @get_bucket(), and client has to provide bucket name in the index methods.
		auto &meta_bucket = config["meta-bucket"];
		if (!meta_bucket.IsString()) {
			ILOG_ERROR("\"application.meta-bucket\" must be string");
			return false;
		}
		m_meta_bucket = meta_bucket.GetString();
		bnames.insert(m_meta_bucket);


		if (!config.HasMember("metadata-groups")) {
			ILOG_ERROR("\"application.metadata-groups\" field is missed");
			return false;
		}

		std::vector<int> mgroups;
		auto &groups_meta_array = config["metadata-groups"];
		for (auto it = groups_meta_array.Begin(), end = groups_meta_array.End(); it != end; ++it) {
			if (it->IsInt())
				mgroups.push_back(it->GetInt());
		}

		if (!m_bucket->init(mgroups, std::vector<std::string>(bnames.begin(), bnames.end())))
			return false;

		return true;
	}

	bool greylock_init(const rapidjson::Value &config) {
		if (config.HasMember("max-page-size")) {
			auto &ps = config["max-page-size"];
			if (ps.IsNumber())
				ioremap::greylock::max_page_size = ps.GetInt();
		}

		if (config.HasMember("reserve-size")) {
			auto &ps = config["reserve-size"];
			if (ps.IsNumber())
				ioremap::greylock::default_reserve_size = ps.GetInt();
		}

		return true;
	}

	std::string index_name(const std::string &mbox, const std::string &aname, const std::string &iname) {
		std::string ret = std::string(mbox) + "." + std::string(aname) + "." + std::string(iname);
		return ret;
	}

};

int main(int argc, char **argv)
{
	ioremap::thevoid::register_signal_handler(SIGINT, ioremap::thevoid::handle_stop_signal);
	ioremap::thevoid::register_signal_handler(SIGTERM, ioremap::thevoid::handle_stop_signal);
	ioremap::thevoid::register_signal_handler(SIGHUP, ioremap::thevoid::handle_reload_signal);
	ioremap::thevoid::register_signal_handler(SIGUSR1, ioremap::thevoid::handle_ignore_signal);
	ioremap::thevoid::register_signal_handler(SIGUSR2, ioremap::thevoid::handle_ignore_signal);

	ioremap::thevoid::run_signal_thread();

	auto server = ioremap::thevoid::create_server<http_server>();
	int err = server->run(argc, argv);

	ioremap::thevoid::stop_signal_thread();

	return err;
}

