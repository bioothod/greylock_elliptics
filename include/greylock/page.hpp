#ifndef __INDEXES_PAGE_HPP
#define __INDEXES_PAGE_HPP

#include "greylock/error.hpp"
#include "greylock/key.hpp"

#include <iterator>
#include <vector>

#include <lz4frame.h>

namespace ioremap { namespace greylock {

#define PAGE_LEAF		(1<<0)

struct page {
	uint32_t flags = 0;
	std::vector<greylock::key> objects;
	size_t total_size = 0;
	eurl next;

	enum {
		serialization_version_raw = 1,
		serialization_version_packed,
		serialization_version_max,
	};

	page(bool leaf = false) {
		if (leaf) {
			flags = PAGE_LEAF;
		}
	}

	bool is_empty() const {
		return objects.size() == 0;
	}

	bool operator==(const page &other) const {
		return ((flags == other.flags) && (objects == other.objects));
	}
	bool operator!=(const page &other) const {
		return ((flags != other.flags) || (objects != other.objects));
	}

	std::string str() const {
		std::ostringstream ss;
		if (objects.size() > 0) {
			ss << "[" << objects.front().str() <<
				", " << objects.back().str() <<
				", L" << (is_leaf() ? 1 : 0) <<
				", N" << objects.size() <<
				", T" << total_size <<
				")";
		} else {
			ss << "[" << 
				"L" << (is_leaf() ? 1 : 0) <<
				", N" << objects.size() <<
				", T" << total_size <<
				")";
		}
		return ss.str();
	}

	bool is_leaf() const {
		return flags & PAGE_LEAF;
	}

	void load(const void *data, size_t size) {
		objects.clear();
		flags = 0;
		next = eurl();
		total_size = 0;

		msgpack::unpacked result;

		msgpack::unpack(&result, (const char *)data, size);
		msgpack::object obj = result.get();
		obj.convert(this);

		dprintf("page load: %s\n", str().c_str());
	}

	std::string save() const {
		std::stringstream ss;
		msgpack::pack(ss, *this);

		dprintf("page save: %s\n", str().c_str());

		return ss.str();
	}

	// return position of the given key in @objects vector
	int search_leaf(const key &obj) const {
		if (!is_leaf()) {
			return -1;
		}

		auto it = std::lower_bound(objects.begin(), objects.end(), obj);
		if (it == objects.end())
			return -1;

		if (*it != obj)
			return -1;

		return it - objects.begin();
	}

	// returns position of the key in @objects vector
	int search_node(const key &obj) const {
		if (objects.size() == 0)
			return -1;

		if (is_leaf()) {
			return search_leaf(obj);
		}

		if (obj <= objects.front()) {
			return 0;
		}

		auto it = std::lower_bound(objects.begin(), objects.end(), obj);
		if (it == objects.end()) {
			return objects.size() - 1;
		}

		if (*it == obj)
			return it - objects.begin();

		return (it - objects.begin()) - 1;
	}

	// returnes true if modified page is subject to compaction
	bool remove(size_t remove_pos) {
		total_size -= objects[remove_pos].size();
		for (size_t pos = remove_pos + 1, objects_num = objects.size(); pos < objects_num; ++pos) {
			objects[pos - 1] = objects[pos];
		}

		objects.resize(objects.size() - 1);


		return total_size < max_page_size / 3;
	}

	bool insert_and_split(const key &obj, page &other, bool &replaced) {
		std::vector<key> copy;
		bool copied = false;

		replaced = false;

		for (auto it = objects.begin(); it != objects.end(); ++it) {
			if (obj <= *it) {
				copy.push_back(obj);
				total_size += obj.size();
				copied = true;

				if (obj == *it) {
					replaced = true;
					total_size -= it->size();
					++it;
				}

				copy.insert(copy.end(), it, objects.end());
				break;
			}

			copy.push_back(*it);
		}

		if (!copied) {
			copy.push_back(obj);
			total_size += obj.size();
		}

		if (total_size > max_page_size) {
			size_t split_idx = copy.size() / 2;

			other.flags = flags;
			other.objects = std::vector<key>(std::make_move_iterator(copy.begin() + split_idx),
							 std::make_move_iterator(copy.end()));
			other.recalculate_size();

			copy.erase(copy.begin() + split_idx, copy.end());
			objects.swap(copy);
			recalculate_size();

			dprintf("insert/split: %s: split: %s %s\n", obj.str().c_str(), str().c_str(), other.str().c_str());

			return true;
		}

		objects.swap(copy);

		dprintf("insert/split: %s: %s\n", obj.str().c_str(), str().c_str());
		return false;
	}

	void recalculate_size() {
		total_size = 0;
		for_each(objects.begin(), objects.end(), [&] (const key &obj)
				{
					total_size += obj.size();
				});
	}
};

template <typename T>
class page_iterator {
public:
	typedef page_iterator self_type;
	typedef page value_type;
	typedef page& reference;
	typedef page* pointer;
	typedef std::forward_iterator_tag iterator_category;
	typedef std::ptrdiff_t difference_type;

	page_iterator(T &t, const page &p) : m_t(t), m_page(p) {}
	page_iterator(T &t, const eurl &url) : m_t(t), m_url(url) {
		status e = m_t.read(url);
		if (e.error)
			return;

		m_page.load(e.data.data(), e.data.size());
	}
	page_iterator(const page_iterator &i) : m_t(i.m_t) {
		m_page = i.m_page;
		m_page_index = i.m_page_index;
	}

	self_type operator++() {
		try_loading_next_page();

		return *this;
	}

	self_type operator++(int num) {
		try_loading_next_page();

		return *this;
	}

	reference operator*() {
		return m_page;
	}
	pointer operator->() {
		return &m_page;
	}

	bool operator==(const self_type& rhs) {
		return m_page == rhs.m_page;
	}
	bool operator!=(const self_type& rhs) {
		return m_page != rhs.m_page;
	}

	eurl url() const {
		return m_url;
	}

private:
	T &m_t;
	page m_page;
	size_t m_page_index = 0;
	eurl m_url;

	void try_loading_next_page() {
		++m_page_index;

		if (m_page.next.empty()) {
			m_page = page();
			m_url = eurl();
		} else {
			m_url = m_page.next;
			status e = m_t.read(m_url);
			if (e.error) {
				m_page = page();
				return;
			}
			m_page.load(e.data.data(), e.data.size());
		}
	}
};

template <typename T>
class iterator {
public:
	typedef iterator self_type;
	typedef key value_type;
	typedef key& reference;
	typedef key* pointer;
	typedef std::forward_iterator_tag iterator_category;
	typedef std::ptrdiff_t difference_type;

	iterator(T &t, page &p, size_t internal_index) : m_t(t), m_page(p), m_page_internal_index(internal_index) {}
	iterator(const iterator &i) : m_t(i.m_t) {
		m_page = i.m_page;
		m_page_internal_index = i.m_page_internal_index;
		m_page_index = i.m_page_index;
	}

	self_type operator++() {
		++m_page_internal_index;
		try_loading_next_page();

		return *this;
	}

	self_type operator++(int num) {
		m_page_internal_index += num;
		try_loading_next_page();

		return *this;
	}

	reference operator*() {
		return m_page.objects[m_page_internal_index];
	}
	pointer operator->() {
		return &m_page.objects[m_page_internal_index];
	}

	bool operator==(const self_type& rhs) {
		return (m_page == rhs.m_page) && (m_page_internal_index == rhs.m_page_internal_index);
	}
	bool operator!=(const self_type& rhs) {
		return (m_page != rhs.m_page) || (m_page_internal_index != rhs.m_page_internal_index);
	}
private:
	T &m_t;
	page m_page;
	size_t m_page_index = 0;
	size_t m_page_internal_index = 0;

	void try_loading_next_page() {
		if (m_page_internal_index >= m_page.objects.size()) {
			m_page_internal_index = 0;
			++m_page_index;

			if (m_page.next.empty()) {
				m_page = page();
			} else {
				status e = m_t.read(m_page.next);
				if (e.error) {
					m_page = page();
					return;
				}
				m_page.load(e.data.data(), e.data.size());
			}
		}
	}
};

}} // namespace ioremap::greylock

namespace msgpack {
static inline ioremap::greylock::page &operator >>(msgpack::object o, ioremap::greylock::page &page)
{
	if (o.type != msgpack::type::ARRAY) {
		std::ostringstream ss;
		ss << "page unpack: type: " << o.type <<
			", must be: " << msgpack::type::ARRAY <<
			", size: " << o.via.array.size;
		throw std::runtime_error(ss.str());
	}

	object *p = o.via.array.ptr;
	const uint32_t size = o.via.array.size;
	uint16_t version = 0;
	p[0].convert(&version);
	switch (version) {
	case ioremap::greylock::page::serialization_version_raw:
	case ioremap::greylock::page::serialization_version_packed: {
		if (size != 4) {
			std::ostringstream ss;
			ss << "page unpack: array size mismatch: read: " << size << ", must be: 4";
			throw std::runtime_error(ss.str());
		}

		p[1].convert(&page.flags);
		p[2].convert(&page.next);

		switch (version) {
		case ioremap::greylock::page::serialization_version_raw:
			p[3].convert(&page.objects);
			break;
		case ioremap::greylock::page::serialization_version_packed: {
			msgpack::unpacked result;

			//msgpack::unpack(&result, raw.data(), raw.size());
			msgpack::object obj;// = result.get();

			const char *src = p[3].via.raw.ptr;
			size_t src_size = p[3].via.raw.size;

			LZ4F_decompressionContext_t dctx;
			LZ4F_errorCode_t err = LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
			if (LZ4F_isError(err)) {
				std::stringstream ss;
				ss << "page unpack: " << page.str() <<
					": expected compressed page (version: " << version << ")"
					", but failed to create decompression context"
					", error: " << LZ4F_getErrorName(err) <<
					", code: " << err;
				throw std::runtime_error(ss.str());
			}

			size_t src_orig = src_size;

			LZ4F_frameInfo_t fi;
			err = LZ4F_getFrameInfo(dctx, &fi, src, &src_size);
			if (LZ4F_isError(err)) {
				std::stringstream ss;
				ss << "page unpack: " << page.str() <<
					": expected compressed page (version: " << version << ")"
					", but failed to get frame info"
					", error: " << LZ4F_getErrorName(err) <<
					", code: " << err;
				LZ4F_freeDecompressionContext(dctx);
				throw std::runtime_error(ss.str());
			}

			src += src_size;
			src_size = src_orig - src_size;

			size_t dst_size = ioremap::greylock::max_page_size * 10;
			// unknown original size
			if (fi.contentSize != 0)
				dst_size = fi.contentSize;

			std::string dst_string;
			dst_string.resize(dst_size);
			char *dst = const_cast<char *>(dst_string.data());

			while (src_size != 0) {
				size_t dst_orig = dst_size;
				size_t src_orig = src_size;

				// only running decompression once, it should process the whole buffer since
				// compression runs in one go too
				err = LZ4F_decompress(dctx, dst, &dst_size, src, &src_size, NULL);
				if (LZ4F_isError(err)) {
					std::stringstream ss;
					ss << "page unpack: " << page.str() <<
						": expected compressed page (version: " << version << ")"
						", but failed to decompress frame"
						", error: " << LZ4F_getErrorName(err) <<
						", code: " << err;
					LZ4F_freeDecompressionContext(dctx);
					throw std::runtime_error(ss.str());
				}

				dst += dst_size;
				dst_size = dst_orig - dst_size;

				src += src_size;
				src_size = src_orig - src_size;
			}

			dst_string.resize(dst - dst_string.data());

			LZ4F_freeDecompressionContext(dctx);

			msgpack::unpack(&result, dst_string.data(), dst_string.size());
			obj = result.get();

			obj.convert(&page.objects);
			page.recalculate_size();
		}
		}
		break;
	}
	default: {
		std::ostringstream ss;
		ss << "page unpack: version mismatch: read: " << version <<
			", must be: < " << ioremap::greylock::page::serialization_version_max;
		throw std::runtime_error(ss.str());
	}
	}

	return page;
}

template <typename Stream>
inline msgpack::packer<Stream> &operator <<(msgpack::packer<Stream> &o, const ioremap::greylock::page &p)
{
	o.pack_array(4);
	o.pack((int)ioremap::greylock::page::serialization_version_packed);
	o.pack(p.flags);
	o.pack(p.next);

	std::stringstream ss;
	msgpack::pack(ss, p.objects);

	const std::string &s = ss.str();

	size_t max_size = LZ4F_compressFrameBound(s.size(), NULL);
	std::string buf;
	buf.resize(max_size);
	size_t real_size = LZ4F_compressFrame((char *)buf.data(), max_size, (char *)s.data(), s.size(), NULL);
	if (LZ4F_isError(real_size)) {
		std::stringstream ss;
		ss << "page pack: " << p.str() <<
			": failed to compress frame:" <<
			" max frame bound: " << max_size <<
			", packed raw size: " << s.size() <<
			", error: " << LZ4F_getErrorName(real_size) <<
			", code: " << real_size;
		throw std::runtime_error(ss.str());
	}

	buf.resize(real_size);

	o.pack_raw(buf.size());
	o.pack_raw_body(buf.data(), buf.size());

	dprintf("pack: objects: %zd, total_size: %zd, data size: %zd -> %zd\n", p.objects.size(), p.total_size, s.size(), buf.size());

	return o;
}

} // namespace msgpack

#endif // __INDEXES_PAGE_HPP
