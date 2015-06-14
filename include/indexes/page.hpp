#ifndef __INDEXES_PAGE_HPP
#define __INDEXES_PAGE_HPP

#include "indexes/error.hpp"
#include "indexes/key.hpp"

#include <iterator>
#include <vector>

namespace ioremap { namespace indexes {

#define PAGE_LEAF		(1<<0)

struct page {
	uint32_t flags = 0;
	std::vector<indexes::key> objects;
	size_t total_size = 0;
	eurl next;

	MSGPACK_DEFINE(flags, objects, total_size, next);

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

	bool insert_and_split(const key &obj, page &other) {
		std::vector<key> copy;
		bool copied = false;

		for (auto it = objects.begin(); it != objects.end(); ++it) {
			if (obj <= *it) {
				copy.push_back(obj);
				total_size += obj.size();
				copied = true;

				if (obj == *it) {
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


}} // namespace ioremap::indexes

#endif // __INDEXES_PAGE_HPP
