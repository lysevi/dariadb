#pragma once

#include <algorithm>
#include <cstdint>
#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/utils/utils.h>
#include <limits>
#include <memory>

namespace dariadb {
namespace storage {
namespace clmn {
//"dariadb."
const uint64_t MAGIC_NUMBER = 0x2E62646169726164;

typedef uint32_t gnrt_t; // generation
typedef uint8_t node_kind_t;
typedef uint32_t node_id_t;
typedef uint32_t node_sz_t;
/// in tree level - just a node id.
/// in storage level - physical address
typedef node_id_t node_addr_t;


const node_addr_t NODE_ID_ZERO =std::numeric_limits<node_id_t>::max(); // for null pointers
const node_addr_t NODE_PTR_NULL =
    std::numeric_limits<node_addr_t>::max(); // for null pointers
const node_kind_t NODE_KIND_FOOTER = 1;
const node_kind_t NODE_KIND_ROOT = 1 << 1;
const node_kind_t NODE_KIND_NODE = 1 << 2;
const node_kind_t NODE_KIND_LEAF = 1 << 3;

#pragma pack(push, 1)

struct NodeHeader {
  node_kind_t kind;

  // generation of storage.
  // increment when writing to disk. need for
  // version control.
  gnrt_t gen;
  // node id
  node_id_t id;
  // size of key array
  node_sz_t size;

  Id meas_id;
  NodeHeader(gnrt_t g, node_id_t _id, node_sz_t sz, node_kind_t k, Id m_id) {
    meas_id = Id(0);
    gen = g;
    id = _id;
    size = sz;
    kind = k;
	meas_id = m_id;
  }
};

struct Footer {
  typedef std::shared_ptr<Footer> Footer_Ptr;

  uint64_t magic_num; // for detect foother, when something going wrong
  NodeHeader hdr;
  uint32_t crc;
  node_addr_t *roots;
  Id *ids;

  Footer(Id m_id, node_addr_t p) : hdr(0, 0, node_sz_t(1), NODE_KIND_FOOTER, m_id) {
    magic_num = MAGIC_NUMBER;
    crc = 0;
    roots = new node_addr_t[hdr.size];
    ids = new Id[hdr.size];

	ids[0] = m_id;
	roots[0] = p;

	update_crc();
  }

  Footer(const Footer &other, Id m_id, node_addr_t p) : hdr(other.hdr) {
    magic_num = other.magic_num;
    crc = 0;
	bool exists_in_origin = false;
	for (node_sz_t i = 0; i < hdr.size; ++i) {
		if (other.ids[i] == m_id || other.roots[i] == NODE_PTR_NULL) {
			exists_in_origin = true;
		}
	}
	
	if (!exists_in_origin) {
		hdr.size++;
	}

    roots = new node_addr_t[hdr.size];
    ids = new Id[hdr.size];

    for (node_sz_t i = 0; i < other.hdr.size; ++i) {
      ids[i] = other.ids[i];
      roots[i] = other.roots[i];
    }

	for (node_sz_t i = 0; i < hdr.size; ++i) {
		if (ids[i] == m_id || roots[i] == NODE_PTR_NULL) {
			ids[i] = m_id;
			roots[i] = p;
			break;
		}
	}

    ++hdr.gen;

	update_crc();
  }

  ~Footer() {
    delete[] roots;
    delete[] ids;
  }

  static Footer_Ptr make_footer(Id m_id, node_addr_t p) {
    return std::make_shared<Footer>(m_id,p);
  }

  static Footer_Ptr copy_on_write(const Footer_Ptr f, Id m_id, node_addr_t p) {
    return std::make_shared<Footer>(*f, m_id, p);
  }

  // maximal id of child nodes.
  node_id_t max_node_id() const { return hdr.id; }

  EXPORT void update_crc();
};

struct Statistic {
  Time min_time;
  Time max_time;

  uint32_t count;

  uint64_t flg_bloom;

  Value min_value;
  Value max_value;

  Value sum;

  Statistic() {
    flg_bloom = bloom_empty<Flag>();
    count = uint32_t(0);
    min_time = MAX_TIME;
    max_time = MIN_TIME;

    min_value = MAX_VALUE;
    max_value = MIN_VALUE;

    sum = MIN_VALUE;
  }

  void update(const Meas &m) {
    count++;

    min_time = std::min(m.time, min_time);
    max_time = std::max(m.time, max_time);

    flg_bloom = bloom_add<Flag>(flg_bloom, m.flag);

    min_value = std::min(m.value, min_value);
    max_value = std::max(m.value, max_value);

    sum += m.value;
  }

  void update(const Statistic &st) {
    count += st.count;

    min_time = std::min(st.min_time, min_time);
    max_time = std::max(st.max_time, max_time);

    flg_bloom = flg_bloom | st.flg_bloom;

    min_value = std::min(st.min_value, min_value);
    max_value = std::max(st.max_value, max_value);

    sum += st.sum;
  }
};

struct Node {
  typedef std::shared_ptr<Node> Ptr;
  NodeHeader hdr;
  Statistic stat;
  node_addr_t neighbor; // ptr to neighbor;
  Time *keys;
  bool children_is_leaf;
  node_addr_t *children;

  Node(gnrt_t g, node_id_t _id, node_sz_t sz, node_kind_t _k)
      : hdr(g, _id, sz, _k, MAX_ID), stat() {
    keys = new Time[hdr.size];
    std::fill_n(keys, hdr.size, Time(0));

    children = new node_addr_t[sz];
    std::fill_n(children, hdr.size, NODE_PTR_NULL);

    neighbor = NODE_PTR_NULL;

    children_is_leaf = false;
  }

  Node(const Ptr&other)
	  : hdr(other->hdr), stat(other->stat) {
	  keys = new Time[hdr.size];
	  std::fill_n(keys, hdr.size, Time(0));

	  children = new node_addr_t[hdr.size];
	  for (node_sz_t i = 0; i < hdr.size;++i) {
		  children[i] = other->children[i];
	  }

	  neighbor = other->neighbor;

	  children_is_leaf = other->children_is_leaf;

	  ++hdr.gen;
  }

  ~Node() {
    delete[] keys;
    delete[] children;
  }

  static Ptr make_node(gnrt_t g, node_id_t _id, node_sz_t sz) {
    return std::make_shared<Node>(g, _id, sz, NODE_KIND_NODE);
  }

  static Ptr make_root(gnrt_t g, node_id_t _id, node_sz_t sz) {
    return std::make_shared<Node>(g, _id, sz, NODE_KIND_ROOT);
  }

  static Ptr copy_on_write(const Ptr&other) {
	  return std::make_shared<Node>(other);
  }

  // return true if succes.
  bool add_child(const Time key, const node_addr_t c) {
    for (node_sz_t i = 0; i < hdr.size; ++i) {
      if (keys[i] == NODE_PTR_NULL) {
        keys[i] = key;
        children[i] = c;
        return true;
      }
    }
    return false;
  }
};

struct Leaf {
  typedef std::shared_ptr<Leaf> Ptr;
  NodeHeader hdr;
  node_addr_t neighbor; // ptr to neighbor;
  ChunkHeader chunk_hdr;
  uint8_t *chunk_buffer;

  Leaf(gnrt_t g, node_id_t _id, node_sz_t sz)
      : hdr(g, _id, sz, NODE_KIND_LEAF, MAX_ID) {
    chunk_buffer = new uint8_t[hdr.size];
    std::fill_n(chunk_buffer, hdr.size, uint8_t(0));

    neighbor = NODE_PTR_NULL;
  }

  ~Leaf() { delete[] chunk_buffer; }

  static Ptr make_leaf(gnrt_t g, node_id_t _id, node_sz_t sz) {
    return std::make_shared<Leaf>(g, _id, sz);
  }
};

#pragma pack(pop)

typedef std::vector<Node::Ptr> node_vector;
typedef std::vector<Leaf::Ptr> leaf_vector;
typedef std::vector<node_addr_t> addr_vector;

/// must fill Footer
class NodeStorage {
public:
  virtual Footer::Footer_Ptr getFooter() = 0;
  virtual Node::Ptr readNode(const node_addr_t ptr) = 0;
  virtual Leaf::Ptr readLeaf(const node_addr_t ptr) = 0;

  virtual void write(const node_vector &nodes) = 0;
  virtual addr_vector write(const leaf_vector &leafs) = 0;
};

typedef std::shared_ptr<NodeStorage> NodeStorage_Ptr;

class MemoryNodeStorage : public NodeStorage {
public:
  EXPORT static NodeStorage_Ptr create();
  EXPORT ~MemoryNodeStorage();

  EXPORT virtual Footer::Footer_Ptr getFooter() override;
  EXPORT virtual Node::Ptr readNode(const node_addr_t ptr) override;
  EXPORT virtual Leaf::Ptr readLeaf(const node_addr_t ptr) override;

  EXPORT virtual void write(const node_vector &nodes) override;
  EXPORT virtual addr_vector write(const leaf_vector &leafs) override;
protected:
  EXPORT MemoryNodeStorage();

private:
  struct Private;
  std::unique_ptr<Private> _impl;
};
}
}
}