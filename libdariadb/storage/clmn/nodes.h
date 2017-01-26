#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/bloom_filter.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/utils/utils.h>
#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>

namespace dariadb {
namespace storage {
namespace clmn {
//"dariadb."
const uint64_t MAGIC_NUMBER = 0x2E62646169726164;

typedef uint32_t generation_t;
typedef uint32_t node_id_t;
typedef uint32_t node_size_t;
typedef uint64_t node_ptr;
typedef uint8_t node_kind;

const node_ptr NODE_PTR_NULL = std::numeric_limits<node_ptr>::max(); // for null pointers
const node_kind NODE_KIND_FOOTER = 1;
const node_kind NODE_KIND_ROOT = 1 << 1;
const node_kind NODE_KIND_NODE = 1 << 2;
const node_kind NODE_KIND_LEAF = 1 << 3;

#pragma pack(push, 1)

struct NodeHeader {
  node_kind kind;
  generation_t gen; // generation of storage. increment when writing to disk. need for
                    // version control.
  node_id_t id;     // node id
  node_size_t size; // size of key array

  NodeHeader(generation_t g, node_id_t _id, node_size_t sz, node_kind k) {
    gen = g;
    id = _id;
    size = sz;
    kind = k;
  }
};

struct Footer {
  typedef std::shared_ptr<Footer> Footer_Ptr;

  uint64_t magic_num; // for detect foother, when something going wrong
  NodeHeader hdr;
  uint32_t crc;
  node_ptr *roots;
  Id *ids;

  Footer(node_size_t sz) : hdr(0, 0, sz, NODE_KIND_FOOTER) {
    magic_num = MAGIC_NUMBER;
    crc = 0;
    roots = new node_ptr[hdr.size];
    std::fill_n(roots, hdr.size, NODE_PTR_NULL);

    ids = new Id[hdr.size];
    std::fill_n(ids, hdr.size, MAX_ID);
  }

  ~Footer() {
    delete[] roots;
    delete[] ids;
  }

  static Footer_Ptr make_footer(node_size_t sz) { return std::make_shared<Footer>(sz); }

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
  typedef std::shared_ptr<Node> Node_Ptr;
  NodeHeader hdr;
  Statistic stat;
  node_ptr neighbor; // ptr to neighbor;
  Time *keys;
  bool children_is_leaf;
  node_ptr *children;

  Node(generation_t g, node_id_t _id, node_size_t sz, node_kind _k)
      : hdr(g, _id, sz, _k), stat() {
    keys = new Time[sz];
    std::fill_n(keys, hdr.size, Time(0));

    children = new node_ptr[sz];
    std::fill_n(children, hdr.size, NODE_PTR_NULL);

    neighbor = NODE_PTR_NULL;

    children_is_leaf = false;
  }

  ~Node() {
    delete[] keys;
    delete[] children;
  }

  static Node_Ptr make_node(generation_t g, node_id_t _id, node_size_t sz) {
    return std::make_shared<Node>(g, _id, sz, NODE_KIND_NODE);
  }

  static Node_Ptr make_root(generation_t g, node_id_t _id, node_size_t sz) {
    return std::make_shared<Node>(g, _id, sz, NODE_KIND_ROOT);
  }
};

struct Leaf {
  typedef std::shared_ptr<Leaf> Leaf_Ptr;
  NodeHeader hdr;
  node_ptr neighbor; // ptr to neighbor;
  ChunkHeader chunk_hdr;
  uint8_t *chunk_buffer;

  Leaf(generation_t g, node_id_t _id, node_size_t sz) : hdr(g, _id, sz, NODE_KIND_LEAF) {
    chunk_buffer = new uint8_t[hdr.size];
    std::fill_n(chunk_buffer, hdr.size, uint8_t(0));

    neighbor = NODE_PTR_NULL;
  }

  ~Leaf() { delete[] chunk_buffer; }

  static Leaf_Ptr make_leaf(generation_t g, node_id_t _id, node_size_t sz) {
    return std::make_shared<Leaf>(g, _id, sz);
  }
};

#pragma pack(pop)

class NodeStorage {
public:
  virtual Footer::Footer_Ptr getFooter() = 0;
  virtual Node::Node_Ptr readNode(const node_ptr ptr) = 0;
  virtual Leaf::Leaf_Ptr readLeaf(const node_ptr ptr) = 0;
};
typedef std::shared_ptr<NodeStorage> NodeStorage_Ptr;

class MemoryNodeStorage : public NodeStorage {
public:
  EXPORT static NodeStorage_Ptr create();
  EXPORT ~MemoryNodeStorage();

  EXPORT virtual Footer::Footer_Ptr getFooter() override;
  EXPORT virtual Node::Node_Ptr readNode(const node_ptr ptr) override;
  EXPORT virtual Leaf::Leaf_Ptr readLeaf(const node_ptr ptr) override;

protected:
  EXPORT MemoryNodeStorage();

private:
  struct Private;
  std::unique_ptr<Private> _impl;
};
}
}
}