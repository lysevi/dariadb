#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/utils/utils.h>
#include <cstdint>

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
  uint64_t magic_num; // for detect foother, when something going wrong
  NodeHeader hdr;
  uint32_t crc;

  Footer() : hdr(0, 0, 0, NODE_KIND_FOOTER) { magic_num = MAGIC_NUMBER; }

  // maximal id of child nodes.
  node_id_t max_node_id() const { return hdr.id; }
  EXPORT void update_crc();
};

struct Node {
  NodeHeader hdr;
  Time *keys;

  ~Node() {}

  static Node make_node(generation_t g, node_id_t _id, node_size_t sz) {
    Node result{g, _id, sz, NODE_KIND_NODE};
    return result;
  }

  static Node make_root(generation_t g, node_id_t _id, node_size_t sz) {
    Node result{g, _id, sz, NODE_KIND_ROOT};
    return result;
  }

protected:
  Node(generation_t g, node_id_t _id, node_size_t sz, node_kind _k)
      : hdr(g, _id, sz, _k) {}
};

#pragma pack(pop)
}
}
}