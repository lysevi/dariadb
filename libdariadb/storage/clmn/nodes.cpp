#include <libdariadb/storage/clmn/nodes.h>
#include <libdariadb/utils/crc.h>
#include <libdariadb/utils/utils.h>
#include <unordered_map>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::storage::clmn;

void Footer::update_crc() {}

struct MemoryNodeStorage::Private : public NodeStorage {
  Private() : _ptr() {}

  Footer::Footer_Ptr getFooter() override { return _last_footer; }

  Node::Ptr readNode(const node_addr_t ptr) override {
    auto fres = _nodes.find(ptr);
    ENSURE(fres != _nodes.end());
    return fres->second;
  }

  Leaf::Ptr readLeaf(const node_addr_t ptr) override {
    auto fres = _leafs.find(ptr);
    ENSURE(fres != _leafs.end());
    return fres->second;
  }

  addr_vector write(const node_vector &nodes) override {
    ENSURE(nodes.size() != 0);

    addr_vector result;
    result.resize(nodes.size());

    for (size_t i = 0; i < nodes.size(); ++i) {
      if (nodes[i]->hdr.kind == NODE_KIND_ROOT) {
        auto p = ++_ptr;
        _roots[p] = nodes[i];
        update_footer(nodes[i], p);
        result[i] = p;
      } else {
        _nodes[++_ptr] = nodes[i];
        result[i] = _ptr;
      }
    }
    return result;
  }

  addr_vector write(const leaf_vector &leafs) override {
    ENSURE(leafs.size() != 0);

    addr_vector result;
    result.resize(leafs.size());
    std::fill(result.begin(), result.end(), NODE_PTR_NULL);

    for (size_t i = 0; i < leafs.size(); ++i) {
      result[i] = write_leaf(leafs[i]);
    }
    return result;
  }

  node_addr_t write_leaf(const Leaf::Ptr &leaf) override {
	  _leafs[++_ptr] = leaf;
	  return _ptr;
  }

  void update_footer(const Node::Ptr &r, const node_addr_t &ptr) {
    ENSURE(r->hdr.kind == NODE_KIND_ROOT);

    if (_last_footer == nullptr) {
      _last_footer = Footer::make_footer(r->hdr.meas_id, ptr);
    } else {
      _last_footer = Footer::copy_on_write(_last_footer, r->hdr.meas_id, ptr);
    }

    _footers[++_ptr] = _last_footer;
  }

  Footer::Footer_Ptr _last_footer;
  std::unordered_map<node_addr_t, Footer::Footer_Ptr> _footers;
  std::unordered_map<node_addr_t, Node::Ptr> _nodes;
  std::unordered_map<node_addr_t, Leaf::Ptr> _leafs;
  std::unordered_map<node_addr_t, Node::Ptr> _roots;
  node_addr_t _ptr;
};

NodeStorage_Ptr MemoryNodeStorage::create() {
  return NodeStorage_Ptr{new MemoryNodeStorage};
}

MemoryNodeStorage::MemoryNodeStorage() : _impl{new Private()} {}

MemoryNodeStorage::~MemoryNodeStorage() { _impl = nullptr; }

Footer::Footer_Ptr MemoryNodeStorage::getFooter() { return _impl->getFooter(); }

Node::Ptr MemoryNodeStorage::readNode(const node_addr_t ptr) {
  return _impl->readNode(ptr);
}

Leaf::Ptr MemoryNodeStorage::readLeaf(const node_addr_t ptr) {
  return _impl->readLeaf(ptr);
}

addr_vector MemoryNodeStorage::write(const node_vector &nodes) {
  return _impl->write(nodes);
}

node_addr_t MemoryNodeStorage::write_leaf(const Leaf::Ptr &leaf) {
	return _impl->write_leaf(leaf);
}

addr_vector MemoryNodeStorage::write(const leaf_vector &leafs) {
  return _impl->write(leafs);
}