#include <libdariadb/storage/clmn/nodes.h>
#include <libdariadb/utils/crc.h>
#include <libdariadb/utils/utils.h>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::storage::clmn;

void Footer::update_crc() {
  NOT_IMPLEMENTED;
}

struct MemoryNodeStorage::Private : public NodeStorage {
  Private() {}

  virtual Footer::Footer_Ptr getFooter() override { return Footer::Footer_Ptr(); }
  virtual Node::Node_Ptr readNode(const node_ptr ptr) override {
    return Node::Node_Ptr();
  }
  virtual Leaf::Leaf_Ptr readLeaf(const node_ptr ptr) override {
    return Leaf::Leaf_Ptr();
  }
};

NodeStorage_Ptr MemoryNodeStorage::create() {
  return NodeStorage_Ptr{new MemoryNodeStorage};
}

MemoryNodeStorage::MemoryNodeStorage() : _impl{new Private()} {}

MemoryNodeStorage::~MemoryNodeStorage() {
  _impl = nullptr;
}

Footer::Footer_Ptr MemoryNodeStorage::getFooter() {
  return _impl->getFooter();
}

Node::Node_Ptr MemoryNodeStorage::readNode(const node_ptr ptr) {
  return _impl->readNode(ptr);
}

Leaf::Leaf_Ptr MemoryNodeStorage::readLeaf(const node_ptr ptr) {
  return _impl->readLeaf(ptr);
}
