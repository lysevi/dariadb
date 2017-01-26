#include <libdariadb/storage/clmn/tree.h>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::storage::clmn;

struct Tree::Private {
  Private(const NodeStorage_Ptr &nstore) {}

  NodeStorage_Ptr _nstore;
};

Tree::Tree(const NodeStorage_Ptr &nstore) : _impl{new Tree::Private(nstore)} {}

Tree::~Tree() {
  _impl = nullptr;
}