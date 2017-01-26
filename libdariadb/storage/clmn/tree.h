#pragma once

#include <libdariadb/st_exports.h>
#include <libdariadb/storage/clmn/nodes.h>

namespace dariadb {
namespace storage {
namespace clmn {
class Tree {
public:
  EXPORT Tree(const NodeStorage_Ptr &nstore);
  EXPORT ~Tree();

protected:
  struct Private;
  std::unique_ptr<Private> _impl;
};
}
}
}