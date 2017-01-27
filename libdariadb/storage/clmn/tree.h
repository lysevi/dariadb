#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/clmn/nodes.h>

namespace dariadb {
namespace storage {
namespace clmn {
class Tree {
public:
  struct Params {
    node_sz_t measurementsInLeaf;
    node_sz_t nodeChildren;
    Params() { measurementsInLeaf = nodeChildren = 0; }
  };
  typedef std::shared_ptr<Tree> Ptr;

  EXPORT Ptr static create(const NodeStorage_Ptr &nstore, const Params &params);
  EXPORT ~Tree();
  EXPORT void append(const Meas &m);

protected:
  Tree(const NodeStorage_Ptr &nstore, const Params &params);
  struct Private;
  std::unique_ptr<Private> _impl;
};
}
}
}