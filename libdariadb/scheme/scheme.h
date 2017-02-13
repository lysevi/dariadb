#pragma once

#include <libdariadb/st_exports.h>
#include <libdariadb/storage/manifest.h>
#include <memory>

namespace dariadb {
namespace scheme {
class Scheme {
public:
	EXPORT Scheme(const storage::Manifest_ptr m);
protected:
  class Private;
  std::unique_ptr<Private> _impl;
};
}
}