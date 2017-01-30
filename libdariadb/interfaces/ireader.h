#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <memory>

namespace dariadb {
class IReader;
using Reader_Ptr = std::shared_ptr<IReader>;

class IReader {
public:
  virtual Meas readNext() = 0;
  virtual Meas top() = 0; /// null if isEnd==true
  virtual bool is_end() const = 0;
  virtual ~IReader() {}
};
}