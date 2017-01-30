#pragma once

#include <libdariadb/interfaces/icallbacks.h>
#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <memory>
#include <unordered_map>

namespace dariadb {
class IReader;
using Reader_Ptr = std::shared_ptr<IReader>;

class IReader {
public:
  virtual Meas readNext() = 0;
  virtual Meas top() = 0; /// null if isEnd==true
  virtual bool is_end() const = 0;
  virtual ~IReader() {}

  EXPORT virtual void apply(storage::IReaderClb *clbk);
};

using Id2Reader = std::unordered_map<Id, Reader_Ptr>;
using Id2ReadersList = std::unordered_map<Id, std::list<Reader_Ptr>>;
}