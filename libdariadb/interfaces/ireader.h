#pragma once

#include <libdariadb/interfaces/icallbacks.h>
#include <libdariadb/meas.h>
#include <libdariadb/utils/utils.h>
#include <libdariadb/storage/query_param.h>
#include <libdariadb/st_exports.h>
#include <memory>
#include <unordered_map>

namespace dariadb {
class IReader;
using Reader_Ptr = std::shared_ptr<IReader>;

class IReader:public utils::NonCopy {
public:
  virtual Meas readNext() = 0;
  virtual Meas top() = 0; /// null if isEnd==true
  virtual bool is_end() const = 0;
  virtual ~IReader() {}
  
  virtual Time minTime() = 0;
  virtual Time maxTime() = 0;

  EXPORT virtual void apply(storage::IReaderClb *clbk);
  EXPORT virtual void apply(storage::IReaderClb *clbk, const storage::QueryInterval&q);
};

using Id2Reader = std::unordered_map<Id, Reader_Ptr>;
using Id2ReadersList = std::unordered_map<Id, std::list<Reader_Ptr>>;
}