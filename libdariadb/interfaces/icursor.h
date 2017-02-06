#pragma once

#include <libdariadb/interfaces/icallbacks.h>
#include <libdariadb/meas.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/storage/query_param.h>
#include <libdariadb/utils/utils.h>
#include <memory>
#include <unordered_map>

namespace dariadb {
class ICursor;
using Cursor_Ptr = std::shared_ptr<ICursor>;

class ICursor : public utils::NonCopy {
public:
  virtual Meas readNext() = 0;
  virtual Meas top() = 0; /// null if isEnd==true
  virtual bool is_end() const = 0;
  virtual ~ICursor() {}

  virtual Time minTime() = 0;
  virtual Time maxTime() = 0;

  EXPORT virtual void apply(storage::IReadCallback *clbk);
  EXPORT virtual void apply(storage::IReadCallback *clbk,
                            const storage::QueryInterval &q);
};

using Id2Cursor = std::unordered_map<Id, Cursor_Ptr>;
using CursorsList = std::list<dariadb::Cursor_Ptr>;
using Id2CursorsList = std::unordered_map<Id, CursorsList>;
}
