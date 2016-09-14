#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/storage/query_param.h>
#include <libdariadb/utils/locker.h>
#include <common/net_common.h>
#include <functional>
#include <memory>
#include <string>
#include "dariadb_cl_exports.h"
namespace dariadb {
namespace net {
namespace client {

struct DARIADBCL_EXPORTS ReadResult {
  using callback = std::function<void(const ReadResult *parent, const Meas &m)>;
  QueryNumber id;
  DataKinds kind;
  utils::Locker locker;
  callback clbk;
  bool is_ok;     //true - if server send OK to this query.
  bool is_closed; //true - if all data sended.
  bool is_error;  //true - if error. errc contain error type.
  ERRORS errc;
  ReadResult() { is_error = false; is_ok = false; }
  void wait() { locker.lock(); }
};
using ReadResult_ptr = std::shared_ptr<ReadResult>;

class DARIADBCL_EXPORTS Client {
public:
  struct Param {
    std::string host;
    unsigned short port;
    Param(const std::string &_host, unsigned short _port) {
      host = _host;
      port = _port;
    }
  };
  Client(const Param &p);
  ~Client();

  void connect();
  void disconnect();

  ClientState state() const;
  size_t pings_answers() const;

  /// connection id on server
  int id() const;

  void append(const Meas::MeasArray &ma);
  Meas::MeasList readInterval(const storage::QueryInterval &qi);
  ReadResult_ptr readInterval(const storage::QueryInterval &qi, ReadResult::callback &clbk);

  Meas::Id2Meas readInTimePoint(const storage::QueryTimePoint &qi);
  ReadResult_ptr readInTimePoint(const storage::QueryTimePoint &qi, ReadResult::callback &clbk);

  ReadResult_ptr currentValue(const IdArray &ids, const Flag &flag, ReadResult::callback &clbk);
  Meas::Id2Meas currentValue(const IdArray &ids, const Flag &flag);

  ReadResult_ptr subscribe(const IdArray &ids, const Flag &flag, ReadResult::callback &clbk);
protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};
}
}
}
