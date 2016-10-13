#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/storage/query_param.h>
#include <libdariadb/utils/locker.h>
#include <common/net_common.h>
#include <functional>
#include <memory>
#include <string>
#include <libclient/dariadb_cl_exports.h>

namespace dariadb {
namespace net {
namespace client {

struct DARIADBCL_EXPORTS ReadResult {
  using callback = std::function<void(const ReadResult *parent, const Meas &m)>;
  QueryNumber id;
  DATA_KINDS kind;
  utils::Locker locker;
  callback clbk;
  bool is_ok;     //true - if server send OK to this query.
  bool is_closed; //true - if all data received.
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

  CLIENT_STATE state() const;
  size_t pings_answers() const;

  /// connection id on server
  int id() const;

  void append(const MeasArray &ma);
  MeasList readInterval(const storage::QueryInterval &qi);
  ReadResult_ptr readInterval(const storage::QueryInterval &qi, ReadResult::callback &clbk);

  Id2Meas readTimePoint(const storage::QueryTimePoint &qi);
  ReadResult_ptr readTimePoint(const storage::QueryTimePoint &qi, ReadResult::callback &clbk);

  ReadResult_ptr currentValue(const IdArray &ids, const Flag &flag, ReadResult::callback &clbk);
  Id2Meas currentValue(const IdArray &ids, const Flag &flag);

  ReadResult_ptr subscribe(const IdArray &ids, const Flag &flag, ReadResult::callback &clbk);
protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};

typedef std::shared_ptr<dariadb::net::client::Client> Client_Ptr;

}
}
}
