#pragma once

#include <libclient/net_cl_exports.h>
#include <libdariadb/meas.h>
#include <libdariadb/query.h>
#include <libdariadb/stat.h>
#include <libdariadb/timeutil.h>
#include <libdariadb/utils/async/locker.h>
#include <common/net_common.h>
#include <functional>
#include <memory>
#include <string>

namespace dariadb {
namespace net {
namespace client {

struct ReadResult {
  using callback =
      std::function<void(const ReadResult *parent, const Meas &m, const Statistic &st)>;
  QueryNumber id;
  DATA_KINDS kind;
  utils::async::Locker locker;
  callback clbk;
  bool is_ok;     // true - if server send OK to this query.
  bool is_closed; // true - if all data received.
  bool is_error;  // true - if error. 'errc' contain error type.
  ERRORS errc;
  ReadResult() {
    is_error = false;
    is_ok = false;
    id = std::numeric_limits<QueryNumber>::max();
    is_closed = false;
  }
  void wait() { locker.lock(); }
};
using ReadResult_ptr = std::shared_ptr<ReadResult>;

class Client {
public:
  struct Param {
    std::string host;
    unsigned short port;
    unsigned short http_port;
    Param(const std::string &_host, unsigned short _port, unsigned short _http_port) {
      host = _host;
      port = _port;
      http_port = _http_port;
    }
  };
  CL_EXPORT Client(const Param &p);
  CL_EXPORT ~Client();

  CL_EXPORT void connect();
  CL_EXPORT void disconnect();

  CL_EXPORT CLIENT_STATE state() const;
  CL_EXPORT size_t pings_answers() const;

  /// connection id on server
  CL_EXPORT int id() const;

  CL_EXPORT void append(const MeasArray &ma);
  CL_EXPORT MeasArray readInterval(const QueryInterval &qi);
  CL_EXPORT ReadResult_ptr readInterval(const QueryInterval &qi,
                                        ReadResult::callback &clbk);

  CL_EXPORT Id2Meas readTimePoint(const QueryTimePoint &qi);
  CL_EXPORT ReadResult_ptr readTimePoint(const QueryTimePoint &qi,
                                         ReadResult::callback &clbk);

  CL_EXPORT ReadResult_ptr currentValue(const IdArray &ids, const Flag &flag,
                                        ReadResult::callback &clbk);
  CL_EXPORT Id2Meas currentValue(const IdArray &ids, const Flag &flag);

  CL_EXPORT ReadResult_ptr subscribe(const IdArray &ids, const Flag &flag,
                                     ReadResult::callback &clbk);

  CL_EXPORT std::map<std::string, dariadb::Id> loadScheme();
  CL_EXPORT bool addToScheme(const std::string &value);

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};

typedef std::shared_ptr<dariadb::net::client::Client> Client_Ptr;
}
}
}
