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

struct ReadResult {
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

class Client {
public:
  struct Param {
    std::string host;
    unsigned short port;
    Param(const std::string &_host, unsigned short _port) {
      host = _host;
      port = _port;
    }
  };
  DARIADBNET_CL_EXPORTS Client(const Param &p);
  DARIADBNET_CL_EXPORTS ~Client();

  DARIADBNET_CL_EXPORTS void connect();
  DARIADBNET_CL_EXPORTS void disconnect();

  DARIADBNET_CL_EXPORTS CLIENT_STATE state() const;
  DARIADBNET_CL_EXPORTS size_t pings_answers() const;

  /// connection id on server
  DARIADBNET_CL_EXPORTS int id() const;

  DARIADBNET_CL_EXPORTS void append(const MeasArray &ma);
  DARIADBNET_CL_EXPORTS MeasList readInterval(const storage::QueryInterval &qi);
  DARIADBNET_CL_EXPORTS ReadResult_ptr readInterval(const storage::QueryInterval &qi, ReadResult::callback &clbk);

  DARIADBNET_CL_EXPORTS Id2Meas readTimePoint(const storage::QueryTimePoint &qi);
  DARIADBNET_CL_EXPORTS ReadResult_ptr readTimePoint(const storage::QueryTimePoint &qi, ReadResult::callback &clbk);

  DARIADBNET_CL_EXPORTS ReadResult_ptr currentValue(const IdArray &ids, const Flag &flag, ReadResult::callback &clbk);
  DARIADBNET_CL_EXPORTS Id2Meas currentValue(const IdArray &ids, const Flag &flag);

  DARIADBNET_CL_EXPORTS ReadResult_ptr subscribe(const IdArray &ids, const Flag &flag, ReadResult::callback &clbk);
protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};

typedef std::shared_ptr<dariadb::net::client::Client> Client_Ptr;

}
}
}
