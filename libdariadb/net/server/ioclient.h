#pragma once

#include <atomic>
#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
#include <memory>

#include "../interfaces/iclientmanager.h"
#include "../../interfaces/imeasstorage.h"
#include "../../meas.h"
#include "../net_common.h"
#include "../common.h"
#include "../async_connection.h"

namespace dariadb {
namespace net {
struct ClientIO:public AsyncConnection {
  int id;
  socket_ptr sock;
  std::string host;

  ClientState state;
  IClientManager *srv;
  storage::IMeasStorage*storage;
  std::atomic_int pings_missed;

  ClientIO(int _id, socket_ptr&_sock, IClientManager *_srv, storage::IMeasStorage*_storage);
  ~ClientIO();
  void disconnect();
  void ping();
  
  void onDataRecv(const NetData_ptr&d) override;
  void onNetworkError(const boost::system::error_code&err)override;
 /* void readNextQuery();
  void readHello();
  void onHello(const boost::system::error_code &err, size_t read_bytes);
  void onReadQuery(const boost::system::error_code &err, size_t read_bytes);
  void onPingSended(const boost::system::error_code &err, size_t read_bytes);
  void onOkSended(const boost::system::error_code &err, size_t read_bytes);
  void onDisconnectSended(const boost::system::error_code &err,
                          size_t read_bytes);
  void onRecvValues(size_t values_count,const boost::system::error_code &err, size_t read_bytes);
  void onReadIntervalAnswerSended(const boost::system::error_code &err, size_t read_bytes);*/
};

typedef std::shared_ptr<ClientIO> ClientIO_ptr;
}
}
