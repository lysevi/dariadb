#pragma once

#include <atomic>
#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
#include <memory>

#include "../interfaces/iclientmanager.h"
#include "../net_common.h"
#include "common.h"

namespace dariadb {
namespace net {
struct ClientIO {
  int id;
  socket_ptr sock;
  boost::asio::streambuf buff;
  std::string host;

  ClientState state;
  IClientManager *srv;

  std::atomic_int pings_missed;

  ClientIO(int _id, socket_ptr _sock, IClientManager *_srv);
  void disconnect();
  void ping();

  void readNext();

  void readHello();
  void onHello(const boost::system::error_code &err, size_t read_bytes);
  void onRead(const boost::system::error_code &err, size_t read_bytes);
  void onPingSended(const boost::system::error_code &err, size_t read_bytes);
  void onOkSended(const boost::system::error_code &err, size_t read_bytes);
  void onDisconnectSended(const boost::system::error_code &err,
                          size_t read_bytes);
};

typedef std::shared_ptr<ClientIO> ClientIO_ptr;
}
}