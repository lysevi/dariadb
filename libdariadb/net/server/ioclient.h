#pragma once

#include <atomic>
#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
#include <memory>

#include "../interfaces/iclientmanager.h"
#include "../../interfaces/imeasstorage.h"
#include "../../meas.h"
#include "../net_common.h"
#include "common.h"

namespace dariadb {
namespace net {
struct ClientIO {
  int id;
  socket_ptr sock;
  boost::asio::streambuf query_buff;
  std::string host;

  ClientState state;
  IClientManager *srv;
  storage::IMeasStorage*storage;
  std::atomic_int pings_missed;
  Meas::MeasArray in_values_buffer;

  ClientIO(int _id, socket_ptr _sock, IClientManager *_srv, storage::IMeasStorage*_storage);
  void disconnect();
  void ping();

  void readNextQuery();
  void readHello();
  void onHello(const boost::system::error_code &err, size_t read_bytes);
  void onReadQuery(const boost::system::error_code &err, size_t read_bytes);
  void onPingSended(const boost::system::error_code &err, size_t read_bytes);
  void onOkSended(const boost::system::error_code &err, size_t read_bytes);
  void onDisconnectSended(const boost::system::error_code &err,
                          size_t read_bytes);
  void onReadValues(size_t values_count,const boost::system::error_code &err, size_t read_bytes);
  void onReadIntervalAnswerSended(const boost::system::error_code &err, size_t read_bytes);
  void onReadIntervalValuesSended(const boost::system::error_code &err, size_t read_bytes);
};

typedef std::shared_ptr<ClientIO> ClientIO_ptr;
}
}
