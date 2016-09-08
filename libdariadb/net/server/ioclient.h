#pragma once

#include <atomic>
#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
#include <memory>

#include "../interfaces/iclientmanager.h"
#include "../../interfaces/imeasstorage.h"
#include "../../meas.h"
#include "../net_common.h"
#include "../async_connection.h"

namespace dariadb {
namespace net {
	
struct ClientIO:public AsyncConnection {
	struct Environment {
		Environment() {
			srv = nullptr;
			storage = nullptr;
			nd_pool = nullptr;
			write_meases_strand = nullptr;
		}
		IClientManager *srv;
		storage::IMeasStorage*storage;
		NetData_Pool*nd_pool;
		boost::asio::io_service::strand *write_meases_strand;
		boost::asio::io_service::strand *read_meases_strand;
		boost::asio::io_service *service;
	};

  socket_ptr sock;
  std::string host;

  ClientState state;
  Environment *env;
  std::atomic_int pings_missed;

  ClientIO(int _id, socket_ptr&_sock, Environment *_env);
  ~ClientIO();
  void end_session();
  void close();
  void ping();
  
  void onDataRecv(const NetData_ptr&d, bool&cancel, bool&dont_free_memory) override;
  void onNetworkError(const boost::system::error_code&err)override;

  void writeMeasurementsCall(const NetData_ptr&d);
  void readInterval(const NetData_ptr&d);
  void sendOk(int32_t query_num);
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
