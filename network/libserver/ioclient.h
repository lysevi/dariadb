#pragma once

#include <atomic>
#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
#include <memory>

#include <libdariadb/engine.h>
#include <libdariadb/interfaces/icallbacks.h>
#include <libdariadb/meas.h>
#include <common/net_common.h>
#include <common/async_connection.h>
#include <libserver/iclientmanager.h>

namespace dariadb {
namespace net {
   const int PING_TIMER_INTERVAL = 1000;
struct IOClient{

  struct Environment {
    Environment() {
      srv = nullptr;
      storage = nullptr;
      nd_pool = nullptr;
    }
    IClientManager *srv;
    storage::Engine *storage;
    NetData_Pool *nd_pool;
    boost::asio::io_service *service;
  };

  struct ClientDataReader : public storage::IReaderClb {
    static const size_t BUFFER_LENGTH =
        (NetData::MAX_MESSAGE_SIZE - sizeof(QueryAppend_header)) / sizeof(Meas);
    utils::Locker _locker;
    IOClient *_parent;
    QueryNumber _query_num;
    size_t pos;
    std::array<Meas, BUFFER_LENGTH> _buffer;

    ClientDataReader(IOClient *parent, QueryNumber query_num);
    ~ClientDataReader();
    void call(const Meas &m) override;
    void is_end() override;
    void send_buffer();
  };

  IOClient(int _id, socket_ptr &_sock, Environment *_env);
  ~IOClient();
  void start(){
      _async_connection->start(sock);
  }
  void end_session();
  void close();
  void ping();

  void onDataRecv(const NetData_ptr &d, bool &cancel, bool &dont_free_memory);
  void onNetworkError(const boost::system::error_code &err);

  void append(const NetData_ptr &d);
  void readInterval(const NetData_ptr &d);
  void readTimePoint(const NetData_ptr &d);
  void currentValue(const NetData_ptr &d);
  void subscribe(const NetData_ptr &d);
  void sendOk(QueryNumber query_num);
  void sendError(QueryNumber query_num, const ERRORS &err);

  // data - queryInterval or QueryTimePoint
  void readerAdd(ClientDataReader*cdr, void*data);
  void readerRemove(QueryNumber number);
  Time _last_query_time;
  socket_ptr sock;
  std::string host;

  CLIENT_STATE state;
  Environment *env;
  std::atomic_int pings_missed;
  //std::list<storage::IReaderClb *> readers;
  std::shared_ptr<storage::IReaderClb> subscribe_reader;
  std::shared_ptr<AsyncConnection> _async_connection;
  
  std::map<QueryNumber, std::pair<ClientDataReader*, void*>> _readers;
  std::mutex _readers_lock;
};

typedef std::shared_ptr<IOClient> ClientIO_ptr;
}
}
