#pragma once

#include <atomic>
#include <boost/asio.hpp>
#include <boost/system/error_code.hpp>
#include <memory>

#include "../../interfaces/icallbacks.h"
#include "../../interfaces/imeasstorage.h"
#include "../../meas.h"
#include "../async_connection.h"
#include "../interfaces/iclientmanager.h"
#include "../net_common.h"

namespace dariadb {
namespace net {

struct IOClient : public AsyncConnection {

  struct Environment {
    Environment() {
      srv = nullptr;
      storage = nullptr;
      nd_pool = nullptr;
      write_meases_strand = nullptr;
    }
    IClientManager *srv;
    storage::IMeasStorage *storage;
    NetData_Pool *nd_pool;
    boost::asio::io_service::strand *write_meases_strand;
    boost::asio::io_service::strand *read_meases_strand;
    boost::asio::io_service *service;
  };

  struct ClientDataReader:public storage::IReaderClb{
      static const size_t BUFFER_LENGTH=(NetData::MAX_MESSAGE_SIZE - sizeof(QueryWrite_header))/sizeof(Meas);
      utils::Locker _locker;
      IOClient*_parent;
      QueryNumber _query_num;
      size_t pos;
      std::array<Meas, BUFFER_LENGTH> _buffer;
      ClientDataReader(IOClient*parent,QueryNumber query_num){
          _parent=parent;
          pos=0;
          _query_num=query_num;
      }
      void call(const Meas &m){
          std::lock_guard<utils::Locker> lg(_locker);
        if(pos==BUFFER_LENGTH){
            send_buffer();
            pos=0;
        }
         _buffer[pos++]=m;
      }

      void is_end(){
          send_buffer();

          auto nd = _parent->env->nd_pool->construct(DataKinds::WRITE);
          nd->size += sizeof(QueryWrite_header);
          auto hdr = reinterpret_cast<QueryWrite_header*>(&nd->data);
          hdr->id = _query_num;
          hdr->count = 0;
          _parent->send(nd);
      }

      void send_buffer(){
          if(pos==0){
              return;
          }
          auto nd = _parent->env->nd_pool->construct(DataKinds::WRITE);
          nd->size += sizeof(QueryWrite_header);

          auto hdr = reinterpret_cast<QueryWrite_header*>(&nd->data);
          hdr->id = _query_num;
          hdr->count = static_cast<uint32_t>(pos);

          auto size_to_write = hdr->count * sizeof(Meas);

          auto meas_ptr = (Meas*)((char*)(&hdr->count) + sizeof(hdr->count));
          nd->size += static_cast<NetData::MessageSize>(size_to_write);

          auto it = _buffer.begin();
          size_t i = 0;
          for (; it != _buffer.end() && i< hdr->count; ++it, ++i) {
              *meas_ptr = *it;
              ++meas_ptr;
          }
          _parent->send(nd);
      }
      ~ClientDataReader() {}
  };

  socket_ptr sock;
  std::string host;

  ClientState state;
  Environment *env;
  std::atomic_int pings_missed;
  std::list<storage::IReaderClb*> readers;

  IOClient(int _id, socket_ptr &_sock, Environment *_env);
  ~IOClient();
  void end_session();
  void close();
  void ping();

  void onDataRecv(const NetData_ptr &d, bool &cancel, bool &dont_free_memory) override;
  void onNetworkError(const boost::system::error_code &err) override;

  void writeMeasurementsCall(const NetData_ptr &d);
  void readInterval(const NetData_ptr &d);
  void readTimePoint(const NetData_ptr &d);
  void sendOk(QueryNumber query_num);
  void sendError(QueryNumber query_num);
};

typedef std::shared_ptr<IOClient> ClientIO_ptr;
}
}
