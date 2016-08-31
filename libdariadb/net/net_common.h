#pragma once

#include <ostream>
#include <string>

namespace dariadb {
namespace net {

const std::string OK_ANSWER = "OK";
const std::string ERROR_ANSWER = "error";
const std::string HELLO_PREFIX = "hello";
const std::string DISCONNECT_PREFIX = "bye";
const std::string DISCONNECT_ANSWER = "bye";
const std::string PING_QUERY = "ping";
const std::string PONG_ANSWER = "pong";
const std::string WRITE_QUERY = "write";
const std::string READ_INTERVAL_QUERY = "read_interval";

enum class ClientState {
  CONNECT, // connection is beginning but a while not ended.
  WORK,    // normal client.
  DISCONNECTED
};

std::ostream &operator<<(std::ostream &stream, const ClientState &state);

/// before send big data, send to client "kind"' '"size"\n. then send 'data'.
struct NetData{
    int query_id;
    uint64_t size;
    std::string kind;
    uint8_t *data;
    ~NetData(){
        delete[] data;
    }
};

}
}
