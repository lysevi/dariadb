#pragma once
#include <libdariadb/interfaces/iengine.h>
#include <string>

namespace dariadb {
namespace net {
namespace http {

struct reply;
struct request;

/// The common handler for all incoming requests.
class request_handler {
public:
  request_handler(const request_handler &) = delete;
  request_handler &operator=(const request_handler &) = delete;

  explicit request_handler();
  void handle_request(const request &req, reply &rep);

  void set_storage(dariadb::IEngine_Ptr &storage_engine) {
    _storage_engine = storage_engine;
  }

private:
  dariadb::IEngine_Ptr _storage_engine;
};

} // namespace http
} // namespace net
} // namespace dariadb
