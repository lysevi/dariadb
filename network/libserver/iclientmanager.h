#pragma once

namespace dariadb {
namespace net {
//TODO remove?
class IClientManager {
public:
  virtual void client_connect(int id) = 0;
  virtual void client_disconnect(int id) = 0;
  virtual void write_begin() = 0;
  virtual void write_end() = 0;
    virtual ~IClientManager(){}
};
}
}
