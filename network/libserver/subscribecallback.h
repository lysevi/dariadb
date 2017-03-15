#pragma once

#include <libdariadb/meas.h>
#include <libserver/ioclient.h>
#include <cassert>

namespace dariadb {
namespace net {

struct SubscribeCallback : public IReadCallback {
  utils::async::Locker _locker;
  IOClient *_parent;
  QueryNumber _query_num;

  SubscribeCallback(IOClient *parent, QueryNumber query_num) {
    _parent = parent;
    _query_num = query_num;
  }
  ~SubscribeCallback() {}
  void apply(const Meas &m) override { send_buffer(m); }
  void is_end() override { IReadCallback::is_end(); }
  void send_buffer(const Meas &m);
};
}
}