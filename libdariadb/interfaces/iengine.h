#pragma once
#include <libdariadb/interfaces/imeasstorage.h>
#include <memory>
namespace dariadb {

class IEngine : public IMeasStorage {
public:
  virtual void fsck() = 0;
  virtual void eraseOld(const Time &t) = 0;
  virtual void repack() = 0;
  virtual void stop() = 0;
};
using IEngine_Ptr = std::shared_ptr<IEngine>;

}