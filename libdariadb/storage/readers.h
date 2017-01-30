#pragma once

#include <libdariadb/interfaces/ireader.h>

namespace dariadb {
namespace storage {
class FullReader : public IReader {
public:
  EXPORT FullReader(MeasArray &ml);
  EXPORT virtual Meas readNext() override;

  EXPORT bool is_end() const override;

  EXPORT Meas top() override;

  MeasArray _ma;
  size_t _index;
};
}
}