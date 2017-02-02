#pragma once

#include <libdariadb/interfaces/ireader.h>
#include <list>
#include <vector>

namespace dariadb {
namespace storage {
class FullReader : public IReader {
public:
  EXPORT FullReader(MeasArray &ma);
  EXPORT virtual Meas readNext() override;

  EXPORT bool is_end() const override;

  EXPORT Meas top() override;
  EXPORT Time minTime() override;
  EXPORT Time maxTime() override;
  MeasArray _ma;
  size_t _index;

  Time _minTime;
  Time _maxTime;
};

class MergeSortReader : public IReader {
public:
  EXPORT MergeSortReader(const std::list<Reader_Ptr> &readers);
  EXPORT virtual Meas readNext() override;
  EXPORT bool is_end() const override;
  EXPORT Meas top() override;
  EXPORT Time minTime() override;
  EXPORT Time maxTime() override;

  std::vector<Reader_Ptr> _readers;
  std::vector<Time> _top_times;
  std::vector<bool> _is_end_status;
  Time _minTime;
  Time _maxTime;
};

struct ReaderFactory {
  EXPORT static Id2Reader colapseReaders(const Id2ReadersList &i2r);
};
}
}