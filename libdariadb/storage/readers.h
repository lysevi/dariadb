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

class LinearReader : public IReader {
public:
  EXPORT LinearReader(const std::list<Reader_Ptr> &readers);
  EXPORT virtual Meas readNext() override;
  EXPORT bool is_end() const override;
  EXPORT Meas top() override;
  EXPORT Time minTime() override;
  EXPORT Time maxTime() override;

  std::list<Reader_Ptr> _readers;
  Time _minTime;
  Time _maxTime;
};

struct ReaderFactory {
  // if the intervals overlap.
  EXPORT static bool is_linear_readers(const Reader_Ptr &r1,
                                       const Reader_Ptr &r2);
  EXPORT static Reader_Ptr colapseReaders(const std::list<Reader_Ptr> &i2r);
  EXPORT static Id2Reader colapseReaders(const Id2ReadersList &i2r);
};
}
}