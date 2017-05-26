#pragma once

#include <libdariadb/interfaces/icursor.h>
#include <list>
#include <vector>

namespace dariadb {
namespace storage {

/**
Store all values in memory.
*/
class FullCursor final : public ICursor {
public:
  EXPORT FullCursor(MeasArray &&ma);
  EXPORT virtual Meas readNext() override;

  EXPORT bool is_end() const override;

  EXPORT Meas top() override;
  EXPORT Time minTime() override;
  EXPORT Time maxTime() override;

  EXPORT size_t count() const override;
  MeasArray _ma;
  size_t _index;

  Time _minTime;
  Time _maxTime;
};

/**
Merge sort.
*/
class MergeSortCursor final : public ICursor {
public:
  EXPORT MergeSortCursor(CursorsList &&readers);
  EXPORT virtual Meas readNext() override;
  EXPORT bool is_end() const override;
  EXPORT Meas top() override;
  EXPORT Time minTime() override;
  EXPORT Time maxTime() override;
  EXPORT size_t count() const override;

  size_t _values_count;
  std::vector<Cursor_Ptr> _readers;
  std::vector<Time> _top_times;
  std::vector<bool> _is_end_status;
  Time _minTime;
  Time _maxTime;
};

/**
Sequential read
*/
class LinearCursor final : public ICursor {
public:
  EXPORT LinearCursor(CursorsList &&readers);
  EXPORT virtual Meas readNext() override;
  EXPORT bool is_end() const override;
  EXPORT Meas top() override;
  EXPORT Time minTime() override;
  EXPORT Time maxTime() override;
  EXPORT size_t count() const override;

  size_t _values_count;
  std::list<Cursor_Ptr> _readers;
  Time _minTime;
  Time _maxTime;
};

/**
make LinearCursor or MergSortCursor
*/
struct CursorWrapperFactory {
  // if the intervals overlap.
  EXPORT static bool is_linear_readers(const Cursor_Ptr &r1, const Cursor_Ptr &r2);
  EXPORT static Cursor_Ptr colapseCursors(CursorsList &&i2r);
  EXPORT static Id2Cursor colapseCursors(Id2CursorsList &&i2r);
};
}
}
