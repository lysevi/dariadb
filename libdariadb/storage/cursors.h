#pragma once

#include <libdariadb/interfaces/icursor.h>
#include <list>
#include <vector>

namespace dariadb {
namespace storage {

class FullCursor : public ICursor {
public:
  EXPORT FullCursor(MeasArray &ma);
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

class MergeSortCursor : public ICursor {
public:
  EXPORT MergeSortCursor(const CursorsList &readers);
  EXPORT virtual Meas readNext() override;
  EXPORT bool is_end() const override;
  EXPORT Meas top() override;
  EXPORT Time minTime() override;
  EXPORT Time maxTime() override;

  std::vector<Cursor_Ptr> _readers;
  std::vector<Time> _top_times;
  std::vector<bool> _is_end_status;
  Time _minTime;
  Time _maxTime;
};

class LinearCursor : public ICursor {
public:
  EXPORT LinearCursor(const CursorsList &readers);
  EXPORT virtual Meas readNext() override;
  EXPORT bool is_end() const override;
  EXPORT Meas top() override;
  EXPORT Time minTime() override;
  EXPORT Time maxTime() override;

  std::list<Cursor_Ptr> _readers;
  Time _minTime;
  Time _maxTime;
};

struct CursorWrapperFactory {
  // if the intervals overlap.
  EXPORT static bool is_linear_readers(const Cursor_Ptr &r1,
                                       const Cursor_Ptr &r2);
  EXPORT static Cursor_Ptr colapseReaders(const CursorsList &i2r);
  EXPORT static Id2Cursor colapseReaders(const Id2CursorsList &i2r);
};

struct Join {
  struct Callback {
    virtual void apply(const MeasArray &) = 0;
  };

  EXPORT static void join(const CursorsList &l, Callback *clbk);
};
}
}
