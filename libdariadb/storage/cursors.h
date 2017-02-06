#pragma once

#include <libdariadb/interfaces/icursor.h>
#include <list>
#include <vector>

namespace dariadb {
namespace storage {

/**
Store all values in memory.
*/
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

/**
Merge sort.
*/
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

/**
Sequential read
*/
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

struct EmptyCursor : public ICursor {
  Meas readNext() override {
    NOT_IMPLEMENTED;
    return Meas();
  }
  Meas top() override {
    NOT_IMPLEMENTED;
    return Meas();
  }
  bool is_end() const override { return true; }
  Time minTime() override {
    NOT_IMPLEMENTED;
    return Time();
  }
  Time maxTime() override {
    NOT_IMPLEMENTED;
    return Time();
  }
};

/**
make LinearCursor or MergSortCursor
*/
struct CursorWrapperFactory {
  // if the intervals overlap.
  EXPORT static bool is_linear_readers(const Cursor_Ptr &r1,
                                       const Cursor_Ptr &r2);
  EXPORT static Cursor_Ptr colapseCursors(const CursorsList &i2r);
  EXPORT static Id2Cursor colapseCursors(const Id2CursorsList &i2r);
};

struct Join {
  using Table = std::list<MeasArray>;
  struct Callback {
    virtual void apply(const MeasArray &row) = 0;
  };

  struct TableMaker : public Callback {
    void apply(const MeasArray &a) override { result.push_back(a); }
    Join::Table result;
  };

  EXPORT static void join(const CursorsList &l, const IdArray&ids, Callback *clbk);
  EXPORT static Table makeTable(const CursorsList &l, const IdArray&ids);
};
}
}
