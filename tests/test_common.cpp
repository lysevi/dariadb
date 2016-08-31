#include "test_common.h"
#include <flags.h>
#include <iostream>
#include <mutex>
#include <thread>
#include <thread>
#include <utils/exception.h>
#include <utils/utils.h>

namespace dariadb_test {
#undef NO_DATA
using namespace dariadb;
using namespace dariadb::storage;

class Callback : public storage::IReaderClb {
public:
	Callback() { count = 0; is_end_called = 0; }
  void call(const Meas &v) {
    std::lock_guard<std::mutex> lg(_locker);
    count++;
    all.push_back(v);
  }
  void is_end()override {
	  is_end_called++;
  }
  size_t count;
  Meas::MeasList all;
  std::mutex _locker;
  std::atomic_int is_end_called;
};

void checkAll(Meas::MeasList res, std::string msg, Time from, Time to, Time step) {

  Id id_val(0);
  Flag flg_val(0);
  for (auto i = from; i < to; i += step) {
    size_t count = 0;
    for (auto &m : res) {
      if ((m.id == id_val) && ((m.flag == flg_val) || (m.flag == Flags::_NO_DATA)) &&
          ((m.src == flg_val) || (m.src == Flags::_NO_DATA))) {
        count++;
      }
    }
    if (count < copies_count) {
      throw MAKE_EXCEPTION("count < copies_count");
    }
    ++id_val;
    ++flg_val;
  }
}

void check_reader_of_all(Meas::MeasList all, Time from, Time to, Time step,
                         size_t total_count, std::string message) {

  std::map<Id, Meas::MeasList> _dict;
  for (auto &v : all) {
    _dict[v.id].push_back(v);
  }

  Id cur_id = all.front().id;
  Value cur_val = all.front().value;
  auto it = all.cbegin();
  ++it;
  // values in  in each id must be sorted by value. value is eq of time in current check.
  for (; it != all.cend(); ++it) {
    auto cur_m = *it;
    if (cur_m.id != cur_id) {
      if (cur_m.id < cur_id) {
        throw MAKE_EXCEPTION("(it->id < cur_id)");
      } else {
        cur_id = cur_m.id;
        cur_val = cur_m.value;
        continue;
      }
    }
    if (cur_m.value < cur_val) {
      std::cout << cur_m.value << " => " << cur_val << std::endl;
      throw MAKE_EXCEPTION("(it->value < cur_val)");
    }
  }

  // check total count.
  auto all_sz = all.size();
  if (all_sz != total_count) {
    throw MAKE_EXCEPTION("(all.size() != total_count)");
  }

  checkAll(all, message, from, to, step);
}

size_t fill_storage_for_test(storage::IMeasStorage *as, Time from, Time to, Time step,
                             IdSet *_all_ids_set, Time *maxWritedTime) {
  auto m = Meas::empty();
  size_t total_count = 0;

  Id id_val = 0;

  Flag flg_val(0);
  for (auto i = from; i < to; i += step) {
    _all_ids_set->insert(id_val);
    m.id = id_val;
    m.flag = flg_val;
    m.src = flg_val;
    m.time = i;
    m.value = 0;

    auto copies_for_id = (id_val == 0 ? copies_count / 2 : copies_count);
    Meas::MeasArray values{copies_for_id};
    size_t pos = 0;
    for (size_t j = 1; j < copies_for_id + 1; j++) {
      *maxWritedTime = std::max(*maxWritedTime, m.time);
      values[pos] = m;
      total_count++;
      m.value = Value(j);
      m.time++;
      pos++;
    }
    if (as->append(values.begin(), values.end()).writed != values.size()) {
      throw MAKE_EXCEPTION("->append(m).writed != values.size()");
    }
    ++id_val;
    ++flg_val;
  }
  {
    auto new_from = copies_count / 2 + 1;
    id_val = 0;
    _all_ids_set->insert(id_val);
    m.id = id_val;
    m.flag = 0;
    m.src = 0;
    m.time = new_from;
    m.value = 0;
    ++id_val;
    ++flg_val;
    Meas::MeasList mlist;
    for (size_t j = new_from; j < copies_count + 1; j++) {
      m.value = Value(j);
      *maxWritedTime = std::max(*maxWritedTime, m.time);
      mlist.push_back(m);
      total_count++;
      m.time++;
    }

    if (as->append(mlist.begin(), mlist.end()).writed != mlist.size()) {
      throw MAKE_EXCEPTION("->append(m).writed != mlist.size()");
    }
  }
  return total_count;
}

void minMaxCheck(storage::IMeasStorage *as, Time from, Time maxWritedTime) {
  Time minTime, maxTime;
  if (!(as->minMaxTime(Id(0), &minTime, &maxTime))) {
    throw MAKE_EXCEPTION("!(as->minMaxTime)");
  }
  if (minTime == MAX_TIME && maxTime == MIN_TIME) {
    throw MAKE_EXCEPTION("minTime == MAX_TIME && maxTime == MIN_TIME");
  }
  auto max_res = as->maxTime();
  if (max_res != maxWritedTime) {
    throw MAKE_EXCEPTION("max_res != maxWritedTime");
  }

  auto min_res = as->minTime();
  if (min_res != from) {
    throw MAKE_EXCEPTION("(min_res != from)");
  }
}

void readIntervalCheck(storage::IMeasStorage *as, Time from, Time to, Time step,
                       const IdSet &_all_ids_set, const IdArray &_all_ids_array,
                       size_t total_count, bool check_stop_flag) {
  storage::QueryInterval qi_all(_all_ids_array, 0, from, to + copies_count);
  Meas::MeasList all = as->readInterval(qi_all);

  check_reader_of_all(all, from, to, step, total_count, "readAll error: ");
  std::unique_ptr<Callback> clbk{new Callback};
  as->foreach (qi_all, clbk.get());

  if (all.size() != clbk->count) {
    THROW_EXCEPTION_SS("all.size()!=clbk->count: " << all.size() << "!=" << clbk->count);
  }
  if (check_stop_flag && clbk->is_end_called!=1) {
	  THROW_EXCEPTION_SS("clbk->is_end_called!=1: "<< clbk->is_end_called);
  }
  IdArray ids(_all_ids_set.begin(), _all_ids_set.end());

  all = as->readInterval(storage::QueryInterval(
      ids, 0, to + copies_count - copies_count / 3, to + copies_count));
  if (all.size() == size_t(0)) {
    throw MAKE_EXCEPTION("all.size() != == size_t(0)");
  }

  ids.clear();
  ids.push_back(2);
  Meas::MeasList fltr_res{};
  fltr_res = as->readInterval(storage::QueryInterval(ids, 0, from, to + copies_count));

  if (fltr_res.size() != copies_count) {
    throw MAKE_EXCEPTION("fltr_res.size() != copies_count");
  }
  // check filter by source;
  {
    fltr_res.clear();
    fltr_res = as->readInterval(
        storage::QueryInterval(IdArray{_all_ids_set.begin(), _all_ids_set.end()}, 0,
                               Flag{2}, from, to + copies_count));

    if (fltr_res.size() != copies_count) {
      throw MAKE_EXCEPTION("(fltr_res.size() != copies_count)");
    }
    for (auto v : fltr_res) {
      if (v.src != Flag{2}) {
        throw MAKE_EXCEPTION("(m.src != Flag{ 2 })");
      }
    }
  }
}

void readTimePointCheck(storage::IMeasStorage *as, Time from, Time to, Time step,
                        const IdArray &_all_ids_array, bool check_stop_flag) {
  auto qp = storage::QueryTimePoint(_all_ids_array, 0, to + copies_count);
  auto all_id2meas = as->readInTimePoint(qp);

  size_t ids_count = (size_t)((to - from) / step);
  if (all_id2meas.size() < ids_count) {
    throw MAKE_EXCEPTION("all.size() < ids_count. must be GE");
  }

  std::unique_ptr<Callback> qpoint_clbk{new Callback};
  as->foreach (qp, qpoint_clbk.get());
  if (qpoint_clbk->count != all_id2meas.size()) {
    throw MAKE_EXCEPTION("qpoint_clbk->count != all_id2meas.size()");
  }

  if (check_stop_flag && qpoint_clbk->is_end_called != 1) {
	  THROW_EXCEPTION_SS("clbk->is_end_called!=1: "<< qpoint_clbk->is_end_called);
  }

  qp.time_point = to + copies_count;
  auto fltr_res_tp = as->readInTimePoint(qp);
  if (fltr_res_tp.size() < ids_count) {
    throw MAKE_EXCEPTION("fltr_res.size() < ids_count. must be GE");
  }

  IdArray notExstsIDs{9999};
  fltr_res_tp.clear();
  qp.ids = notExstsIDs;
  qp.time_point = to - 1;
  fltr_res_tp = as->readInTimePoint(qp);
  if (fltr_res_tp.size() != size_t(1)) { // must return NO_DATA
    throw MAKE_EXCEPTION("fltr_res.size() != size_t(1)");
  }

  Meas m_not_exists = fltr_res_tp[notExstsIDs.front()];
  if (m_not_exists.flag != Flags::_NO_DATA) {
    throw MAKE_EXCEPTION("fltr_res.front().flag != Flags::NO_DATA");
  }
}

void storage_test_check(storage::IMeasStorage *as, Time from, Time to, Time step, bool check_stop_flag) {
  IdSet _all_ids_set;
  Time maxWritedTime = MIN_TIME;
  size_t total_count =
      fill_storage_for_test(as, from, to, step, &_all_ids_set, &maxWritedTime);
  as->flush();
  minMaxCheck(as, from, maxWritedTime);

  Meas::Id2Meas current_mlist;
  IdArray _all_ids_array(_all_ids_set.begin(), _all_ids_set.end());
  current_mlist = as->currentValue(_all_ids_array, 0);

  if (current_mlist.size() == 0) {
    throw MAKE_EXCEPTION("current_mlist.size()>0");
  }

  as->flush();
  if (as->minTime() != from) {
    throw MAKE_EXCEPTION("as->minTime() != from");
  }
  if (as->maxTime() < to) {
    throw MAKE_EXCEPTION("as->maxTime() < to");
  }

  readIntervalCheck(as, from, to, step, _all_ids_set, _all_ids_array, total_count, check_stop_flag);
  readTimePointCheck(as, from, to, step, _all_ids_array, check_stop_flag);
}
/*
void readIntervalCommonTest(storage::MeasStorage *ds) {
  Meas m;
  {
    m.id = 1;
    m.time = 1;
    ds->append(m);
    m.id = 2;
    m.time = 2;
    ds->append(m);

    m.id = 4;
    m.time = 4;
    ds->append(m);
    m.id = 5;
    m.time = 5;
    ds->append(m);
    m.id = 55;
    m.time = 5;
    ds->append(m);

    IdArray all_id = {1, 2, 4, 5, 55};
    {
      auto tp_reader = ds->readInTimePoint({all_id, 0, 6});
      Meas::MeasList output_in_point{};
      tp_reader->readAll(&output_in_point);

      if (output_in_point.size() != size_t(5)) {
        throw MAKE_EXCEPTION("(output_in_point.size() != size_t(5))");
      }

      auto rdr = ds->readInterval(storage::QueryInterval(all_id, 0, 0, 6));
      output_in_point.clear();
      rdr->readAll(&output_in_point);
      if (output_in_point.size() != size_t(5)) {
        throw MAKE_EXCEPTION("(output_in_point.size() != size_t(5))");
      }
    }
    {

      auto tp_reader = ds->readInTimePoint({all_id, 0, 3});
      Meas::MeasList output_in_point{};
      tp_reader->readAll(&output_in_point);

      ///+ timepoint(3) with no_data
      if (output_in_point.size() != size_t(2 + 3)) {
        throw MAKE_EXCEPTION("(output_in_point.size() != size_t(2 + 3))");
      }
      for (auto v : output_in_point) {
        if (!(v.time <= 3)) {
          throw MAKE_EXCEPTION("!(v.time <= 3)");
        }
      }
    }
    auto reader = ds->readInterval(storage::QueryInterval(all_id, 0, 3, 5));
    Meas::MeasList output{};
    reader->readAll(&output);
    // if (output.size() != size_t(5 + 3)) { //+ timepoint(3) with no_data
    //  throw MAKE_EXCEPTION("output.size() != size_t(5 + 3)");
    //}
    if (output.size() != size_t(5)) { //+ timepoint(3) with no_data
      throw MAKE_EXCEPTION("output.size() != size_t(5)");
    }
  }
  // from this point read not from firsts.
  {
    m.id = 1;
    m.time = 6;
    ds->append(m);
    m.id = 2;
    m.time = 7;
    ds->append(m);

    m.id = 4;
    m.time = 9;
    ds->append(m);
    m.id = 5;
    m.time = 10;
    ds->append(m);
    m.id = 6;
    m.time = 10;
    ds->append(m);
    IdArray second_all_id = {1, 2, 4, 5, 6, 55};
    {

      auto tp_reader = ds->readInTimePoint({second_all_id, 0, 8});
      Meas::MeasList output_in_point{};
      tp_reader->readAll(&output_in_point);

      ///+ timepoimt(8) with no_data
      if (output_in_point.size() != size_t(5 + 1)) {
        throw MAKE_EXCEPTION("(output_in_point.size() != size_t(5 + 1))");
      }
      for (auto v : output_in_point) {
        if (!(v.time <= 8)) {
          throw MAKE_EXCEPTION("!(v.time <= 8))");
        }
      }
    }

    auto reader = ds->readInterval(
        storage::QueryInterval(IdArray{1, 2, 4, 5, 55}, 0, 8, 10));
    Meas::MeasList output{};
    reader->readAll(&output);
    if (output.size() != size_t(7)) {
      throw MAKE_EXCEPTION("output.size() != size_t(7)");
    }
    // expect: {1,8} {2,8} {4,8} {55,8} {4,9} {5,10}
    if (output.size() != size_t(7)) {
      std::cout << " ERROR!!!!" << std::endl;

      for (Meas v : output) {
        std::cout << " id:" << v.id << " flg:" << v.flag << " v:" << v.value
                  << " t:" << v.time << std::endl;
      }
      throw MAKE_EXCEPTION("!!!");
    }
  }
}
*/
}
