#include "test_common.h"
#include <flags.h>
#include <thread>
#include <utils/exception.h>
#include <utils/utils.h>

namespace dariadb_test {
#undef NO_DATA

void checkAll(dariadb::Meas::MeasList res, std::string msg, dariadb::Time from,
              dariadb::Time to, dariadb::Time step) {

  dariadb::Id id_val(0);
  dariadb::Flag flg_val(0);
  for (auto i = from; i < to; i += step) {
    size_t count = 0;
    for (auto &m : res) {
      if ((m.id == id_val) &&
          ((m.flag == flg_val) || (m.flag == dariadb::Flags::_NO_DATA)) &&
          ((m.src == flg_val) || (m.src == dariadb::Flags::_NO_DATA))) {
        count++;
	  }
	 /* if ((m.id == id_val) &&
		  (((m.flag != flg_val)) ||
		  ((m.src != flg_val)))) {
		  std::cout<<1;
	  }*/
    }

    if (count < copies_count) {
      throw MAKE_EXCEPTION("count < copies_count");
    }
    ++id_val;
    ++flg_val;
  }
}

void check_reader_of_all(dariadb::storage::Reader_ptr reader, dariadb::Time from,
                         dariadb::Time to, dariadb::Time step, size_t, size_t total_count,
                         std::string message) {
  dariadb::Meas::MeasList all{};
  reader->readAll(&all);

  std::map<dariadb::Id, dariadb::Meas::MeasList> _dict;
  for (auto &v : all) {
    _dict[v.id].push_back(v);
  }

  dariadb::Id cur_id = all.front().id;
  dariadb::Value cur_val = all.front().value;
  auto it = all.cbegin();
  ++it;
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

  auto all_sz = all.size();
  if (all_sz != total_count) {
    throw MAKE_EXCEPTION("(all.size() != total_count)");
  }
  // TODO reset is sucks
  reader->reset();
  auto readed_ids = reader->getIds();
  if (readed_ids.size() != _dict.size()) {
    throw MAKE_EXCEPTION("(readed_ids.size() != size_t((to - from) / step))");
  }

  checkAll(all, message, from, to, step);
}

void storage_test_check(dariadb::storage::MeasStorage *as, dariadb::Time from,
                        dariadb::Time to, dariadb::Time step) {
  auto m = dariadb::Meas::empty();
  size_t total_count = 0;

  dariadb::Id id_val(0);

  dariadb::Flag flg_val(0);
  dariadb::IdSet _all_ids_set;
  for (auto i = from; i < to; i += step) {
    _all_ids_set.insert(id_val);
    m.id = id_val;
    m.flag = flg_val;
    m.src = flg_val;
    m.time = i;
    m.value = 0;

    auto copies_for_id = (id_val == 0 ? copies_count / 2 : copies_count);
    for (size_t j = 1; j < copies_for_id + 1; j++) {
      if (as->append(m).writed != 1) {
        throw MAKE_EXCEPTION("->append(m).writed != 1");
      }
      total_count++;
      m.value = dariadb::Value(j);
      m.time++;
    }
    ++id_val;
    ++flg_val;
  }
  {
    auto new_from = copies_count / 2 + 1;
    id_val = 0;
    _all_ids_set.insert(id_val);
    m.id = id_val;
    m.flag = 0;
    m.src = 0;
    m.time = new_from;
    m.value = 0;
    ++id_val;
    ++flg_val;
    for (size_t j = new_from; j < copies_count + 1; j++) {
      m.value = dariadb::Value(j);
      if (as->append(m).writed != 1) {
        throw MAKE_EXCEPTION("->append(m).writed != 1");
      }
      total_count++;
      m.time++;
    }
  }
  dariadb::Time minTime, maxTime;
  if (!(as->minMaxTime(dariadb::Id(id_val - 1), &minTime, &maxTime))) {
    throw MAKE_EXCEPTION("!(as->minMaxTime)");
  }
  if (minTime == dariadb::MAX_TIME && maxTime == dariadb::MIN_TIME) {
    throw MAKE_EXCEPTION("minTime == dariadb::MAX_TIME && maxTime == dariadb::MIN_TIME");
  }
  dariadb::Meas::MeasList current_mlist;
  dariadb::IdArray _all_ids_array(_all_ids_set.begin(), _all_ids_set.end());
  auto current_vals_rdr=as->currentValue(_all_ids_array, 0);
  if (current_vals_rdr == nullptr) {
    throw MAKE_EXCEPTION("current_vals_rdr == nullptr");
  }
  current_vals_rdr->readAll(&current_mlist);
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
  auto reader = as->readInterval(
      dariadb::storage::QueryInterval(_all_ids_array, 0, from, to + copies_count));
  check_reader_of_all(reader, from, to, step, id_val, total_count, "readAll error: ");

  auto cloned_reader = reader->clone();
  cloned_reader->reset();
  check_reader_of_all(cloned_reader, from, to, step, id_val, total_count,
                      "cloned readAll error: ");

  cloned_reader->reset();
  auto cl_readed_ids = cloned_reader->getIds();
  if (cl_readed_ids.size() != _all_ids_set.size()) {
    throw MAKE_EXCEPTION("(cl_readed_ids.size() != _all_ids_set.size())");
  }

  dariadb::IdArray ids(_all_ids_set.begin(), _all_ids_set.end());
  dariadb::Meas::MeasList all{};
  as->readInterval(dariadb::storage::QueryInterval(ids, 0, from, to + copies_count))
      ->readAll(&all);
  if (all.size() != total_count) {
    throw MAKE_EXCEPTION("all.size() != total_count");
  }

  checkAll(all, "read error: ", from, to, step);
  all.clear();

  as->readInterval(dariadb::storage::QueryInterval(
                       ids, 0, to + copies_count - copies_count / 3, to + copies_count))
      ->readAll(&all);
  if (all.size() == size_t(0)) {
    throw MAKE_EXCEPTION("all.size() != == size_t(0)");
  }

  ids.clear();
  ids.push_back(2);
  dariadb::Meas::MeasList fltr_res{};
  as->readInterval(dariadb::storage::QueryInterval(ids, 0, from, to + copies_count))
      ->readAll(&fltr_res);

  if (fltr_res.size() != copies_count) {
    throw MAKE_EXCEPTION("fltr_res.size() != copies_count");
  }

  all.clear();
  auto qp = dariadb::storage::QueryTimePoint(
      dariadb::IdArray(_all_ids_set.begin(), _all_ids_set.end()), 0, to + copies_count);
  auto tp_reader = as->readInTimePoint(qp);
  tp_reader->readAll(&all);
  size_t ids_count = (size_t)((to - from) / step);
  if (all.size() < ids_count) {
    throw MAKE_EXCEPTION("all.size() < ids_count. must be GE");
  }

  tp_reader->reset();
  auto readed_ids = tp_reader->getIds();
  if (readed_ids.size() != _all_ids_set.size()) {
    throw MAKE_EXCEPTION("(readed_ids.size() != _all_ids_set.size())");
  }

  fltr_res.clear();
  qp.time_point = to + copies_count;
  as->readInTimePoint(qp)->readAll(&fltr_res);
  if (fltr_res.size() < ids_count) {
    throw MAKE_EXCEPTION("fltr_res.size() < ids_count. must be GE");
  }

  dariadb::IdArray notExstsIDs{9999};
  fltr_res.clear();
  qp.ids = notExstsIDs;
  qp.time_point = to - 1;
  as->readInTimePoint(qp)->readAll(&fltr_res);
  if (fltr_res.size() != size_t(1)) { // must return NO_DATA
    throw MAKE_EXCEPTION("fltr_res.size() != size_t(1)");
  }

  if (fltr_res.front().flag != dariadb::Flags::_NO_DATA) {
    throw MAKE_EXCEPTION("fltr_res.front().flag != dariadb::Flags::NO_DATA");
  }
}

void readIntervalCommonTest(dariadb::storage::MeasStorage *ds) {
  dariadb::Meas m;
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

    dariadb::IdArray all_id = {1, 2, 4, 5, 55};
    {
      auto tp_reader = ds->readInTimePoint({all_id, 0, 6});
      dariadb::Meas::MeasList output_in_point{};
      tp_reader->readAll(&output_in_point);

      if (output_in_point.size() != size_t(5)) {
        throw MAKE_EXCEPTION("(output_in_point.size() != size_t(5))");
      }

      auto rdr = ds->readInterval(dariadb::storage::QueryInterval(all_id, 0, 0, 6));
      output_in_point.clear();
      rdr->readAll(&output_in_point);
      if (output_in_point.size() != size_t(5)) {
        throw MAKE_EXCEPTION("(output_in_point.size() != size_t(5))");
      }
    }
    {

      auto tp_reader = ds->readInTimePoint({all_id, 0, 3});
      dariadb::Meas::MeasList output_in_point{};
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
    auto reader = ds->readInterval(dariadb::storage::QueryInterval(all_id, 0, 3, 5));
    dariadb::Meas::MeasList output{};
    reader->readAll(&output);
    if (output.size() != size_t(5 + 3)) { //+ timepoint(3) with no_data
      throw MAKE_EXCEPTION("output.size() != size_t(5 + 3)");
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
    dariadb::IdArray second_all_id = {1, 2, 4, 5, 6, 55};
    {

      auto tp_reader = ds->readInTimePoint({second_all_id, 0, 8});
      dariadb::Meas::MeasList output_in_point{};
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
        dariadb::storage::QueryInterval(dariadb::IdArray{1, 2, 4, 5, 55}, 0, 8, 10));
    dariadb::Meas::MeasList output{};
    reader->readAll(&output);
    if (output.size() != size_t(7)) {
      throw MAKE_EXCEPTION("output.size() != size_t(7)");
    }
    // expect: {1,8} {2,8} {4,8} {55,8} {4,9} {5,10}
    if (output.size() != size_t(7)) {
      std::cout << " ERROR!!!!" << std::endl;

      for (dariadb::Meas v : output) {
        std::cout << " id:" << v.id << " flg:" << v.flag << " v:" << v.value
                  << " t:" << v.time << std::endl;
      }
      throw MAKE_EXCEPTION("!!!");
    }
  }
}
}
