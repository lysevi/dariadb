#include "helpers.h"
#include <libdariadb/utils/exception.h>
#include <algorithm>
#include <mutex>
#include <random>
#include <thread>

#include <catch.hpp>

namespace dariadb_test {
#undef NO_DATA
using namespace dariadb;

class WriteCallback : public dariadb::IReadCallback {
public:
  WriteCallback(dariadb::IMeasStorage *store) {
    count = 0;
    is_end_called = false;
    storage = store;
  }
  void apply(const dariadb::Meas &m) override {
    count++;
    dariadb::Meas nm = m;
    nm.id = dariadb::MAX_ID - m.id;

    ENSURE(storage != nullptr);

    storage->append(nm);
  }

  void is_end() override {
    is_end_called = true;
    IReadCallback::is_end();
  }
  std::atomic<size_t> count;
  bool is_end_called;
  dariadb::IMeasStorage *storage;
};

class Callback : public IReadCallback {
public:
  Callback() {
    count = 0;
    is_end_called = 0;
  }
  void apply(const Meas &v) override {
    std::lock_guard<std::mutex> lg(_locker);
    count++;
    all.push_back(v);
  }
  void is_end() override {
    is_end_called++;
    IReadCallback::is_end();
  }
  size_t count;
  MeasArray all;
  std::mutex _locker;
  std::atomic_int is_end_called;
};

class OrderCheckCallback : public IReadCallback {
public:
  OrderCheckCallback() {
    is_greater = false;
    is_first = true;
  }
  void apply(const Meas &v) override {
    std::lock_guard<std::mutex> lg(_locker);
    all.push_back(v);
    if (is_first) {
      _last = v.time;
      is_first = false;
    } else {
      is_greater = _last <= v.time;
      _last = v.time;
    }
  }
  std::mutex _locker;
  dariadb::Time _last;
  bool is_first;
  bool is_greater;

  MeasArray all;
};

void checkAll(MeasArray res, std::string msg, Time from, Time to, Time step) {

  Id id_val(0);
  Flag flg_val(0);
  for (auto i = from; i < to; i += step) {
    size_t count = 0;
    for (auto &m : res) {
      if ((m.id == id_val) && ((m.flag == flg_val) || (m.flag == FLAGS::_NO_DATA))) {
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

void check_reader_of_all(MeasArray &all, Time from, Time to, Time step,
                         size_t total_count, std::string message) {

  std::map<Id, MeasesList> _dict;
  for (auto &v : all) {
    _dict[v.id].push_back(v);
  }

  Id cur_id = all.front().id;
  Value cur_val = all.front().value;
  auto it = all.cbegin();
  ++it;
  // values in  in each id must be sorted by value. value is eq of time in
  // current check.
  for (; it != all.cend(); ++it) {
    auto cur_m = *it;
    if (cur_m.id != cur_id) {
      cur_id = cur_m.id;
      cur_val = cur_m.value;
      continue;
    }
    if (cur_m.value < cur_val) {
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

size_t fill_storage_for_test(dariadb::IMeasStorage *as, dariadb::Time from,
                             dariadb::Time to, dariadb::Time step,
                             dariadb::IdSet *_all_ids_set, dariadb::Time *maxWritedTime,
                             bool random_timestamps) {
  std::random_device rd;
  std::mt19937 eng(rd());
  auto m = Meas();
  size_t total_count = 0;

  Id id_val = 0;

  Flag flg_val(0);
  
  dariadb::logger_info("reserve place for id");
  IdArray reservedIds;
  reservedIds.reserve(to - from);
  for (auto i = from; i < to; i += step) {
	  reservedIds.push_back(id_val);
	  ++id_val;
  }
  as->reserve(reservedIds);
  id_val = 0;
  for (auto i = from; i < to; i += step) {
    _all_ids_set->insert(id_val);
    m.id = id_val;
    m.flag = flg_val;
    m.time = i;
    m.value = 0;

    auto copies_for_id = (id_val == 0 ? copies_count / 2 : copies_count);
    MeasArray values(copies_for_id);
    size_t pos = 0;
    for (size_t j = 1; j < copies_for_id + 1; j++) {
      *maxWritedTime = std::max(*maxWritedTime, m.time);
      values[pos] = m;
      total_count++;
      m.value = Value(j);
      m.time++;
      pos++;
    }
    if (random_timestamps) {
      std::uniform_int_distribution<size_t> distr(0, values.size() - 1);
      for (size_t i = 0; i < values.size(); ++i) {
        size_t from = distr(eng);
        while (from == i) {
          from = distr(eng);
        }
        std::swap(values[i], values[from]);
      }
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
    m.time = new_from;
    m.value = 0;
    ++id_val;
    ++flg_val;
    MeasArray mlist;
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

void minMaxCheck(IMeasStorage *as, Time from, Time maxWritedTime) {
  auto stat = as->stat(Id(0), from, maxWritedTime);
  if (stat.count != copies_count) {
    throw MAKE_EXCEPTION("stat.count != copies_count");
  }

  if (stat.minTime != dariadb::MIN_TIME) {
    throw MAKE_EXCEPTION("stat.minTime != dariadb::MIN_TIME");
  }

  if (stat.maxTime != dariadb::Time(copies_count)) {
    throw MAKE_EXCEPTION("(stat.maxTime != dariadb::Time(copies_count))");
  }

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

void readIntervalCheck(IMeasStorage *as, Time from, Time to, Time step,
                       const IdSet &_all_ids_set, const IdArray &_all_ids_array,
                       size_t total_count, bool check_stop_flag) {

  IdArray ids(_all_ids_set.begin(), _all_ids_set.end());
  QueryInterval qi_all(_all_ids_array, 0, from, to + copies_count);
  MeasArray all = as->readInterval(qi_all);

  auto all_size = all.size();
  check_reader_of_all(all, from, to, step, total_count, "readAll error: ");
  auto clbk = new Callback();
  as->foreach (qi_all, clbk);

  while (clbk->count != all_size) {
    std::this_thread::yield();
  }

  if (all_size != clbk->count) {
    THROW_EXCEPTION("all.size()!=clbk->count: ", all.size(), "!=", clbk->count);
  }

  if (check_stop_flag) {
    clbk->wait();
  }
  delete clbk;

  all = as->readInterval(
      QueryInterval(ids, 0, to + copies_count - copies_count / 3, to + copies_count));
  if (all.size() == size_t(0)) {
    throw MAKE_EXCEPTION("all.size() != size_t(0)");
  }

  ids.clear();
  ids.push_back(2);
  MeasArray fltr_res{};
  fltr_res = as->readInterval(QueryInterval(ids, 0, from, to + copies_count));

  if (fltr_res.size() != copies_count) {
    throw MAKE_EXCEPTION("fltr_res.size() != copies_count");
  }

  if (check_stop_flag) { // this check works only when engine test.
                         // calls must be sorted by time.
    auto order_clbk = new OrderCheckCallback();
    IdArray zero_id;
    zero_id.resize(1);
    zero_id[0] = dariadb::Id(0);
    as->foreach (QueryInterval(zero_id, 0, from, to + copies_count), order_clbk);
    order_clbk->wait();
    if (!order_clbk->is_greater) {
      throw MAKE_EXCEPTION("!order_clbk->is_greater");
    }
    delete order_clbk;
  }
}

void readTimePointCheck(IMeasStorage *as, Time from, Time to, Time step,
                        const IdArray &_all_ids_array, bool check_stop_flag) {
  auto qp = QueryTimePoint(_all_ids_array, 0, to + copies_count);
  auto all_id2meas = as->readTimePoint(qp);

  size_t ids_count = (size_t)((to - from) / step);
  if (all_id2meas.size() < ids_count) {
    throw MAKE_EXCEPTION("all.size() < ids_count. must be GE");
  }

  auto qpoint_clbk = std::make_unique<Callback>();
  as->foreach (qp, qpoint_clbk.get());
  if (qpoint_clbk->count != all_id2meas.size()) {
    throw MAKE_EXCEPTION("qpoint_clbk->count != all_id2meas.size()");
  }

  if (check_stop_flag && qpoint_clbk->is_end_called != 1) {
    THROW_EXCEPTION("clbk->is_end_called!=1: ", qpoint_clbk->is_end_called.load());
  }

  qp.time_point = to + copies_count;
  auto fltr_res_tp = as->readTimePoint(qp);
  if (fltr_res_tp.size() < ids_count) {
    throw MAKE_EXCEPTION("fltr_res.size() < ids_count. must be GE");
  }

  IdArray notExstsIDs{9999};
  fltr_res_tp.clear();
  qp.ids = notExstsIDs;
  qp.time_point = to - 1;
  fltr_res_tp = as->readTimePoint(qp);
  if (fltr_res_tp.size() != size_t(1)) { // must return NO_DATA
    throw MAKE_EXCEPTION("fltr_res.size() != size_t(1)");
  }

  Meas m_not_exists = fltr_res_tp[notExstsIDs.front()];
  if (m_not_exists.flag != FLAGS::_NO_DATA) {
    throw MAKE_EXCEPTION("fltr_res.front().flag != FLAGS::NO_DATA");
  }
}

void storage_test_check(IMeasStorage *as, Time from, Time to, Time step,
                        bool check_stop_flag, bool random_timestamps,
                        bool run_copy_test) {
  IdSet _all_ids_set;
  Time maxWritedTime = MIN_TIME;
  dariadb::logger_info("fill storage");
  size_t total_count = fill_storage_for_test(as, from, to, step, &_all_ids_set,
                                             &maxWritedTime, random_timestamps);
  dariadb::logger_info("loadMinMax");
  auto minMax = as->loadMinMax();

  if (minMax->size() != _all_ids_set.size()) {
    throw MAKE_EXCEPTION("minMax.size()!=_all_ids_set.size()");
  }

  auto f = [from](const dariadb::Id2MinMax::value_type &v) {
    auto mm = v.second;
    if (mm.min.time < from) {
      throw MAKE_EXCEPTION("mm.min<from");
    }

    if (mm.max.time == 0 && mm.min.time > mm.max.time) {
      throw MAKE_EXCEPTION("mm.max==0 && mm.min>mm.max");
    }
  };
  minMax->apply(f);

  dariadb::logger_info("minMaxCheck");
  minMaxCheck(as, from, maxWritedTime);
  dariadb::logger_info("currentValue");
  Id2Meas current_mlist;

  IdArray _all_ids_array(_all_ids_set.begin(), _all_ids_set.end());
  current_mlist = as->currentValue(_all_ids_array, 0);

  if (current_mlist.size() != _all_ids_set.size()) {
    throw MAKE_EXCEPTION("current_mlist.size()!= _all_ids_set.size()");
  }

  if (as->minTime() != from) {
    throw MAKE_EXCEPTION("as->minTime() != from");
  }
  if (as->maxTime() < to) {
    throw MAKE_EXCEPTION("as->maxTime() < to");
  }
  dariadb::logger_info("readIntervalCheck");

  readIntervalCheck(as, from, to, step, _all_ids_set, _all_ids_array, total_count,
                    check_stop_flag);
  dariadb::logger_info("readTimePointCheck");
  readTimePointCheck(as, from, to, step, _all_ids_array, check_stop_flag);

  if (run_copy_test) {
    dariadb::logger_info("copyCheck");
    for (auto id : _all_ids_set) {
      WriteCallback clbk(as);

      auto qi = dariadb::QueryInterval({id}, 0, from, to);
      as->foreach (qi, &clbk);
      clbk.wait();
    }
  }
  dariadb::logger_info("flush");
  as->flush();
}

void check_reader(const dariadb::Cursor_Ptr &rdr) {
  dariadb::Meas top;
  bool is_first = true;
  std::map<dariadb::Time, size_t> time2count;
  while (!rdr->is_end()) {
    auto v = rdr->readNext();
    if (time2count.count(v.time) == size_t(0)) {
      time2count[v.time] = size_t(1);
    } else {
      time2count[v.time]++;
    }
    if (!is_first) {
      is_first = true;
      if (v.time != top.time) {
        THROW_EXCEPTION("! v.time != top.time: ", v.time, top.time);
      }
    }
    if (!rdr->is_end()) {
      top = rdr->top();
      if (v.time > top.time) {
        THROW_EXCEPTION("! v.time > top.time: ", v.time, top.time);
      }
    }
  }
  for (auto kv : time2count) {
    if (kv.second > size_t(1)) {
      THROW_EXCEPTION("kv.second > size_t(1) - ", kv.first, " => ", kv.second);
    }
  }
}

class Moc_SubscribeClbk : public dariadb::IReadCallback {
public:
  std::list<dariadb::Meas> values;
  void apply(const dariadb::Meas &m) override { values.push_back(m); }
  void is_end() override {}
  ~Moc_SubscribeClbk() {}
};

void subscribe_test(dariadb::IEngine *ms) {
  const size_t id_count = 5;
  auto c1 = std::make_shared<Moc_SubscribeClbk>();
  auto c2 = std::make_shared<Moc_SubscribeClbk>();
  auto c3 = std::make_shared<Moc_SubscribeClbk>();
  auto c4 = std::make_shared<Moc_SubscribeClbk>();

  dariadb::IdArray ids{};
  ms->subscribe(ids, 0, c1); // all
  ids.push_back(2);
  ms->subscribe(ids, 0, c2); // 2
  ids.push_back(1);
  ms->subscribe(ids, 0, c3); // 1 2
  ids.clear();
  ms->subscribe(ids, 1, c4); // with flag=1

  auto m = dariadb::Meas();
  const size_t total_count = 100;
  const dariadb::Time time_step = 1;

  for (size_t i = 0; i < total_count; i += time_step) {
    m.id = i % id_count;
    m.flag = dariadb::Flag(i);
    m.time = i;
    m.value = 0;
    ms->append(m);
  }
  if (c1->values.size() != total_count) {
    THROW_EXCEPTION("c1->values.size() != total_count", c1->values.size(), total_count);
  }

  if (c2->values.size() != size_t(total_count / id_count)) {
    THROW_EXCEPTION("c2->values.size() != size_t(total_count / id_count)");
  }
  if (c2->values.front().id != dariadb::Id(2)) {
    THROW_EXCEPTION("c2->values.size() != size_t(total_count / id_count)");
  }

  if (c3->values.size() != (size_t(total_count / id_count) * 2)) {
    THROW_EXCEPTION("c3->values.size() != size_t(total_count / id_count) * 2");
  }
  if (c3->values.front().id != dariadb::Id(1)) {
    THROW_EXCEPTION("c3->values.front().id != dariadb::Id(1)");
  }

  if (c4->values.size() != size_t(1)) {
    THROW_EXCEPTION("c4->values.size() != size_t(1)");
  }
  if (c4->values.front().flag != dariadb::Flag(1)) {
    THROW_EXCEPTION("c4->values.front().flag != dariadb::Flag(1)");
  }
}
}
