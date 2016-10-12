#include "libdariadb/storage/time_tree.h"

using namespace dariadb;
using namespace dariadb::storage;

struct TimeTree::Private : public IMeasStorage {
  Private(const TimeTree::Params &p) {}

  // Inherited via IMeasStorage
  virtual Time minTime() override { return Time(); }
  virtual Time maxTime() override { return Time(); }
  virtual bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                          dariadb::Time *maxResult) override {
    return false;
  }
  virtual void foreach (const QueryInterval &q, IReaderClb * clbk) override {}
  virtual Id2Meas readTimePoint(const QueryTimePoint &q) override { return Id2Meas(); }
  virtual Id2Meas currentValue(const IdArray &ids, const Flag &flag) override {
    return Id2Meas();
  }
  virtual append_result append(const Meas &value) override { return append_result(); }
  virtual void flush() override {}
};

TimeTree::TimeTree(const TimeTree::Params &p) : _impl(new TimeTree::Private(p)) {}

TimeTree::~TimeTree() {
  _impl = nullptr;
}

Time dariadb::storage::TimeTree::minTime() {
	return _impl->minTime();
}

Time dariadb::storage::TimeTree::maxTime() {
	return _impl->maxTime();
}

bool dariadb::storage::TimeTree::minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                                            dariadb::Time *maxResult) {
  return _impl->minMaxTime(id,minResult, maxResult);
}

void dariadb::storage::TimeTree::foreach (const QueryInterval &q, IReaderClb * clbk) {
	_impl->foreach(q, clbk);
}

Id2Meas dariadb::storage::TimeTree::readTimePoint(const QueryTimePoint &q) {
	return _impl->readTimePoint(q);
}

Id2Meas dariadb::storage::TimeTree::currentValue(const IdArray &ids, const Flag &flag) {
	return _impl->currentValue(ids, flag);
}

append_result dariadb::storage::TimeTree::append(const Meas &value) {
	return _impl->append(value);
}

void dariadb::storage::TimeTree::flush() {
	_impl->flush();
}
