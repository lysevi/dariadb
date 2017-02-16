#include <libdariadb/shard.h>

using namespace dariadb;

class ShardEngine::Private : IMeasStorage, IEngine {
public:
  Private(const ShardEngine::Description &description)
      : _description(description) {}
  ShardEngine::Description _description;

  // Inherited via IMeasStorage
  Time minTime() override { return Time(); }
  Time maxTime() override { return Time(); }
  bool minMaxTime(Id id, Time *minResult, Time *maxResult) override {
    return false;
  }
  void foreach (const QueryInterval &q, IReadCallback * clbk) override {}
  Id2Cursor intervalReader(const QueryInterval &query) override {
    return Id2Cursor();
  }
  Id2Meas readTimePoint(const QueryTimePoint &q) override { return Id2Meas(); }
  Id2Meas currentValue(const IdArray &ids, const Flag &flag) override {
    return Id2Meas();
  }
  Statistic stat(const Id id, Time from, Time to) override {
    return Statistic();
  }

  void fsck() override {}

  void eraseOld(const Time &t) override {}

  void repack() override {}
};

ShardEngine_Ptr
ShardEngine::create(const ShardEngine::Description &description) {
  return nullptr;
}

ShardEngine_Ptr ShardEngine::open(const std::string &path) { return nullptr; }

ShardEngine::ShardEngine(const Description &description)
    : _impl(new ShardEngine::Private(description)) {}

Time ShardEngine::minTime() { return _impl->minTime(); }

Time ShardEngine::maxTime() { return _impl->maxTime(); }

bool ShardEngine::minMaxTime(Id id, Time *minResult, Time *maxResult) {
  return _impl->minMaxTime(id, minResult, maxResult);
}

void ShardEngine::foreach (const QueryInterval &q, IReadCallback * clbk) {
  _impl->foreach (q, clbk);
}

Id2Cursor ShardEngine::intervalReader(const QueryInterval &query) {
  return _impl->intervalReader(query);
}

Id2Meas ShardEngine::readTimePoint(const QueryTimePoint &q) {
  return _impl->readTimePoint(q);
}

Id2Meas ShardEngine::currentValue(const IdArray &ids, const Flag &flag) {
  return _impl->currentValue(ids, flag);
}

Statistic ShardEngine::stat(const Id id, Time from, Time to) {
  return _impl->stat(id, from, to);
}

void ShardEngine::fsck() { _impl->fsck(); }

void ShardEngine::eraseOld(const Time &t) { _impl->eraseOld(t); }

void ShardEngine::repack() { _impl->repack(); }
