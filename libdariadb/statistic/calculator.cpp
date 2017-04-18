#include <libdariadb/statistic/calculator.h>

using namespace dariadb;
using namespace dariadb::storage;
using namespace dariadb::statistic;

Average::Average() : _result() {}
void Average::apply(const Meas &ma) {}

Meas Average::result() const {
  return _result;
}

Calculator::Calculator(const IEngine_Ptr &storage) : _storage(storage) {}

MeasArray Calculator::apply(const IdArray &ids, Time from, Time to, Flag f,
                            const std::vector<std::string> &functions,
                            const MeasArray &ma) {
  return MeasArray();
}