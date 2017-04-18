#pragma once

#include <libdariadb/interfaces/iengine.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/statistic/ifunction.h>
namespace dariadb {
namespace statistic {

class Average : public IFunction {
public:
  EXPORT Average();
  EXPORT void apply(const Meas &ma) override;
  EXPORT Meas result() const override;

protected:
  Meas _result;
};

class Calculator {
public:
  EXPORT Calculator(const IEngine_Ptr &storage);
  EXPORT MeasArray apply(const IdArray &ids, Time from, Time to, Flag f,
                         const std::vector<std::string> &functions, const MeasArray &ma);

protected:
  IEngine_Ptr _storage;
};
} // namespace statistic
} // namespace dariadb