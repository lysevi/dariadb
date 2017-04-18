#pragma once

#include <libdariadb/interfaces/iengine.h>
#include <libdariadb/st_exports.h>
#include <libdariadb/statistic/ifunction.h>
#include <istream>
#include <ostream>

namespace dariadb {
namespace statistic {

enum class FUNCKTION_KIND : int { AVERAGE };

class Average : public IFunction {
public:
  EXPORT Average();
  EXPORT void apply(const Meas &ma) override;
  EXPORT Meas result() const override;
  int kind() const override { return (int)FUNCKTION_KIND::AVERAGE; }

protected:
  Meas _result;
  size_t _count;
};

EXPORT std::istream &operator>>(std::istream &in, FUNCKTION_KIND &f);
EXPORT std::ostream &operator<<(std::ostream &stream, const FUNCKTION_KIND &f);

class FunctionFactory {
public:
  EXPORT static std::vector<IFunction_ptr> make(const std::vector<FUNCKTION_KIND> &kinds);
};

class Calculator {
public:
  EXPORT Calculator(const IEngine_Ptr &storage);
  EXPORT MeasArray apply(const IdArray &ids, Time from, Time to, Flag f,
                         const std::vector<FUNCKTION_KIND> &functions,
                         const MeasArray &ma);

protected:
  IEngine_Ptr _storage;
};
} // namespace statistic
} // namespace dariadb