#include <libdariadb/interfaces/imeaswriter.h>

using namespace dariadb;

void IMeasWriter::flush() {}

Status IMeasWriter::append(const MeasArray::const_iterator &begin,
                           const MeasArray::const_iterator &end) {
  dariadb::Status ar{};

  for (auto it = begin; it != end; ++it) {
    ar = ar + this->append(*it);
  }
  return ar;
}

IMeasWriter::~IMeasWriter() {}
