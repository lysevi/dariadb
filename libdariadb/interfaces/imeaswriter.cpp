#include <libdariadb/interfaces/imeaswriter.h>

using namespace dariadb;

Status IMeasWriter::append(const Meas &) {
  return Status(0, 1);
}

void IMeasWriter::flush() {}

Status IMeasWriter::append(const MeasArray::const_iterator &begin,
                           const MeasArray::const_iterator &end) {
  dariadb::Status ar{};

  for (auto it = begin; it != end; ++it) {
    ar = ar + this->append(*it);
  }
  return ar;
}

Status IMeasWriter::append(const MeasList::const_iterator &begin,
                           const MeasList::const_iterator &end) {
  dariadb::Status ar{};
  for (auto it = begin; it != end; ++it) {
    ar = ar + this->append(*it);
  }
  return ar;
}

IMeasWriter::~IMeasWriter() {}
