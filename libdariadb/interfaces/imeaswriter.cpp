#include <libdariadb/interfaces/imeaswriter.h>

using namespace dariadb;
using namespace dariadb::storage;

append_result IMeasWriter::append(const Meas &value){
    return append_result(0,1);
}

void IMeasWriter::flush(){

}

append_result IMeasWriter::append(const MeasArray::const_iterator &begin,
                                  const MeasArray::const_iterator &end) {
  dariadb::append_result ar{};

  for (auto it = begin; it != end; ++it) {
    ar = ar + this->append(*it);
  }
  return ar;
}

append_result IMeasWriter::append(const MeasList::const_iterator &begin,
                                  const MeasList::const_iterator &end) {
  dariadb::append_result ar{};
  for (auto it = begin; it != end; ++it) {
    ar = ar + this->append(*it);
  }
  return ar;
}


IMeasWriter::~IMeasWriter(){

}
